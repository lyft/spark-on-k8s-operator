/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/time/rate"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler"
	schedulerinterface "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/interface"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	sparkExecutorIDLabel = "spark-exec-id"
	queueTokenRefillRate = 50
	queueTokenBucketSize = 500
)

var (
	keyFunc     = cache.DeletionHandlingMetaNamespaceKeyFunc
	execCommand = exec.Command
)

// Controller manages instances of SparkApplication.
type Controller struct {
	crdClient               crdclientset.Interface
	kubeClient              clientset.Interface
	queue                   workqueue.RateLimitingInterface
	cacheSynced             cache.InformerSynced
	recorder                record.EventRecorder
	metrics                 *sparkAppMetrics
	applicationLister       crdlisters.SparkApplicationLister
	podLister               v1.PodLister
	ingressURLFormat        string
	batchSchedulerMgr       *batchscheduler.SchedulerManager
	subJobManager           submissionJobManager
	clientModeSubPodManager clientModeSubmissionPodManager
}

// NewController creates a new Controller.
func NewController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	crdInformerFactory crdinformers.SharedInformerFactory,
	informerFactory informers.SharedInformerFactory,
	metricsConfig *util.MetricConfig,
	namespace string,
	ingressURLFormat string,
	batchSchedulerMgr *batchscheduler.SchedulerManager) *Controller {
	crdscheme.AddToScheme(scheme.Scheme)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(namespace),
	})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "spark-operator"})

	return newSparkApplicationController(crdClient, kubeClient, crdInformerFactory, informerFactory, recorder, metricsConfig, ingressURLFormat, batchSchedulerMgr)
}

func newSparkApplicationController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	crdInformerFactory crdinformers.SharedInformerFactory,
	informerFactory informers.SharedInformerFactory,
	eventRecorder record.EventRecorder,
	metricsConfig *util.MetricConfig,
	ingressURLFormat string,
	batchSchedulerMgr *batchscheduler.SchedulerManager) *Controller {
	queue := workqueue.NewNamedRateLimitingQueue(&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(queueTokenRefillRate), queueTokenBucketSize)},
		"spark-application-controller")

	controller := &Controller{
		crdClient:               crdClient,
		kubeClient:              kubeClient,
		recorder:                eventRecorder,
		queue:                   queue,
		ingressURLFormat:        ingressURLFormat,
		batchSchedulerMgr:       batchSchedulerMgr,
		subJobManager:           &realSubmissionJobManager{kubeClient: kubeClient},
		clientModeSubPodManager: &realClientModeSubmissionPodManager{kubeClient: kubeClient},
	}

	if metricsConfig != nil {
		controller.metrics = newSparkAppMetrics(metricsConfig)
		controller.metrics.registerMetrics()
	}

	crdInformer := crdInformerFactory.Sparkoperator().V1beta2().SparkApplications()
	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	controller.applicationLister = crdInformer.Lister()

	podsInformer := informerFactory.Core().V1().Pods()
	sparkObjectEventHandler := newSparkObjectEventHandler(controller.queue.AddRateLimited, controller.applicationLister)
	podsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sparkObjectEventHandler.onObjectAdded,
		UpdateFunc: sparkObjectEventHandler.onObjectUpdated,
		DeleteFunc: sparkObjectEventHandler.onObjectDeleted,
	})
	controller.podLister = podsInformer.Lister()

	jobInformer := informerFactory.Batch().V1().Jobs()
	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sparkObjectEventHandler.onObjectAdded,
		UpdateFunc: sparkObjectEventHandler.onObjectUpdated,
		DeleteFunc: sparkObjectEventHandler.onObjectDeleted,
	})
	controller.subJobManager = &realSubmissionJobManager{kubeClient: kubeClient, jobLister: jobInformer.Lister()}
	controller.clientModeSubPodManager = &realClientModeSubmissionPodManager{kubeClient: kubeClient, podLister: podsInformer.Lister()}

	controller.cacheSynced = func() bool {
		return crdInformer.Informer().HasSynced() && podsInformer.Informer().HasSynced()
	}

	return controller
}

// Start starts the Controller by registering a watcher for SparkApplication objects.
func (c *Controller) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the workers of the SparkApplication controller")
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// Wait for all involved caches to be synced, before processing items from the queue is started.
	if !cache.WaitForCacheSync(stopCh, c.cacheSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}
	return nil
}

// Stop stops the controller.
func (c *Controller) Stop() {
	glog.Info("Stopping the SparkApplication controller")
	c.queue.ShutDown()
}

// Callback function called when a new SparkApplication object gets created.
func (c *Controller) onAdd(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	glog.Infof("SparkApplication %s/%s was added, enqueueing it for submission", app.Namespace, app.Name)
	c.enqueue(app)
}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {
	oldApp := oldObj.(*v1beta2.SparkApplication)
	newApp := newObj.(*v1beta2.SparkApplication)

	// The informer will call this function on non-updated resources during resync, avoid
	// enqueuing unchanged applications, unless it has expired or is subject to retry.
	if oldApp.ResourceVersion == newApp.ResourceVersion && !c.hasApplicationExpired(newApp) && !shouldRetry(newApp) {
		return
	}

	// The spec has changed. This is currently best effort as we can potentially miss updates
	// and end up in an inconsistent state.
	if !equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		// Force-set the application status to Invalidating which handles clean-up and application re-run.
		if _, err := c.updateApplicationStatusWithRetries(newApp, func(status *v1beta2.SparkApplicationStatus) {
			status.AppState.State = v1beta2.InvalidatingState
		}); err != nil {
			c.recorder.Eventf(
				newApp,
				apiv1.EventTypeWarning,
				"SparkApplicationSpecUpdateFailed",
				"failed to process spec update for SparkApplication %s: %v",
				newApp.Name,
				err)
			return
		}

		c.recorder.Eventf(
			newApp,
			apiv1.EventTypeNormal,
			"SparkApplicationSpecUpdateProcessed",
			"Successfully processed spec update for SparkApplication %s",
			newApp.Name)
	}

	glog.V(2).Infof("SparkApplication %s/%s was updated, enqueueing it", newApp.Namespace, newApp.Name)
	c.enqueue(newApp)
}

func (c *Controller) onDelete(obj interface{}) {
	var app *v1beta2.SparkApplication
	switch obj.(type) {
	case *v1beta2.SparkApplication:
		app = obj.(*v1beta2.SparkApplication)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		app = deletedObj.(*v1beta2.SparkApplication)
	}

	if app != nil {
		c.handleSparkApplicationDeletion(app)
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationDeleted",
			"SparkApplication %s was deleted",
			app.Name)
	}
}

// runWorker runs a single controller worker.
func (c *Controller) runWorker() {
	defer utilruntime.HandleCrash()
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	glog.V(2).Infof("Starting processing key: %q", key)
	defer glog.V(2).Infof("Ending processing key: %q", key)
	err := c.syncSparkApplication(key.(string))
	if err == nil {
		// Successfully processed the key or the key was not found so tell the queue to stop tracking
		// history for your key. This will reset things like failure counts for per-item rate limiting.
		c.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("failed to sync SparkApplication %q: %v", key, err))
	return true
}

func (c *Controller) getExecutorPods(app *v1beta2.SparkApplication) ([]*apiv1.Pod, error) {
	matchLabels := getResourceLabels(app)
	matchLabels[config.SparkRoleLabel] = config.SparkExecutorRole
	// Fetch all the executor pods for the current run of the application.
	selector := labels.SelectorFromSet(labels.Set(matchLabels))
	pods, err := c.podLister.Pods(app.Namespace).List(selector)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
	return pods, nil
}

func (c *Controller) getDriverPod(app *v1beta2.SparkApplication) (*apiv1.Pod, error) {
	pod, err := c.podLister.Pods(app.Namespace).Get(app.Status.DriverInfo.PodName)
	if err == nil {
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}

	// The driver pod was not found in the informer cache, try getting it directly from the API server.
	pod, err = c.kubeClient.CoreV1().Pods(app.Namespace).Get(app.Status.DriverInfo.PodName, metav1.GetOptions{})
	if err == nil {
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}
	// Driver pod was not found on the API server either.
	return nil, nil
}

// getAndUpdateDriverState finds the driver pod of the application
// and updates the driver state based on the current phase of the pod.
func (c *Controller) getAndUpdateDriverState(app *v1beta2.SparkApplication) error {
	// Either the driver pod doesn't exist yet or its name has not been updated.
	if app.Status.DriverInfo.PodName == "" {
		return fmt.Errorf("empty driver pod name with application state %s", app.Status.AppState.State)
	}

	driverPod, err := c.getDriverPod(app)
	if err != nil {
		return err
	}

	if driverPod == nil {
		app.Status.AppState.ErrorMessage = "Driver Pod not found"
		app.Status.AppState.State = v1beta2.FailingState
		app.Status.TerminationTime = metav1.Now()
		return nil
	}

	app.Status.SparkApplicationID = getSparkApplicationID(driverPod)
	driverState := podStatusToDriverState(driverPod.Status)

	if hasDriverTerminated(driverState) {
		if app.Status.TerminationTime.IsZero() {
			app.Status.TerminationTime = metav1.Now()
		}
		if driverState == v1beta2.DriverFailedState {
			state := getDriverContainerTerminatedState(driverPod.Status)
			if state != nil {
				if state.ExitCode != 0 {
					app.Status.AppState.ErrorMessage = fmt.Sprintf("driver container failed with ExitCode: %d, Reason: %s", state.ExitCode, state.Reason)
				}
			} else {
				app.Status.AppState.ErrorMessage = "driver container status missing"
			}
		}
	}
	newState := driverStateToApplicationState(driverState)

	// Only record a driver event if the application state (derived from the driver pod phase) has changed.
	if newState != app.Status.AppState.State {
		c.recordDriverEvent(app, driverState, driverPod.Name)
		app.Status.AppState.State = newState
	}

	return nil
}

// getAndUpdateExecutorState lists the executor pods of the application
// and updates the executor state based on the current phase of the pods.
func (c *Controller) getAndUpdateExecutorState(app *v1beta2.SparkApplication) error {
	pods, err := c.getExecutorPods(app)
	if err != nil {
		return err
	}

	executorStateMap := make(map[string]v1beta2.ExecutorState)
	var executorApplicationID string
	for _, pod := range pods {
		if util.IsExecutorPod(pod) {
			newState := podPhaseToExecutorState(pod.Status.Phase)
			oldState, exists := app.Status.ExecutorState[pod.Name]
			// Only record an executor event if the executor state is new or it has changed.
			if !exists || newState != oldState {
				c.recordExecutorEvent(app, newState, pod.Name)
			}
			executorStateMap[pod.Name] = newState

			if executorApplicationID == "" {
				executorApplicationID = getSparkApplicationID(pod)
			}
		}
	}

	// ApplicationID label can be different on driver/executors. Prefer executor ApplicationID if set.
	// Refer https://issues.apache.org/jira/projects/SPARK/issues/SPARK-25922 for details.
	if executorApplicationID != "" {
		app.Status.SparkApplicationID = executorApplicationID
	}

	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1beta2.ExecutorState)
	}
	for name, execStatus := range executorStateMap {
		app.Status.ExecutorState[name] = execStatus
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !isExecutorTerminated(oldStatus) && !exists && !isDriverRunning(app) {
			// If ApplicationState is SUCCEEDING, in other words, the driver pod has been completed
			// successfully. The executor pods terminate and are cleaned up, so we could not found
			// the executor pod, under this circumstances, we assume the executor pod are completed.
			if app.Status.AppState.State == v1beta2.SucceedingState {
				app.Status.ExecutorState[name] = v1beta2.ExecutorCompletedState
			} else {
				glog.Infof("Executor pod %s not found, assuming it was deleted.", name)
				app.Status.ExecutorState[name] = v1beta2.ExecutorFailedState
			}
		}
	}

	return nil
}

func (c *Controller) getAndUpdateAppState(app *v1beta2.SparkApplication) error {
	if err := c.getAndUpdateDriverState(app); err != nil {
		return err
	}
	if err := c.getAndUpdateExecutorState(app); err != nil {
		return err
	}
	return nil
}

func (c *Controller) handleSparkApplicationDeletion(app *v1beta2.SparkApplication) {
	// SparkApplication deletion requested, lets delete driver pod.
	if err := c.deleteSparkResources(app); err != nil {
		glog.Errorf("failed to delete resources associated with deleted SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
}

// ShouldRetry determines if SparkApplication in a given state should be retried.
func shouldRetry(app *v1beta2.SparkApplication) bool {
	switch app.Status.AppState.State {
	case v1beta2.SucceedingState:
		return app.Spec.RestartPolicy.Type == v1beta2.Always
	case v1beta2.FailingState:
		if app.Spec.RestartPolicy.Type == v1beta2.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta2.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.ExecutionAttempts <= *app.Spec.RestartPolicy.OnFailureRetries {
				return true
			}
		}
	case v1beta2.PendingSubmissionState:
		//only used for client mode
		var interval int64 = 257
		if app.Spec.Mode != v1beta2.ClusterMode && hasRetryIntervalPassed(&interval, app.Status.SubmissionAttempts, app.CreationTimestamp) && app.Spec.RestartPolicy.Type == v1beta2.OnFailure {
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts <= *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	case v1beta2.FailedSubmissionState:
		// We retry only if the RestartPolicy is Always. The Submission Job already retries upto the OnSubmissionFailureRetries specified.
		if app.Spec.RestartPolicy.Type == v1beta2.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta2.OnFailure && app.Spec.Mode != v1beta2.ClusterMode {
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts < *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	}

	return false
}

// State Machine for SparkApplication:
//+--------------------------------------------------------------------------------------------------------------------+
//|        +---------------------------------------------------------------------------------------------+             |
//|        |       +----------+                                                                          |             |
//|        |       |          |                                                                          |             |
//|        |       |          |                                                                          |             |
//|        |       |Submission|                                                                          |             |
//|        |  +---->  Failed  +----+------------------------------------------------------------------+  |             |
//|        |  |    |          |    |                                                                  |  |             |
//|        |  |    |          |    |                                                                  |  |             |
//|        |  |    +----^-----+    |  +-----------------------------------------+                     |  |             |
//|        |  |         |          |  |                                         |                     |  |             |
//|        |  |         |          |  |                                         |                     |  |             |
//|      +-+--+----+    |    +-----v--+-+          +----------+           +-----v-----+          +----v--v--+          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |   New   +---------> Submitted+----------> Running  +----------->  Failing  +---------->  Failed  |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      +---------+    |    +----^-----+          +-----+----+           +-----+-----+          +----------+          |
//|                     |         |                      |                      |                                      |
//|                     |         |                      |                      |                                      |
//|    +------------+   |         |             +-------------------------------+                                      |
//|    |            |   |   +-----+-----+       |        |                +-----------+          +----------+          |
//|    |            |   |   |  Pending  |       |        |                |           |          |          |          |
//|    |            |   +---+   Rerun   <-------+        +---------------->Succeeding +---------->Completed |          |
//|    |Invalidating|       |           <-------+                         |           |          |          |          |
//|    |            +------->           |       |                         |           |          |          |          |
//|    |            |       |           |       |                         |           |          |          |          |
//|    |            |       +-----------+       |                         +-----+-----+          +----------+          |
//|    +------------+                           |                               |                                      |
//|                                             |                               |                                      |
//|                                             +-------------------------------+                                      |
//|                                                                                                                    |
//+--------------------------------------------------------------------------------------------------------------------+

func (c *Controller) syncSparkApplication(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("failed to get the namespace and name from key %s: %v", key, err)
	}
	app, err := c.getSparkApplication(namespace, name)
	if err != nil {
		return err
	}
	if app == nil {
		// SparkApplication not found.
		return nil
	}
	if !app.DeletionTimestamp.IsZero() {
		c.handleSparkApplicationDeletion(app)
		return nil
	}

	appToUpdate := app.DeepCopy()

	// Take action based on application state.
	switch appToUpdate.Status.AppState.State {
	case v1beta2.NewState:
		c.recordSparkApplicationEvent(appToUpdate)
		if err := c.validateSparkApplication(appToUpdate); err != nil {
			appToUpdate.Status.AppState.State = v1beta2.FailedState
			appToUpdate.Status.AppState.ErrorMessage = err.Error()
		} else {
			appToUpdate = c.submitSparkApplication(appToUpdate)
		}
	case v1beta2.PendingSubmissionState:
		//Resubmission is based on resource quota. We wait and then see if the interval passed to rerun
		if app.Spec.Mode == v1beta2.ClientMode || app.Spec.Mode == "" {
			if shouldRetry(appToUpdate) {
				appToUpdate.Status.AppState.ErrorMessage = ""
				appToUpdate.Status.AppState.State = v1beta2.PendingRerunState
			}
		} else {
			// Check the status of the submission Job and set the application status accordingly.
			succeeded, completionTime, err := c.subJobManager.hasJobSucceeded(appToUpdate)

			if succeeded != nil {
				// Submission Job terminated in either success or failure.
				if *succeeded {
					c.createSparkUIResources(appToUpdate)
					appToUpdate.Status.AppState.State = v1beta2.SubmittedState
					appToUpdate.Status.ExecutionAttempts++
					if completionTime != nil {
						appToUpdate.Status.SubmissionTime = *completionTime
					}
					c.recordSparkApplicationEvent(appToUpdate)
				} else {
					// Since we delegate submission retries to the Kubernetes Job controller, the fact that the
					// submission Job failed means all the submission attempts failed. So we set the application
					// state to FailedSubmission, which is a terminal state.
					appToUpdate.Status.AppState.State = v1beta2.FailedSubmissionState
					if err != nil {
						// Propagate the error if the submission Job ended in failure after retries.
						appToUpdate.Status.AppState.ErrorMessage = err.Error()
					}
					c.recordSparkApplicationEvent(appToUpdate)
				}
			} else if err != nil {
				// Received an error trying to query the status of the Job.
				return err
			}

		}
	case v1beta2.SucceedingState:
		// The current run of the application has completed, check if it needs to be restarted.
		if !shouldRetry(appToUpdate) {
			// Application is not subject to retry. Move to terminal CompletedState.
			appToUpdate.Status.AppState.State = v1beta2.CompletedState
			c.recordSparkApplicationEvent(appToUpdate)
		} else {
			if err := c.deleteSparkResources(appToUpdate); err != nil {
				glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
					appToUpdate.Namespace, appToUpdate.Name, err)
				return err
			}
			appToUpdate.Status.AppState.State = v1beta2.PendingRerunState
		}
	case v1beta2.FailingState:
		if !shouldRetry(appToUpdate) {
			// Application is not subject to retry. Move to terminal FailedState.
			appToUpdate.Status.AppState.State = v1beta2.FailedState
			c.recordSparkApplicationEvent(appToUpdate)
		} else if hasRetryIntervalPassed(appToUpdate.Spec.RestartPolicy.OnFailureRetryInterval, appToUpdate.Status.ExecutionAttempts, appToUpdate.Status.TerminationTime) {
			if err := c.deleteSparkResources(appToUpdate); err != nil {
				glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
					appToUpdate.Namespace, appToUpdate.Name, err)
				return err
			}
			appToUpdate.Status.AppState.State = v1beta2.PendingRerunState
		}
	case v1beta2.FailedSubmissionState:
		// Submission Job terminated in failure, check if the application needs to be retried.
		if !shouldRetry(appToUpdate) {
			// Application is not subject to retry. Move to terminal FailedState.
			appToUpdate.Status.AppState.State = v1beta2.FailedState
			c.recordSparkApplicationEvent(appToUpdate)
		} else {
			if appToUpdate.Spec.Mode == v1beta2.ClusterMode {
				// Application is subject to retry. Move to PendingRerunState.
				appToUpdate.Status.AppState.ErrorMessage = ""
				appToUpdate.Status.AppState.State = v1beta2.PendingRerunState
			}
		}
	case v1beta2.InvalidatingState:
		// Invalidate the current run and enqueue the SparkApplication for re-submission.
		if err := c.deleteSparkResources(appToUpdate); err != nil {
			glog.Errorf("failed to delete resources associated with SparkApplication %s/%s: %v",
				appToUpdate.Namespace, appToUpdate.Name, err)
			return err
		}
		c.clearStatus(&appToUpdate.Status)
		appToUpdate.Status.AppState.State = v1beta2.PendingRerunState
	case v1beta2.PendingRerunState:
		glog.V(2).Infof("SparkApplication %s/%s pending rerun", appToUpdate.Namespace, appToUpdate.Name)
		if c.validateSparkResourceDeletion(appToUpdate) {
			glog.V(2).Infof("Resources for SparkApplication %s/%s successfully deleted", appToUpdate.Namespace, appToUpdate.Name)
			c.recordSparkApplicationEvent(appToUpdate)
			c.clearStatus(&appToUpdate.Status)
			appToUpdate = c.submitSparkApplication(appToUpdate)
		}
	case v1beta2.SubmittedState, v1beta2.RunningState, v1beta2.UnknownState:
		if err := c.getAndUpdateAppState(appToUpdate); err != nil {
			return err
		}
	case v1beta2.CompletedState, v1beta2.FailedState:
		if appToUpdate.Spec.Mode == v1beta2.ClientMode {
			c.deleteSparkUI(appToUpdate)
		}
		if c.hasApplicationExpired(app) {
			glog.Infof("Garbage collecting expired SparkApplication %s/%s", app.Namespace, app.Name)
			err := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Delete(app.Name, metav1.NewDeleteOptions(0))
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
			return nil
		}
	}

	if appToUpdate != nil {
		err = c.updateStatusAndExportMetrics(app, appToUpdate)
		if err != nil {
			glog.Errorf("failed to update SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
			return err
		}
	}

	return nil
}

// Helper func to determine if we have waited enough to retry the SparkApplication.
func hasRetryIntervalPassed(retryInterval *int64, attemptsDone int32, lastEventTime metav1.Time) bool {
	glog.V(3).Infof("retryInterval: %d , lastEventTime: %v, attempsDone: %d", retryInterval, lastEventTime, attemptsDone)
	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	glog.V(3).Infof("currentTime is %v, interval is %v", currentTime, interval)
	if currentTime.After(lastEventTime.Add(interval)) {
		return true
	}
	return false
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (c *Controller) submitSparkApplication(app *v1beta2.SparkApplication) *v1beta2.SparkApplication {
	// Apply default values before submitting the application to run.
	v1beta2.SetSparkApplicationDefaults(app)

	if app.PrometheusMonitoringEnabled() {
		if err := configPrometheusMonitoring(app, c.kubeClient); err != nil {
			glog.Error(err)
		}
	}

	// Use batch scheduler to perform scheduling task before submitting.
	if needScheduling, scheduler := c.shouldDoBatchScheduling(app); needScheduling {
		err := scheduler.DoBatchSchedulingOnSubmission(app)
		if err != nil {
			glog.Errorf("failed to process batch scheduler BeforeSubmitSparkApplication with error %v", err)
			return app
		}
	}

	var submissionID string
	var driverPodName string
	var err error

	if app.Spec.Mode == v1beta2.ClientMode {
		submissionID, driverPodName, err = c.clientModeSubPodManager.createClientDriverPod(app)
	} else {
		submissionID, driverPodName, err = c.subJobManager.createSubmissionJob(app)
	}

	if err != nil {
		if strings.Contains(err.Error(), "exceeded quota") && app.Spec.Mode == v1beta2.ClientMode && app.Spec.RestartPolicy.Type == v1beta2.OnFailure {
			if app.Status.SubmissionAttempts < *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				app.Status = v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State:        v1beta2.PendingSubmissionState,
						ErrorMessage: err.Error(),
					},
					SubmissionAttempts: app.Status.SubmissionAttempts + 1,
				}
			} else {
				app.Status = v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State:        v1beta2.FailedSubmissionState,
						ErrorMessage: err.Error(),
					},
					SubmissionAttempts: app.Status.SubmissionAttempts,
				}
			}
		} else if !errors.IsAlreadyExists(err) || app.Spec.Mode == v1beta2.ClientMode {
			app.Status = v1beta2.SparkApplicationStatus{
				AppState: v1beta2.ApplicationState{
					State:        v1beta2.FailedSubmissionState,
					ErrorMessage: err.Error(),
				},
				SubmissionAttempts: app.Status.SubmissionAttempts + 1,
			}
		}

		c.recordSparkApplicationEvent(app)
		return app
	}

	glog.Infof("SparkApplication %s/%s has been submitted", app.Namespace, app.Name)
	var appState v1beta2.ApplicationStateType
	if app.Spec.Mode == v1beta2.ClientMode {
		appState = v1beta2.SubmittedState
	} else {
		appState = v1beta2.PendingSubmissionState
	}
	app.Status = v1beta2.SparkApplicationStatus{
		SubmissionID:       submissionID,
		DriverInfo:         v1beta2.DriverInfo{PodName: driverPodName},
		AppState:           v1beta2.ApplicationState{State: appState},
		SubmissionAttempts: app.Status.SubmissionAttempts + 1,
		ExecutionAttempts:  app.Status.ExecutionAttempts + 1,
	}

	c.recordSparkApplicationEvent(app)
	if app.Spec.Mode == v1beta2.ClientMode {
		c.createSparkUIResources(app)
	}

	return app
}

func (c *Controller) createSparkUIResources(app *v1beta2.SparkApplication) {
	service, err := createSparkUIService(app, c.kubeClient)
	if err != nil {
		glog.Errorf("failed to create UI service for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		return
	}

	app.Status.DriverInfo.WebUIServiceName = service.serviceName
	app.Status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", service.serviceIP, app.Status.DriverInfo.WebUIPort)
	app.Status.DriverInfo.WebUIPort = service.servicePort
	// Create UI Ingress if ingress-format is set.
	if c.ingressURLFormat != "" {
		ingress, err := createSparkUIIngress(app, *service, c.ingressURLFormat, c.kubeClient)
		if err != nil {
			glog.Errorf("failed to create UI Ingress for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		} else {
			app.Status.DriverInfo.WebUIIngressAddress = ingress.ingressURL
			app.Status.DriverInfo.WebUIIngressName = ingress.ingressName
		}
	}
}

func (c *Controller) shouldDoBatchScheduling(app *v1beta2.SparkApplication) (bool, schedulerinterface.BatchScheduler) {
	if c.batchSchedulerMgr == nil || app.Spec.BatchScheduler == nil || *app.Spec.BatchScheduler == "" {
		return false, nil
	}

	scheduler, err := c.batchSchedulerMgr.GetScheduler(*app.Spec.BatchScheduler)
	if err != nil {
		glog.Errorf("failed to get batch scheduler for name %s", *app.Spec.BatchScheduler)
		return false, nil
	}
	return scheduler.ShouldSchedule(app), scheduler
}

func (c *Controller) updateApplicationStatusWithRetries(
	original *v1beta2.SparkApplication,
	updateFunc func(status *v1beta2.SparkApplicationStatus)) (*v1beta2.SparkApplication, error) {
	toUpdate := original.DeepCopy()
	updateErr := wait.ExponentialBackoff(retry.DefaultBackoff, func() (ok bool, err error) {
		updateFunc(&toUpdate.Status)
		if equality.Semantic.DeepEqual(original.Status, toUpdate.Status) {
			return true, nil
		}

		toUpdate, err = c.crdClient.SparkoperatorV1beta2().SparkApplications(original.Namespace).UpdateStatus(toUpdate)
		if err == nil {
			return true, nil
		}
		if !errors.IsConflict(err) {
			return false, err
		}

		// There was a conflict updating the SparkApplication, fetch the latest version from the API server.
		toUpdate, err = c.crdClient.SparkoperatorV1beta2().SparkApplications(original.Namespace).Get(original.Name, metav1.GetOptions{})
		if err != nil {
			glog.Errorf("failed to get SparkApplication %s/%s: %v", original.Namespace, original.Name, err)
			return false, err
		}

		// Retry with the latest version.
		return false, nil
	})

	if updateErr != nil {
		glog.Errorf("failed to update SparkApplication %s/%s: %v", original.Namespace, original.Name, updateErr)
		return nil, updateErr
	}

	return toUpdate, nil
}

// updateStatusAndExportMetrics updates the status of the SparkApplication and export the metrics.
func (c *Controller) updateStatusAndExportMetrics(oldApp, newApp *v1beta2.SparkApplication) error {
	// Skip update if nothing changed.
	if equality.Semantic.DeepEqual(oldApp.Status, newApp.Status) {
		return nil
	}

	oldStatusJSON, err := printStatus(&oldApp.Status)
	if err != nil {
		return err
	}
	newStatusJSON, err := printStatus(&newApp.Status)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Update the status of SparkApplication %s/%s from:\n%s\nto:\n%s", newApp.Namespace, newApp.Name, oldStatusJSON, newStatusJSON)
	updatedApp, err := c.updateApplicationStatusWithRetries(oldApp, func(status *v1beta2.SparkApplicationStatus) {
		*status = newApp.Status
	})
	if err != nil {
		return err
	}

	// Export metrics if the update was successful.
	if c.metrics != nil {
		c.metrics.exportMetrics(oldApp, updatedApp)
	}

	return nil
}

func (c *Controller) getSparkApplication(namespace string, name string) (*v1beta2.SparkApplication, error) {
	app, err := c.applicationLister.SparkApplications(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return app, nil
}

//Delete the optional UI resources (Service) to get rid of env vars set by kubernetes
func (c *Controller) deleteSparkUI(app *v1beta2.SparkApplication) error {
	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		glog.V(2).Infof("Deleting Spark UI Service %s in namespace %s", sparkUIServiceName, app.Namespace)
		err := c.kubeClient.CoreV1().Services(app.Namespace).Delete(sparkUIServiceName, metav1.NewDeleteOptions(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// Delete the driver pod and optional UI resources (Service/Ingress) created for the application.
func (c *Controller) deleteSparkResources(app *v1beta2.SparkApplication) error {
	err := c.subJobManager.deleteSubmissionJob(app)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	driverPodName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if driverPodName == "" {
		driverPodName = getDriverPodName(app)
	}

	glog.V(2).Infof("Deleting pod %s in namespace %s", driverPodName, app.Namespace)
	err = c.kubeClient.CoreV1().Pods(app.Namespace).Delete(driverPodName, &metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		glog.V(2).Infof("Deleting Spark UI Service %s in namespace %s", sparkUIServiceName, app.Namespace)
		err := c.kubeClient.CoreV1().Services(app.Namespace).Delete(sparkUIServiceName, metav1.NewDeleteOptions(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		glog.V(2).Infof("Deleting Spark UI Ingress %s in namespace %s", sparkUIIngressName, app.Namespace)
		err := c.kubeClient.ExtensionsV1beta1().Ingresses(app.Namespace).Delete(sparkUIIngressName, metav1.NewDeleteOptions(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (c *Controller) validateSparkApplication(app *v1beta2.SparkApplication) error {
	appSpec := app.Spec
	driverSpec := appSpec.Driver
	executorSpec := appSpec.Executor
	if appSpec.NodeSelector != nil && (driverSpec.NodeSelector != nil || executorSpec.NodeSelector != nil) {
		return fmt.Errorf("NodeSelector property can be defined at SparkApplication or at any of Driver,Executor")
	}

	return nil
}

// Validate that any Spark resources (driver/Service/Ingress) created for the application have been deleted.
func (c *Controller) validateSparkResourceDeletion(app *v1beta2.SparkApplication) bool {
	_, err := c.subJobManager.getSubmissionJob(app)
	if err == nil || !errors.IsNotFound(err) {
		return false
	}

	driverPodName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if driverPodName == "" {
		driverPodName = getDriverPodName(app)
	}
	_, err = c.kubeClient.CoreV1().Pods(app.Namespace).Get(driverPodName, metav1.GetOptions{})
	if err == nil || !errors.IsNotFound(err) {
		return false
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		_, err := c.kubeClient.CoreV1().Services(app.Namespace).Get(sparkUIServiceName, metav1.GetOptions{})
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		_, err := c.kubeClient.ExtensionsV1beta1().Ingresses(app.Namespace).Get(sparkUIIngressName, metav1.GetOptions{})
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	return true
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.AddRateLimited(key)
}

func (c *Controller) recordSparkApplicationEvent(app *v1beta2.SparkApplication) {
	switch app.Status.AppState.State {
	case v1beta2.NewState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationAdded",
			"SparkApplication %s was added, enqueuing it for submission",
			app.Name)
	case v1beta2.PendingSubmissionState:
		if app.Spec.Mode == v1beta2.ClientMode {
			c.recorder.Eventf(
				app,
				apiv1.EventTypeNormal,
				"Submitting Spark Application",
				"Submission SparkApplication %s is pending",
				app.Name)

		} else {
			c.recorder.Eventf(
				app,
				apiv1.EventTypeNormal,
				"SubmissionJobCreated",
				"Submission Job for SparkApplication %s was created",
				app.Name)
		}
	case v1beta2.SubmittedState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationSubmitted",
			"SparkApplication %s was submitted successfully",
			app.Name)
	case v1beta2.FailedSubmissionState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"failed to submit SparkApplication %s: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta2.CompletedState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationCompleted",
			"SparkApplication %s completed",
			app.Name)
	case v1beta2.FailedState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationFailed",
			"SparkApplication %s failed: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta2.PendingRerunState:
		c.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationPendingRerun",
			"SparkApplication %s is pending rerun",
			app.Name)
	}
}

func (c *Controller) recordDriverEvent(app *v1beta2.SparkApplication, phase v1beta2.DriverState, name string) {
	switch phase {
	case v1beta2.DriverCompletedState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	case v1beta2.DriverPendingState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverPending", "Driver %s is pending", name)
	case v1beta2.DriverRunningState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverRunning", "Driver %s is running", name)
	case v1beta2.DriverFailedState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	case v1beta2.DriverUnknownState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverUnknownState", "Driver %s in unknown state", name)
	}
}

func (c *Controller) recordExecutorEvent(app *v1beta2.SparkApplication, state v1beta2.ExecutorState, name string) {
	switch state {
	case v1beta2.ExecutorCompletedState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", name)
	case v1beta2.ExecutorPendingState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorPending", "Executor %s is pending", name)
	case v1beta2.ExecutorRunningState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorRunning", "Executor %s is running", name)
	case v1beta2.ExecutorFailedState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed", name)
	case v1beta2.ExecutorUnknownState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorUnknownState", "Executor %s in unknown state", name)
	}
}

func (c *Controller) clearStatus(status *v1beta2.SparkApplicationStatus) {
	if status.AppState.State == v1beta2.InvalidatingState {
		status.SparkApplicationID = ""
		status.ExecutionAttempts = 0
		status.SubmissionTime = metav1.Time{}
		status.TerminationTime = metav1.Time{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	} else if status.AppState.State == v1beta2.PendingRerunState {
		status.SparkApplicationID = ""
		status.DriverInfo = v1beta2.DriverInfo{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	}
}

func (c *Controller) hasApplicationExpired(app *v1beta2.SparkApplication) bool {
	// The application has no TTL defined and will never expire.
	if app.Spec.TimeToLiveSeconds == nil {
		return false
	}

	ttl := time.Duration(*app.Spec.TimeToLiveSeconds) * time.Second
	now := time.Now()
	if !app.Status.TerminationTime.IsZero() && now.Sub(app.Status.TerminationTime.Time) > ttl {
		return true
	}

	return false
}
