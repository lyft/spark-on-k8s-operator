package sparkapplication

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/webhook/resourceusage"
)

type clientSubmissionPodManager interface {
	createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error)
}

type realClientSubmissionPodManager struct {
	kubeClient kubernetes.Interface
	podLister  v1.PodLister
}

func (spm *realClientSubmissionPodManager) createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error) {
	var image string
	if app.Spec.Image != nil {
		image = *app.Spec.Image
	} else if app.Spec.Driver.Image != nil {
		image = *app.Spec.Driver.Image
	}
	if image == "" {
		return "", "", fmt.Errorf("no image specified in .spec.image or .spec.driver.image in SparkApplication %s/%s",
			app.Namespace, app.Name)
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		return "", "", err
	}

	command := []string{"sh", "-c", fmt.Sprintf("$SPARK_HOME/bin/spark-submit %s", strings.Join(submissionCmdArgs, " "))}

	labels := map[string]string{
		config.SparkAppNameLabel:            app.Name,
		config.LaunchedBySparkOperatorLabel: "true",
	}

	imagePullSecrets := make([]corev1.LocalObjectReference, len(app.Spec.ImagePullSecrets))
	for i, secret := range app.Spec.ImagePullSecrets {
		imagePullSecrets[i] = corev1.LocalObjectReference{Name: secret}
	}
	imagePullPolicy := corev1.PullIfNotPresent
	if app.Spec.ImagePullPolicy != nil {
		imagePullPolicy = corev1.PullPolicy(*app.Spec.ImagePullPolicy)
	}

	var driverCpuQuantity string
	if app.Spec.Driver.CoreRequest != nil {
		driverCpuQuantity = fmt.Sprintf("%d", *app.Spec.Driver.CoreRequest)
	} else {
		driverCpuQuantity = fmt.Sprintf("%d", *app.Spec.Driver.Cores)
	}

	var driverCpuQuantityLimit string
	if app.Spec.Driver.CoreLimit != nil {
		driverCpuQuantityLimit = *app.Spec.Driver.CoreLimit
	} else {
		driverCpuQuantityLimit = "1"
	}

	driverOverheadMemory, err :=
	 	resourceusage.MemoryRequiredForSparkPod(app.Spec.Driver.SparkPodSpec, app.Spec.MemoryOverheadFactor, app.Spec.Type, 1)
	if err != nil {
		return "", "", err
	}

	clientDriver := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driverPodName,
			Namespace:       app.Namespace,
			Labels:          labels,
			Annotations:     app.Annotations,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: corev1.PodSpec{
			ImagePullSecrets: imagePullSecrets,
			Containers: []corev1.Container{
				{
					Name:            "spark-kubernetes-driver",
					Image:           image,
					Command:         command,
					ImagePullPolicy: imagePullPolicy,
					Env: []corev1.EnvVar{
						{
							Name: "SPARK_K8S_DRIVER_POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverCpuQuantity),
							corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%d", driverOverheadMemory)),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverCpuQuantityLimit),
							corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%d", driverOverheadMemory)),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	if app.Spec.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.ServiceAccount
	} else if app.Spec.Driver.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.Driver.ServiceAccount
	}

	for key, val := range app.Labels {
		clientDriver.Labels[key] = val
	}

	glog.Infof("Creating the %s for running spark in client mode", clientDriver.Name)
	_, err = spm.kubeClient.CoreV1().Pods(app.Namespace).Create(clientDriver)
	if err != nil {
		return "", "", err
	}

	return submissionID, driverPodName, nil
}

