# Quick Start Guide

For a more detailed guide on how to use, compose, and work with `SparkApplication`s, please refer to the
[User Guide](user-guide.md). If you are running the Kubernetes Operator for Apache Spark on Google Kubernetes Engine and want to use Google Cloud Storage (GCS) and/or BigQuery for reading/writing data, also refer to the [GCP guide](gcp.md). The Kubernetes Operator for Apache Spark will simply be referred to as the operator for the rest of this guide.

## Table of Contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Upgrade](#upgrade)
4. [Running the Examples](#running-the-examples)
5. [Using the Mutating Admission Webhook](#using-the-mutating-admission-webhook)
6. [Build](#build)

## Installation

To install the operator, use the Helm [chart](https://github.com/helm/charts/tree/master/incubator/sparkoperator). 

```bash
$ helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
$ helm install incubator/sparkoperator
```

Installing the chart will create a namespace `spark-operator`, set up RBAC for the operator to run in the namespace. It will also set up RBAC for driver pods of your Spark applications to be able to manipulate executor pods. In addition, the chart will create a Deployment named `sparkoperator` in namespace `spark-operator`. The chart by default enables a [Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) for Spark pod customization. A webhook service called `spark-webhook` and a secret storing the x509 certificate called `spark-webhook-certs` are created for that purpose. To install the operator **without** the mutating admission webhook on a Kubernetes cluster, install the chart with the flag `enableWebhook=false`:

```bash
$ helm install incubator/sparkoperator --set enableWebhook=false
```

Due to a [known issue](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role) in GKE, you will need to first grant yourself cluster-admin privileges before you can create custom roles and role bindings on a GKE cluster versioned 1.6 and up. Run the following command before installing the chart on GKE:

```bash
$ kubectl create clusterrolebinding <user>-cluster-admin-binding --clusterrole=cluster-admin --user=<user>@<domain>
```

Now you should see the operator running in the cluster by checking the status of the Deployment.

```bash
$ kubectl describe deployment sparkoperator -n sparkoperator
```

### Metrics

The operator exposes a set of metrics via the metric endpoint to be scraped by `Prometheus`. The Helm chart by default installs the operator with the additional flag to enable metrics (`-enable-metrics=true`) as well as other annotations used by Prometheus to scrape the metric endpoint. To install the operator  **without** metrics enabled, pass the appropriate flag during `helm install`:

```bash
$ helm install incubator/sparkoperator --set enableMetrics=false
```

If enabled, the operator generates the following metrics:

| Metric | Description |
| ------------- | ------------- |
| `spark_app_submit_count`  | Total number of SparkApplication submitted by the Operator.|
| `spark_app_success_count` | Total number of SparkApplication which completed successfully.|
| `spark_app_failure_count` | Total number of SparkApplication which failed to complete. |
| `spark_app_submission_failure_count` | Total number of SparkApplication attempts which failed submission. |
| `spark_app_running_count` | Total number of SparkApplication which are currently running.|
| `spark_app_success_execution_time_microseconds` | Execution time for applications which succeeded.|
| `spark_app_failure_execution_time_microseconds` |Execution time for applications which failed. |
| `spark_app_executor_success_count` | Total number of Spark Executors which completed successfully. |
| `spark_app_executor_failure_count` | Total number of Spark Executors which failed. |
| `spark_app_executor_running_count` | Total number of Spark Executors which are currently running. |

The following is a list of all the configurations the operators supports for metrics: 

```bash
-enable-metrics=true
-metrics-port=10254
-metrics-endpoint=/metrics
-metrics-prefix=myServiceName 
-metrics-label=label1Key
-metrics-label=label2Key
```
All configs except `-enable-metrics` are optional. If port and/or endpoint are specified, please ensure that the annotations `prometheus.io/port`,  `prometheus.io/path` and `containerPort` in `spark-operator-with-metrics.yaml` are updated as well.

A note about `metrics-labels`: In `Prometheus`, every unique combination of key-value label pair represents a new time series, which can dramatically increase the amount of data stored.  Hence labels should not be used to store dimensions with high cardinality with potentially a large or unbounded value range.

Additionally, these metrics are best-effort for the current operator run and will be reset on an operator restart. Also some of these metrics are generated by listening to pod state updates for the driver/executors
and deleting the pods outside the operator might lead to incorrect metric values for some of these metrics.


## UI Access and Ingress
The operator, by default, makes the Spark UI accessible by creating a service of type `NodePort` which exposes the UI via the node running the driver.
The operator also supports creating an Ingress for the UI. This can be turned on by setting the `ingress-url-format` command-line flag. The `ingress-url-format`
should be a template like `{{$appName}}.ingress.cluster.com` and the operator will replace the `{{$appName}}` with the appropriate appName.

The operator also sets both `WebUIAddress` which uses the Node's public IP as well as `WebUIIngressAddress` as part of the `DriverInfo` field of the `SparkApplication` CRD.

## Configuration

This operator branch is built on [Spark 2.2 fork with Kubernetes support](https://github.com/apache-spark-on-k8s/spark) which supports PySpark executions.

Spark Operator is typically deployed and run using `manifest/spark-operator.yaml` through a Kubernetes `Deployment`. However, users can still run it outside a Kubernetes cluster and make it talk to the Kubernetes API server of a cluster
by specifying path to `kubeconfig`, which can be done using the `-kubeconfig` flag.

Spark Operator uses multiple workers in the `SparkApplication` controller. The number of worker threads are controlled using command-line flag `-controller-threads` which has a default value of 10.

The operator enables cache resynchronization so periodically the informers used by the operator will re-list existing objects it manages and re-trigger resource events. The resynchronization interval in seconds can be configured using the flag `-resync-interval`, with a default value of 30 seconds.

By default, the operator will install the [CustomResourceDefinitions](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) for the custom resources it managers. This can be disabled by setting the flag `-install-crds=false.`.

The mutating admission webhook is an **optional** component and can be enabled or disabled using the `-enable-webhook` flag, which defaults to `false`.

By default, the operator will manage custom resource objects of the managed CRD types for the whole cluster. It can be configured to manage only the custom resource objects in a specific namespace with the flag `-namespace=<namespace>`

## Upgrade

To upgrade the the operator, e.g., to use a newer version container image with a new tag, run the following command with updated parameters for the Helm release: 

```bash
$ helm upgrade <YOUR-HELM-RELEASE-NAME> --set operatorImageName=org/image --set operatorVersion=newTag
```

Refer to the Helm [documentation](https://docs.helm.sh/helm/#helm-upgrade) for more details on `helm upgrade`.

## Running the Examples

To run the Spark Pi example, run the following command:

```bash
$ kubectl apply -f examples/spark-pi.yaml
```

This will create a `SparkApplication` object named `spark-pi`. Check the object by running the following command:

```bash
$ kubectl get sparkapplications spark-pi -o=yaml
```

This will show something similar to the following:

```yaml
apiVersion: sparkoperator.k8s.io/v1alpha1
kind: SparkApplication
metadata:
  ...
spec:
  deps: {}
  driver:
    coreLimit: 200m
    cores: 0.1
    labels:
      version: 2.3.0
    memory: 512m
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    labels:
      version: 2.3.0
    memory: 512m
  image: gcr.io/ynli-k8s/spark:v2.3.0
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar
  mainClass: org.apache.spark.examples.SparkPi
  mode: cluster
  restartPolicy:
      type: OnFailure
      onFailureRetries: 3
      onFailureRetryInterval: 10
      onSubmissionFailureRetries: 5
      onSubmissionFailureRetryInterval: 20
  type: Scala
status:
  sparkApplicationId: spark-5f4ba921c85ff3f1cb04bef324f9154c9
  applicationState:
    state: COMPLETED
  completionTime: 2018-02-20T23:33:55Z
  driverInfo:
    podName: spark-pi-83ba921c85ff3f1cb04bef324f9154c9-driver
    webUIAddress: 35.192.234.248:31064
    webUIPort: 31064
    webUIServiceName: spark-pi-2402118027-ui-svc
    webUIIngressName: spark-pi-ui-ingress
    webUIIngressAddress: spark-pi.ingress.cluster.com
  executorState:
    spark-pi-83ba921c85ff3f1cb04bef324f9154c9-exec-1: COMPLETED
  submissionTime: 2018-02-20T23:32:27Z
```

To check events for the `SparkApplication` object, run the following command:

```bash
$ kubectl describe sparkapplication spark-pi
```

This will show the events similarly to the following:

```
Events:
  Type    Reason                      Age   From            Message
  ----    ------                      ----  ----            -------
  Normal  SparkApplicationAdded       5m    spark-operator  SparkApplication spark-pi was added, enqueued it for submission
  Normal  SparkApplicationTerminated  4m    spark-operator  SparkApplication spark-pi terminated with state: COMPLETED
```

## Using the Mutating Admission Webhook

The Kubernetes Operator for "Apache Spark comes with an optional mutating admission webhook for customizing Spark driver and executor pods based on the specification in `SparkApplication` objects, e.g., mounting user-specified ConfigMaps and volumes, and setting pod affinity/anti-affinity.

The webhook requires a X509 certificate for TLS for pod admission requests and responses between the Kubernetes API server and the webhook server running inside the operator. For that, the certificate and key files must be accessible by the webhook server and a Kubernetes secret can be used to store the files.

The operator ships with a tool at `hack/gencerts.sh` for generating the CA and server certificate and putting the certificate and key files into a secret. Running `hack/gencerts.sh` will generate a CA certificate and a certificate for the webhook server signed by the CA, and create a secret named `spark-webhook-certs` in namespace `sparkoperator`. This secret will be mounted into the operator pod.  

With the secret storing the certificate and key files available, run the following command to install the operatorr with the mutating admission webhook:

```bash
$ kubectl apply -f manifest/spark-operator-with-webhook.yaml
```

This will create a Deployment named `sparkoperator` and a Service named `spark-webhook` for the webhook in namespace `sparkoperator`.

If the operator is installed via the Helm chart using the default settings (i.e. with webhook enabled), the above steps are all automated for you.

## Build

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build without worrying about dependencies is to just build the Dockerfile.

```bash
$ docker build -t <image-tag> .
```

The operator image is built upon a base Spark image that defaults to `gcr.io/spark-operator/spark:v2.3.1`. If you want to use your own Spark image (e.g., an image with a different version of Spark or some custom dependencies), specify the argument `SPARK_IMAGE` as the following example shows: 

```bash
$ docker build --build-arg SPARK_IMAGE=<your Spark image> -t <image-tag> .
```

If you'd like to build/test the spark-operator locally, follow the instructions below:

```bash
$ mkdir -p $GOPATH/src/k8s.io
$ cd $GOPATH/src/k8s.io
$ git clone git@github.com:GoogleCloudPlatform/spark-on-k8s-operator.git
```

The operator uses [dep](https://golang.github.io/dep/) for dependency management. Please install `dep` following
the instruction on the website if you don't have it available locally. To install the dependencies, run the following command:

```bash
$ dep ensure
```

To update the dependencies, run the following command. (You can skip this unless you know there's a dependency that needs updating):

```bash
$ dep ensure -update
```

Before building the operator the first time, run the following commands to get the required Kubernetes code generators:

```bash
$ go get -u k8s.io/code-generator/cmd/client-gen
$ go get -u k8s.io/code-generator/cmd/deepcopy-gen
$ go get -u k8s.io/code-generator/cmd/defaulter-gen
```

To update the auto-generated code, run the following command. (This step is only required if the CRD types have been changed):

```bash
$ go generate
```

You can verify the current auto-generated code is up to date with:

```bash
$ hack/verify-codegen.sh
```

To build the operator, run the following command:

```bash
$ GOOS=linux go build -o spark-operator
```

To run unit tests, run the following command:

```bash
$ go test ./...
```