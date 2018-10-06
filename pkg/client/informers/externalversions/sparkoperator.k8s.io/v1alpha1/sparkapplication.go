// Code generated by k8s code-generator DO NOT EDIT.

/*
Copyright 2018 Google LLC

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

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha1

import (
	time "time"

	sparkoperator_k8s_io_v1alpha1 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	versioned "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	internalinterfaces "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions/internalinterfaces"
	v1alpha1 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// SparkApplicationInformer provides access to a shared informer and lister for
// SparkApplications.
type SparkApplicationInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha1.SparkApplicationLister
}

type sparkApplicationInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewSparkApplicationInformer constructs a new informer for SparkApplication type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewSparkApplicationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredSparkApplicationInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredSparkApplicationInformer constructs a new informer for SparkApplication type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredSparkApplicationInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkoperatorV1alpha1().SparkApplications(namespace).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkoperatorV1alpha1().SparkApplications(namespace).Watch(options)
			},
		},
		&sparkoperator_k8s_io_v1alpha1.SparkApplication{},
		resyncPeriod,
		indexers,
	)
}

func (f *sparkApplicationInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredSparkApplicationInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *sparkApplicationInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&sparkoperator_k8s_io_v1alpha1.SparkApplication{}, f.defaultInformer)
}

func (f *sparkApplicationInformer) Lister() v1alpha1.SparkApplicationLister {
	return v1alpha1.NewSparkApplicationLister(f.Informer().GetIndexer())
}
