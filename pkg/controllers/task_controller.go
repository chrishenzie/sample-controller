/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	samplecontrollerv1alpha1 "k8s.io/sample-controller/pkg/apis/samplecontroller/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	informers "k8s.io/sample-controller/pkg/generated/informers/externalversions/samplecontroller/v1alpha1"
	listers "k8s.io/sample-controller/pkg/generated/listers/samplecontroller/v1alpha1"
)

const taskControllerAgentName = "task-controller"

const (
	taskMessageResourceSynced = "Task synced successfully"
)

type TaskController struct {
	kubeclientset   kubernetes.Interface
	sampleclientset clientset.Interface

	taskClustersLister listers.TaskClusterLister
	taskClustersSynced cache.InformerSynced
	podsLister         corelisters.PodLister
	podsSynced         cache.InformerSynced
	tasksLister        listers.TaskLister
	tasksSynced        cache.InformerSynced

	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	recorder  record.EventRecorder
}

func NewTaskController(
	ctx context.Context,
	kubeclientset kubernetes.Interface,
	sampleclientset clientset.Interface,
	taskClusterInformer informers.TaskClusterInformer,
	podInformer coreinformers.PodInformer,
	taskInformer informers.TaskInformer) *TaskController {
	logger := klog.FromContext(ctx)
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: taskControllerAgentName})
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	controller := &TaskController{
		kubeclientset:      kubeclientset,
		sampleclientset:    sampleclientset,
		taskClustersLister: taskClusterInformer.Lister(),
		taskClustersSynced: taskClusterInformer.Informer().HasSynced,
		podsLister:         podInformer.Lister(),
		podsSynced:         podInformer.Informer().HasSynced,
		tasksLister:        taskInformer.Lister(),
		tasksSynced:        taskInformer.Informer().HasSynced,
		workqueue:          workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:           recorder,
	}

	logger.Info("Setting up event handlers")
	taskInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueTask,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueTask(new)
		},
	})

	return controller
}

func (c *TaskController) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	logger.Info("Starting Task controller")
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.taskClustersSynced, c.tasksSynced, c.podsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

func (c *TaskController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *TaskController) processNextWorkItem(ctx context.Context) bool {
	objRef, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)
	if shutdown {
		return false
	}

	defer c.workqueue.Done(objRef)

	err := c.syncHandler(ctx, objRef)
	if err == nil {
		c.workqueue.Forget(objRef)
		logger.Info("Successfully synced", "objectName", objRef)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing; requeuing for later retry", "objectReference", objRef)
	c.workqueue.AddRateLimited(objRef)
	return true
}

func (c *TaskController) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	// logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)

	task, err := c.tasksLister.Tasks(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "Task referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	clusterName := task.Spec.ClusterName
	if clusterName == "" {
		utilruntime.HandleErrorWithContext(ctx, nil, "TaskCluster name missing from object reference", "objectReference", objectRef)
		return nil
	}

	taskCluster, err := c.taskClustersLister.TaskClusters(task.Namespace).Get(clusterName)
	if errors.IsNotFound(err) {
		// TODO: Do we need to HandleErrorWithContext?
		return err
	}

	if err != nil {
		return err
	}

	// Get available pods to schedule the task on.
	selector := labels.Set{"controller": taskCluster.GetName()}.AsSelector()
	pods, err := c.podsLister.Pods(taskCluster.Namespace).List(selector)
	if err != nil {
		return err
	}

	// TODO: Replace with more sophisticated approach.
	selectedPod := pods[0].DeepCopy()
	appendTaskContainer(task, selectedPod)

	_, err = c.kubeclientset.CoreV1().Pods(selectedPod.Namespace).Update(ctx, newDeployment(taskCluster), metav1.CreateOptions{FieldManager: FieldManager})

	c.recorder.Event(taskCluster, corev1.EventTypeNormal, SuccessSynced, taskMessageResourceSynced)
	return nil
}

func (c *TaskController) enqueueTask(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.Add(objectRef)
	}
}

func appendTaskContainer(task *samplecontrollerv1alpha1.Task, pod *corev1.Pod) {
	taskContainer := corev1.Container{}
	pod.Spec.Containers = append(pod.Spec.Containers, taskContainer)
}
