package main

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
)

// All waits are watch based so timestamps reflect when the API server reported the change,
// not when a poll happened to notice it.

func byName(name string) func(*metav1.ListOptions) {
	return func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	}
}

func podLW(client kubernetes.Interface, ns, name string) cache.ListerWatcher {
	return cache.NewFilteredListWatchFromClient(client.CoreV1().RESTClient(), "pods", ns, byName(name))
}

func pvcLW(client kubernetes.Interface, ns, name string) cache.ListerWatcher {
	return cache.NewFilteredListWatchFromClient(client.CoreV1().RESTClient(), "persistentvolumeclaims", ns, byName(name))
}

func pvLW(client kubernetes.Interface, name string) cache.ListerWatcher {
	return cache.NewFilteredListWatchFromClient(client.CoreV1().RESTClient(), "persistentvolumes", metav1.NamespaceAll, byName(name))
}

type podStart struct {
	scheduledAt time.Time
	runningAt   time.Time
	node        string
}

// waitPodRunning records when the pod was bound to a node and when it reached Running.
func waitPodRunning(ctx context.Context, client kubernetes.Interface, ns, name string) (podStart, error) {
	var st podStart
	_, err := watchtools.UntilWithSync(ctx, podLW(client, ns, name), &corev1.Pod{}, nil, func(ev watch.Event) (bool, error) {
		if ev.Type == watch.Deleted {
			return false, fmt.Errorf("pod was deleted")
		}
		pod, ok := ev.Object.(*corev1.Pod)
		if !ok {
			return false, nil
		}
		now := time.Now()
		if st.scheduledAt.IsZero() && pod.Spec.NodeName != "" {
			st.scheduledAt = now
			st.node = pod.Spec.NodeName
		}
		switch pod.Status.Phase {
		case corev1.PodRunning:
			st.runningAt = now
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fmt.Errorf("unexpected pod phase %s", pod.Status.Phase)
		}
		return false, nil
	})
	return st, err
}

// waitPodLeavesRunning returns (pod, nil) as soon as the pod is no longer Running (pod is nil if it
// was deleted), or an error once ctx ends with the pod still Running.
func waitPodLeavesRunning(ctx context.Context, client kubernetes.Interface, ns, name string) (*corev1.Pod, error) {
	var last *corev1.Pod
	_, err := watchtools.UntilWithSync(ctx, podLW(client, ns, name), &corev1.Pod{}, nil, func(ev watch.Event) (bool, error) {
		if ev.Type == watch.Deleted {
			last = nil
			return true, nil
		}
		pod, ok := ev.Object.(*corev1.Pod)
		if !ok {
			return false, nil
		}
		last = pod
		return pod.Status.Phase != corev1.PodRunning, nil
	})
	if err != nil {
		return nil, err
	}
	return last, nil
}

// watchPVCBinding returns when each of the cycle's PVCs was first seen Bound. It stops once all
// `want` claims are bound or ctx ends, returning whatever it observed.
func watchPVCBinding(ctx context.Context, client kubernetes.Interface, ns, cycleID string, want int) map[string]time.Time {
	bound := make(map[string]time.Time, want)
	lw := cache.NewFilteredListWatchFromClient(client.CoreV1().RESTClient(), "persistentvolumeclaims", ns,
		func(o *metav1.ListOptions) { o.LabelSelector = cycleLabelKey + "=" + cycleID })
	_, _ = watchtools.UntilWithSync(ctx, lw, &corev1.PersistentVolumeClaim{}, nil, func(ev watch.Event) (bool, error) {
		pvc, ok := ev.Object.(*corev1.PersistentVolumeClaim)
		if !ok {
			return false, nil
		}
		if _, seen := bound[pvc.Name]; !seen && pvc.Status.Phase == corev1.ClaimBound {
			bound[pvc.Name] = time.Now()
		}
		return len(bound) >= want, nil
	})
	return bound
}

// waitGone blocks until the object with the given store key no longer exists.
func waitGone(ctx context.Context, lw cache.ListerWatcher, objType runtime.Object, key string) error {
	_, err := watchtools.UntilWithSync(ctx, lw, objType,
		func(store cache.Store) (bool, error) {
			_, exists, err := store.GetByKey(key)
			return !exists, err
		},
		func(ev watch.Event) (bool, error) { return ev.Type == watch.Deleted, nil },
	)
	return err
}
