package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	trackingLabelKey = "csi-test-run"
	trackingLabelVal = "active"
)

var (
	successfulCycles uint64
	failedCycles     uint64
)

type Config struct {
	NamespacePrefix    string
	NamespaceCount     int
	StorageClass       string
	VolumeCapacity     string
	ConcurrentWorkers  int
	Duration           time.Duration
	MinHoldDuration    time.Duration
	MaxHoldDuration    time.Duration
	MinPvcCount        int
	MaxPvcCount        int
	Kubeconfig         string
	CleanUpOnExit      bool
}

func main() {
	cfg := parseFlags()

	client, err := buildKubeClient(cfg.Kubeconfig)
	if err != nil {
		fmt.Printf("Failed to create kubernetes client: %v\n", err)
		os.Exit(1)
	}

	namespaces := generateNamespaceNames(cfg.NamespacePrefix, cfg.NamespaceCount)

	fmt.Printf("Ensuring %d namespaces (prefix: '%s') exist...\n", len(namespaces), cfg.NamespacePrefix)
	for _, ns := range namespaces {
		if err := ensureNamespace(context.Background(), client, ns); err != nil {
			fmt.Printf("Failed to verify/create target namespace %s: %v\n", ns, err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
		fmt.Printf("Running test for %v with %d concurrent workers across %d namespaces...\n",
			cfg.Duration, cfg.ConcurrentWorkers, len(namespaces))
	} else {
		fmt.Printf("Running test continuously with %d concurrent workers across %d namespaces...\n",
			cfg.ConcurrentWorkers, len(namespaces))
	}

	go runReporter(ctx, 10*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < cfg.ConcurrentWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerLoop(ctx, client, cfg, namespaces, workerID)
		}(i)
	}

	// Wait for OS interruption or timeout expiration
	<-ctx.Done()
	fmt.Println("\nTermination signal received or duration expired.")
	stop() // Unregister signal handlers so a secondary Ctrl+C doesn't abort the cleanup sequence

	fmt.Println("Waiting for active worker loops to exit...")
	wg.Wait()

	if cfg.CleanUpOnExit {
		fmt.Println("Performing final teardown of pods, PVCs, and generated namespaces...")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cleanupCancel()
		cleanupAllStressResources(cleanupCtx, client, namespaces)
	}

	fmt.Printf("\n--- Test Complete ---\nSuccessful Lifecycles: %d\nFailed Lifecycles: %d\n",
		atomic.LoadUint64(&successfulCycles), atomic.LoadUint64(&failedCycles))
}

func parseFlags() Config {
	var cfg Config
	defaultKube := ""
	if home := homedir.HomeDir(); home != "" {
		defaultKube = filepath.Join(home, ".kube", "config")
	}

	flag.StringVar(&cfg.NamespacePrefix, "namespace-prefix", "csi-stress", "Common prefix/basename for namespaces")
	flag.IntVar(&cfg.NamespaceCount, "namespace-count", 5, "Number of namespaces to generate from basename")
	flag.StringVar(&cfg.StorageClass, "storage-class", "", "StorageClass to use for dynamic provisioning (empty for cluster default)")
	flag.StringVar(&cfg.VolumeCapacity, "volume-size", "1Gi", "Size of each PVC (e.g. 1Gi, 500Mi)")
	flag.IntVar(&cfg.ConcurrentWorkers, "workers", 50, "Number of concurrent goroutine workers")
	flag.DurationVar(&cfg.Duration, "duration", 0, "Test duration (e.g. 30m, 2h). 0 means indefinite until Ctrl+C")
	flag.DurationVar(&cfg.MinHoldDuration, "min-hold", 10*time.Second, "Minimum pod I/O execution time before deletion")
	flag.DurationVar(&cfg.MaxHoldDuration, "max-hold", 45*time.Second, "Maximum pod I/O execution time before deletion")
	flag.IntVar(&cfg.MinPvcCount, "min-pvc", 2, "Minimum PVCs per pod")
	flag.IntVar(&cfg.MaxPvcCount, "max-pvc", 3, "Maximum PVCs per pod")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", defaultKube, "Path to the kubeconfig file")
	flag.BoolVar(&cfg.CleanUpOnExit, "cleanup", true, "Delete lingering stress pods, PVCs, and namespaces upon completion")

	flag.Parse()

	if cfg.NamespaceCount < 1 {
		cfg.NamespaceCount = 1
	}
	return cfg
}

func generateNamespaceNames(prefix string, count int) []string {
	list := make([]string, count)
	for i := 0; i < count; i++ {
		list[i] = fmt.Sprintf("%s-%d", prefix, i+1)
	}
	return list
}

func buildKubeClient(kubeconfigPath string) (*kubernetes.Clientset, error) {
	var restCfg *rest.Config
	var err error

	if _, statErr := os.Stat(kubeconfigPath); statErr == nil {
		restCfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		restCfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}

	// High client throughput to sustain rapid multi-worker cycles
	restCfg.QPS = 500.0
	restCfg.Burst = 1000

	return kubernetes.NewForConfig(restCfg)
}

func ensureNamespace(ctx context.Context, client *kubernetes.Clientset, ns string) error {
	_, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if errors.IsNotFound(err) {
		nsObj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "csi-stress-test",
					trackingLabelKey:               trackingLabelVal,
				},
			},
		}
		_, err = client.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
		return err
	}
	return err
}

func workerLoop(ctx context.Context, client *kubernetes.Clientset, cfg Config, namespaces []string, workerID int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			jitter := time.Duration(r.Intn(3000)) * time.Millisecond
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return
			}

			targetNS := namespaces[r.Intn(len(namespaces))]

			err := executeLifecycle(ctx, client, cfg, targetNS, r, workerID)
			if err != nil {
				atomic.AddUint64(&failedCycles, 1)
			} else {
				atomic.AddUint64(&successfulCycles, 1)
			}
		}
	}
}

func executeLifecycle(ctx context.Context, client *kubernetes.Clientset, cfg Config, namespace string, r *rand.Rand, workerID int) error {
	cycleID := fmt.Sprintf("w%d-%x", workerID, r.Uint32())
	pvcCount := cfg.MinPvcCount
	if cfg.MaxPvcCount > cfg.MinPvcCount {
		pvcCount = cfg.MinPvcCount + r.Intn(cfg.MaxPvcCount-cfg.MinPvcCount+1)
	}

	pvcNames := make([]string, pvcCount)
	for i := 0; i < pvcCount; i++ {
		pvcNames[i] = fmt.Sprintf("pvc-%s-%d", cycleID, i)
	}
	podName := fmt.Sprintf("pod-%s", cycleID)

	// Ensure cleanup executes even if cycle encounters an issue
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		_ = client.CoreV1().Pods(namespace).Delete(delCtx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: new(int64),
		})
		for _, pvc := range pvcNames {
			_ = client.CoreV1().PersistentVolumeClaims(namespace).Delete(delCtx, pvc, metav1.DeleteOptions{})
		}
	}()

	// 1. Create PersistentVolumeClaims
	for _, pvcName := range pvcNames {
		pvc := buildPVCSpec(cfg, pvcName)
		_, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("[%s] failed to create PVC %s: %w", namespace, pvcName, err)
		}
	}

	// 2. Create Pod with anti-affinity
	pod := buildPodSpec(namespace, podName, pvcNames)
	_, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("[%s] failed to create pod %s: %w", namespace, podName, err)
	}

	// 3. Wait for Pod to reach Running status
	running, err := waitForPodRunning(ctx, client, namespace, podName, 3*time.Minute)
	if err != nil || !running {
		return fmt.Errorf("[%s] pod %s failed to run: %v", namespace, podName, err)
	}

	// 4. Randomized duration for sustained read/write cycles
	holdRange := int(cfg.MaxHoldDuration - cfg.MinHoldDuration)
	var holdDuration time.Duration
	if holdRange > 0 {
		holdDuration = cfg.MinHoldDuration + time.Duration(r.Intn(holdRange))
	} else {
		holdDuration = cfg.MinHoldDuration
	}

	select {
	case <-time.After(holdDuration):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func buildPVCSpec(cfg Config, name string) *corev1.PersistentVolumeClaim {
	var scName *string
	if cfg.StorageClass != "" {
		scName = &cfg.StorageClass
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				trackingLabelKey: trackingLabelVal,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(cfg.VolumeCapacity),
				},
			},
		},
	}
}

func buildPodSpec(namespace, podName string, pvcNames []string) *corev1.Pod {
	volumes := make([]corev1.Volume, len(pvcNames))
	mounts := make([]corev1.VolumeMount, len(pvcNames))

	script := `
set -e
while true; do
  for dir in /data/*; do
    if [ -d "$dir" ]; then
      FILE="$dir/testfile_$(date +%s%N)"
      dd if=/dev/urandom of="$FILE" bs=1M count=10 oflag=sync status=none
      md5sum "$FILE" > /dev/null
      rm -f "$FILE"
    fi
  done
  sleep 1
done
`

	for i, pvc := range pvcNames {
		volName := fmt.Sprintf("vol-%d", i)
		volumes[i] = corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc,
				},
			},
		}
		mounts[i] = corev1.VolumeMount{
			Name:      volName,
			MountPath: fmt.Sprintf("/data/vol%d", i),
		}
	}

	// Soft pod anti-affinity to spread test pods across all bare-metal nodes
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      trackingLabelKey,
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{trackingLabelVal},
								},
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				trackingLabelKey: trackingLabelVal,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Affinity:      affinity,
			Containers: []corev1.Container{
				{
					Name:            "io-runner",
					Image:           "registry.access.redhat.com/ubi9/ubi-minimal:latest",
					Command:         []string{"/bin/sh", "-c", script},
					VolumeMounts:    mounts,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("20m"),
							corev1.ResourceMemory: resource.MustParse("32Mi"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: new(bool),
					},
				},
			},
			Volumes: volumes,
		},
	}
}

func waitForPodRunning(ctx context.Context, client *kubernetes.Clientset, ns, name string, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(1500 * time.Millisecond):
		}

		pod, err := client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return false, err
		}

		if pod.Status.Phase == corev1.PodRunning {
			return true, nil
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("unexpected pod phase %s", pod.Status.Phase)
		}
	}
	return false, fmt.Errorf("timeout waiting for pod %s to be running", name)
}

func runReporter(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			succ := atomic.LoadUint64(&successfulCycles)
			fail := atomic.LoadUint64(&failedCycles)
			fmt.Printf("[%s] Metrics -> Completed Cycles: %d | Failures/Timeouts: %d\n",
				time.Now().Format("15:04:05"), succ, fail)
		}
	}
}

func cleanupAllStressResources(ctx context.Context, client *kubernetes.Clientset, namespaces []string) {
	opts := metav1.DeleteOptions{GracePeriodSeconds: new(int64)}
	listOpts := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", trackingLabelKey, trackingLabelVal)}

	var wg sync.WaitGroup
	for _, ns := range namespaces {
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()
			fmt.Printf("[%s] Starting cleanup...\n", namespace)

			// 1. Delete all stress pods in this namespace
			_ = client.CoreV1().Pods(namespace).DeleteCollection(ctx, opts, listOpts)

			// 2. Wait until pods are gone so CSI drivers can unmount attachments
			waitForPodsDeleted(ctx, client, namespace, listOpts)

			// 3. Delete all PVCs (avoids pvc-protection deadlocks)
			_ = client.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, listOpts)

			// 4. Delete the namespace
			err := client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				fmt.Printf("[%s] Failed to delete namespace: %v\n", namespace, err)
			} else {
				fmt.Printf("[%s] Namespace deletion initiated.\n", namespace)
			}
		}(ns)
	}
	wg.Wait()
	fmt.Println("Cleanup across all test namespaces completed.")
}

func waitForPodsDeleted(ctx context.Context, client *kubernetes.Clientset, namespace string, listOpts metav1.ListOptions) {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}

		pods, err := client.CoreV1().Pods(namespace).List(ctx, listOpts)
		if err != nil || len(pods.Items) == 0 {
			return
		}
	}
}