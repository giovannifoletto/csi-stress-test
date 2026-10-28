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
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	trackingLabelKey = "csi-test-run"
	trackingLabelVal = "active"
	cycleLabelKey    = "csi-stress-cycle"
	managedByKey     = "app.kubernetes.io/managed-by"
	managedByVal     = "csi-stress-test"

	spreadBalanced = "balanced"
	spreadSoft     = "soft"
	spreadNone     = "none"
)

type Config struct {
	NamespacePrefix   string
	NamespaceCount    int
	StorageClass      string
	VolumeCapacity    string
	ConcurrentWorkers int
	Duration          time.Duration
	MinHoldDuration   time.Duration
	MaxHoldDuration   time.Duration
	MinPvcCount       int
	MaxPvcCount       int
	Kubeconfig        string
	CleanUpOnExit     bool
	Image             string
	IOSizeMiB         int
	Spread            string
	NodeSelector      string
	StartupTimeout    time.Duration
	TeardownTimeout   time.Duration
	ReportInterval    time.Duration
	ResultsFile       string
	RunAsUser         int64

	volumeQuantity resource.Quantity
	nodeSelector   labels.Selector
}

// env bundles the shared state every worker needs.
type env struct {
	client   kubernetes.Interface
	cfg      Config
	metrics  *metrics
	balancer *nodeBalancer // nil unless --spread=balanced
	results  *resultsWriter
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(2)
	}

	client, err := buildKubeClient(cfg.Kubeconfig)
	if err != nil {
		fmt.Printf("Failed to create kubernetes client: %v\n", err)
		os.Exit(1)
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer setupCancel()

	sc, err := resolveStorageClass(setupCtx, client, cfg.StorageClass)
	if err != nil {
		fmt.Printf("Failed to resolve StorageClass: %v\n", err)
		os.Exit(1)
	}
	driver := describeStorageClass(sc, cfg)

	namespaces := generateNamespaceNames(cfg.NamespacePrefix, cfg.NamespaceCount)
	fmt.Printf("Ensuring %d namespaces (prefix: '%s') exist...\n", len(namespaces), cfg.NamespacePrefix)
	for _, ns := range namespaces {
		if err := ensureNamespace(setupCtx, client, ns); err != nil {
			fmt.Printf("Namespace %s: %v\n", ns, err)
			os.Exit(1)
		}
	}

	e := &env{client: client, cfg: cfg, metrics: newMetrics()}

	if cfg.ResultsFile != "" {
		e.results, err = newResultsWriter(cfg.ResultsFile)
		if err != nil {
			fmt.Printf("Failed to open results file: %v\n", err)
			os.Exit(1)
		}
		defer e.results.Close()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Spread == spreadBalanced {
		e.balancer, err = newNodeBalancer(setupCtx, client, cfg.nodeSelector, driver)
		if err != nil {
			fmt.Printf("Failed to initialise node balancer: %v\n", err)
			os.Exit(1)
		}
		e.balancer.printNodes()
		go e.balancer.run(ctx, 30*time.Second)
	}

	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
		fmt.Printf("Running test for %v with %d concurrent workers across %d namespaces (spread: %s)...\n",
			cfg.Duration, cfg.ConcurrentWorkers, len(namespaces), cfg.Spread)
	} else {
		fmt.Printf("Running test continuously with %d concurrent workers across %d namespaces (spread: %s)...\n",
			cfg.ConcurrentWorkers, len(namespaces), cfg.Spread)
	}

	go runReporter(ctx, e, cfg.ReportInterval)

	var wg sync.WaitGroup
	for i := 0; i < cfg.ConcurrentWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerLoop(ctx, e, namespaces, workerID)
		}(i)
	}

	// Wait for OS interruption or timeout expiration
	<-ctx.Done()
	fmt.Println("\nTermination signal received or duration expired.")
	// Restore default signal handling: a second Ctrl+C now kills the process immediately
	// (skipping teardown and leaving resources behind).
	stop()

	fmt.Println("Waiting for in-flight cycles to tear down (press Ctrl+C again to force exit)...")
	wg.Wait()

	if cfg.CleanUpOnExit {
		fmt.Println("Performing final teardown of pods, PVCs, and generated namespaces...")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cleanupCancel()
		cleanupAllStressResources(cleanupCtx, client, namespaces)
	}

	fmt.Println("\n--- Test Complete ---")
	e.metrics.printSummary(os.Stdout)
	if e.balancer != nil {
		e.balancer.printNodes()
	}
}

func parseFlags() (Config, error) {
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
	flag.StringVar(&cfg.Image, "image", "registry.access.redhat.com/ubi9/ubi-minimal:latest", "Container image for the I/O pod (needs sh, dd, md5sum, date)")
	flag.IntVar(&cfg.IOSizeMiB, "io-size", 10, "Size in MiB of each file written/read per I/O iteration")
	flag.StringVar(&cfg.Spread, "spread", spreadBalanced, "Pod placement: 'balanced' (pin each pod to the least-loaded node), 'soft' (preferred anti-affinity), 'none'")
	flag.StringVar(&cfg.NodeSelector, "node-selector", "", "Label selector restricting the nodes used by --spread=balanced (e.g. 'node-role.kubernetes.io/worker')")
	flag.DurationVar(&cfg.StartupTimeout, "startup-timeout", 3*time.Minute, "Maximum time for PVCs to bind and the pod to reach Running")
	flag.DurationVar(&cfg.TeardownTimeout, "teardown-timeout", 5*time.Minute, "Maximum time for pod, PVCs and PVs of a cycle to be deleted")
	flag.DurationVar(&cfg.ReportInterval, "report-interval", 30*time.Second, "Interval between progress reports")
	flag.Int64Var(&cfg.RunAsUser, "run-as-user", 1000, "Non-root UID (also used as GID and fsGroup) for the I/O pod")
	flag.StringVar(&cfg.ResultsFile, "results-file", "", "Optional path of a JSON Lines file receiving one record per cycle")

	flag.Parse()

	var err error
	if cfg.NamespaceCount < 1 {
		return cfg, fmt.Errorf("--namespace-count must be >= 1")
	}
	if cfg.ConcurrentWorkers < 1 {
		return cfg, fmt.Errorf("--workers must be >= 1")
	}
	if cfg.MinPvcCount < 1 || cfg.MaxPvcCount < cfg.MinPvcCount {
		return cfg, fmt.Errorf("--min-pvc must be >= 1 and --max-pvc must be >= --min-pvc")
	}
	if cfg.MinHoldDuration <= 0 || cfg.MaxHoldDuration < cfg.MinHoldDuration {
		return cfg, fmt.Errorf("--min-hold must be > 0 and --max-hold must be >= --min-hold")
	}
	if cfg.StartupTimeout <= 0 || cfg.TeardownTimeout <= 0 || cfg.ReportInterval <= 0 {
		return cfg, fmt.Errorf("--startup-timeout, --teardown-timeout and --report-interval must be > 0")
	}
	if cfg.volumeQuantity, err = resource.ParseQuantity(cfg.VolumeCapacity); err != nil {
		return cfg, fmt.Errorf("--volume-size %q: %w", cfg.VolumeCapacity, err)
	}
	if cfg.RunAsUser < 1 {
		return cfg, fmt.Errorf("--run-as-user must be a non-root UID (>= 1)")
	}
	if cfg.IOSizeMiB < 1 {
		return cfg, fmt.Errorf("--io-size must be >= 1")
	}
	// The test file plus a 1MiB direct-I/O probe must fit, leaving headroom for filesystem metadata.
	if int64(cfg.IOSizeMiB+1)<<20 > cfg.volumeQuantity.Value()*9/10 {
		return cfg, fmt.Errorf("--io-size %dMiB does not fit comfortably in --volume-size %s", cfg.IOSizeMiB, cfg.VolumeCapacity)
	}
	switch cfg.Spread {
	case spreadBalanced, spreadSoft, spreadNone:
	default:
		return cfg, fmt.Errorf("--spread must be one of balanced, soft, none")
	}
	if cfg.nodeSelector, err = labels.Parse(cfg.NodeSelector); err != nil {
		return cfg, fmt.Errorf("--node-selector: %w", err)
	}
	return cfg, nil
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

// resolveStorageClass returns the named StorageClass, or the cluster default when name is empty.
// It returns nil (no error) if no default exists, in which case PVCs rely on pre-provisioned PVs.
func resolveStorageClass(ctx context.Context, client kubernetes.Interface, name string) (*storagev1.StorageClass, error) {
	if name != "" {
		return client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	}
	list, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		a := list.Items[i].Annotations
		if a["storageclass.kubernetes.io/is-default-class"] == "true" || a["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// describeStorageClass prints the provisioning setup and returns the CSI driver name ("" if unknown).
func describeStorageClass(sc *storagev1.StorageClass, cfg Config) string {
	if sc == nil {
		fmt.Println("WARNING: no StorageClass given and no cluster default found; PVCs will only bind to pre-provisioned PVs.")
		return ""
	}
	mode := storagev1.VolumeBindingImmediate
	if sc.VolumeBindingMode != nil {
		mode = *sc.VolumeBindingMode
	}
	reclaim := corev1.PersistentVolumeReclaimDelete
	if sc.ReclaimPolicy != nil {
		reclaim = *sc.ReclaimPolicy
	}
	fmt.Printf("StorageClass %s: provisioner=%s bindingMode=%s reclaimPolicy=%s\n", sc.Name, sc.Provisioner, mode, reclaim)
	if reclaim != corev1.PersistentVolumeReclaimDelete {
		fmt.Println("WARNING: reclaimPolicy is not Delete; PVs will accumulate and pv_release will not be measured.")
	}
	if cfg.Spread == spreadBalanced && mode == storagev1.VolumeBindingImmediate {
		fmt.Println("WARNING: bindingMode=Immediate provisions volumes before the pod's node is chosen; " +
			"with topology-constrained drivers pinned pods may become unschedulable.")
	}
	return sc.Provisioner
}

func ensureNamespace(ctx context.Context, client kubernetes.Interface, ns string) error {
	existing, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		// Never adopt a namespace we did not create: cleanup would delete it.
		if existing.Labels[managedByKey] != managedByVal {
			return fmt.Errorf("already exists and is not labelled %s=%s; refusing to use it (choose another --namespace-prefix)",
				managedByKey, managedByVal)
		}
		if existing.Status.Phase == corev1.NamespaceTerminating {
			return fmt.Errorf("is still terminating from a previous run; wait for it to disappear")
		}
		return nil
	}
	if apierrors.IsNotFound(err) {
		nsObj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
				Labels: map[string]string{
					managedByKey:     managedByVal,
					trackingLabelKey: trackingLabelVal,
					// The I/O pod is built to satisfy the strictest Pod Security Standard.
					"pod-security.kubernetes.io/enforce": "restricted",
				},
			},
		}
		_, err = client.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
		return err
	}
	return err
}

func workerLoop(ctx context.Context, e *env, namespaces []string, workerID int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		jitter := time.Duration(r.Intn(3000)) * time.Millisecond
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		}

		targetNS := namespaces[r.Intn(len(namespaces))]
		res := runCycle(ctx, e, targetNS, r, workerID)
		if res == nil {
			// Shutdown arrived before the cycle created anything.
			return
		}
		e.metrics.recordCycle(res)
		if res.Status == statusFailure {
			fmt.Printf("[%s] FAIL %s/%s node=%s phase=%s: %s\n",
				time.Now().Format("15:04:05"), res.Namespace, res.ID, res.Node, res.FailedPhase, res.Error)
		}
		if e.results != nil {
			e.results.Write(res)
		}
	}
}

func runReporter(ctx context.Context, e *env, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.metrics.printProgress(os.Stdout)
		}
	}
}

func cleanupAllStressResources(ctx context.Context, client kubernetes.Interface, namespaces []string) {
	opts := metav1.DeleteOptions{GracePeriodSeconds: new(int64)}
	listOpts := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", trackingLabelKey, trackingLabelVal)}

	var wg sync.WaitGroup
	for _, ns := range namespaces {
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()

			// Re-check ownership right before deleting anything.
			nsObj, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					fmt.Printf("[%s] Skipping cleanup, cannot read namespace: %v\n", namespace, err)
				}
				return
			}
			if nsObj.Labels[managedByKey] != managedByVal {
				fmt.Printf("[%s] Skipping cleanup, namespace is not managed by csi-stress-test\n", namespace)
				return
			}
			fmt.Printf("[%s] Starting cleanup...\n", namespace)

			// 1. Delete all stress pods in this namespace
			_ = client.CoreV1().Pods(namespace).DeleteCollection(ctx, opts, listOpts)

			// 2. Wait until pods are gone so CSI drivers can unmount attachments
			waitForPodsDeleted(ctx, client, namespace, listOpts)

			// 3. Delete all PVCs (avoids pvc-protection deadlocks)
			_ = client.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, listOpts)

			// 4. Delete the namespace
			err = client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				fmt.Printf("[%s] Failed to delete namespace: %v\n", namespace, err)
			} else {
				fmt.Printf("[%s] Namespace deletion initiated.\n", namespace)
			}
		}(ns)
	}
	wg.Wait()
	fmt.Println("Cleanup across all test namespaces completed.")
}

func waitForPodsDeleted(ctx context.Context, client kubernetes.Interface, namespace string, listOpts metav1.ListOptions) {
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
