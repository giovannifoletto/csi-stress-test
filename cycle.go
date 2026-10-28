package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	statusSuccess = "success"
	statusFailure = "failure"
	statusAborted = "aborted"
)

// ioSample aggregates the I/O iterations a pod performed on one volume.
type ioSample struct {
	Volume     string  `json:"volume"`
	Iterations int     `json:"iterations"`
	WriteMiBps float64 `json:"write_mibps"`
	ReadMiBps  float64 `json:"read_mibps"`
	DirectRead bool    `json:"direct_read"`
}

// cycleResult is the outcome of one PVC+pod lifecycle; it is also the JSONL record.
type cycleResult struct {
	ID          string               `json:"id"`
	Namespace   string               `json:"namespace"`
	Node        string               `json:"node,omitempty"`
	PVCs        int                  `json:"pvcs"`
	Start       time.Time            `json:"start"`
	Status      string               `json:"status"`
	FailedPhase string               `json:"failed_phase,omitempty"`
	Error       string               `json:"error,omitempty"`
	LatenciesMs map[string][]float64 `json:"latencies_ms"`
	IO          []ioSample           `json:"io,omitempty"`
}

type phaseError struct {
	phase string
	err   error
}

func (p *phaseError) Error() string { return p.err.Error() }
func (p *phaseError) Unwrap() error { return p.err }

func phaseErr(phase string, err error) error { return &phaseError{phase: phase, err: err} }

type cycle struct {
	e              *env
	ns             string
	id             string
	podName        string
	pvcNames       []string
	createdPVCs    []string
	podCreated     bool
	reachedRunning bool

	mu  sync.Mutex // guards res.LatenciesMs during parallel teardown
	res *cycleResult
}

// runCycle executes one full lifecycle. It returns nil when shutdown happened before anything was created.
func runCycle(ctx context.Context, e *env, namespace string, r *rand.Rand, workerID int) *cycleResult {
	cfg := e.cfg
	pvcCount := cfg.MinPvcCount + r.Intn(cfg.MaxPvcCount-cfg.MinPvcCount+1)
	hold := cfg.MinHoldDuration
	if cfg.MaxHoldDuration > cfg.MinHoldDuration {
		hold += time.Duration(r.Int63n(int64(cfg.MaxHoldDuration - cfg.MinHoldDuration)))
	}

	c := &cycle{
		e:       e,
		ns:      namespace,
		id:      fmt.Sprintf("w%d-%08x", workerID, r.Uint32()),
		res:     &cycleResult{Namespace: namespace, PVCs: pvcCount, LatenciesMs: map[string][]float64{}},
		podName: "",
	}
	c.res.ID = c.id
	c.podName = "pod-" + c.id
	for i := 0; i < pvcCount; i++ {
		c.pvcNames = append(c.pvcNames, fmt.Sprintf("pvc-%s-%d", c.id, i))
	}

	if e.balancer != nil {
		node, err := e.balancer.acquire(ctx, pvcCount)
		if err != nil {
			return nil
		}
		c.res.Node = node
		// Released only after teardown, i.e. once the volumes are detached from the node.
		defer e.balancer.release(node, pvcCount)
	}

	e.metrics.inFlight.Add(1)
	defer e.metrics.inFlight.Add(-1)

	c.res.Start = time.Now()
	runErr := c.run(ctx, hold)
	aborted := runErr != nil && ctx.Err() != nil
	tdErr := c.teardown()

	switch {
	case runErr != nil && !aborted:
		c.res.Status = statusFailure
		c.res.FailedPhase = "unknown"
		var pe *phaseError
		if errors.As(runErr, &pe) {
			c.res.FailedPhase = pe.phase
		}
		c.res.Error = runErr.Error()
		if tdErr != nil {
			c.res.Error += "; teardown: " + tdErr.Error()
		}
	case tdErr != nil:
		c.res.Status = statusFailure
		c.res.FailedPhase = "teardown"
		c.res.Error = tdErr.Error()
	case aborted:
		c.res.Status = statusAborted
	default:
		c.res.Status = statusSuccess
	}
	return c.res
}

func (c *cycle) record(metric string, d time.Duration) {
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	c.res.LatenciesMs[metric] = append(c.res.LatenciesMs[metric], float64(d.Microseconds())/1000)
	c.mu.Unlock()
}

func (c *cycle) run(ctx context.Context, hold time.Duration) error {
	cfg := c.e.cfg
	client := c.e.client

	startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancelStartup()

	// Track binding in the background: with WaitForFirstConsumer PVCs bind only after the pod is scheduled.
	bindDone := make(chan map[string]time.Time, 1)
	go func() { bindDone <- watchPVCBinding(startupCtx, client, c.ns, c.id, len(c.pvcNames)) }()

	// 1. Create PersistentVolumeClaims
	createdAt := make(map[string]time.Time, len(c.pvcNames))
	for _, name := range c.pvcNames {
		createdAt[name] = time.Now()
		_, err := client.CoreV1().PersistentVolumeClaims(c.ns).Create(ctx, buildPVCSpec(cfg, name, c.id), metav1.CreateOptions{})
		if err != nil {
			return phaseErr("pvc_create", fmt.Errorf("create PVC %s: %w", name, err))
		}
		c.createdPVCs = append(c.createdPVCs, name)
	}

	// 2. Create the pod (pinned to the balancer's node when --spread=balanced)
	podCreatedAt := time.Now()
	_, err := client.CoreV1().Pods(c.ns).Create(ctx, buildPodSpec(cfg, c.ns, c.podName, c.id, c.pvcNames, c.res.Node), metav1.CreateOptions{})
	if err != nil {
		return phaseErr("pod_create", fmt.Errorf("create pod %s: %w", c.podName, err))
	}
	c.podCreated = true

	// 3. Wait for Running: scheduling, attach and mount all have to succeed
	st, err := waitPodRunning(startupCtx, client, c.ns, c.podName)
	if st.node != "" {
		c.res.Node = st.node
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return phaseErr("startup", fmt.Errorf("pod %s did not reach Running: %w%s", c.podName, err, c.diagnose()))
	}
	c.reachedRunning = true
	c.record("pod_schedule", st.scheduledAt.Sub(podCreatedAt))
	c.record("attach_mount", st.runningAt.Sub(st.scheduledAt))
	c.record("startup_total", st.runningAt.Sub(createdAt[c.pvcNames[0]]))

	select {
	case bound := <-bindDone:
		for name, t := range bound {
			c.record("pvc_bind", t.Sub(createdAt[name]))
		}
	case <-startupCtx.Done():
	}

	// 4. Hold while the pod performs I/O, failing fast if it stops running (I/O or checksum error)
	holdCtx, cancelHold := context.WithTimeout(ctx, hold)
	defer cancelHold()
	pod, err := waitPodLeavesRunning(holdCtx, client, c.ns, c.podName)
	switch {
	case err == nil:
		phase := corev1.PodPhase("Deleted")
		if pod != nil {
			phase = pod.Status.Phase
		}
		return phaseErr("io", fmt.Errorf("pod left Running during hold (phase %s): %s", phase, c.logTail()))
	case ctx.Err() != nil:
		return ctx.Err()
	case holdCtx.Err() == nil:
		return phaseErr("io", fmt.Errorf("watching pod during hold: %w", err))
	}

	c.collectIO()
	return nil
}

// collectIO parses the per-iteration lines the pod printed. Missing samples are not a failure:
// the hold may be shorter than one iteration on slow storage.
func (c *cycle) collectIO() {
	logs, err := c.podLogs(0)
	if err != nil {
		fmt.Printf("[%s] WARN %s/%s: cannot read pod logs: %v\n", time.Now().Format("15:04:05"), c.ns, c.id, err)
		return
	}
	c.res.IO = parseIOSamples(logs)
}

func (c *cycle) teardown() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.e.cfg.TeardownTimeout)
	defer cancel()
	client := c.e.client
	var errs []error

	// Pod first: PVCs stay protected while a pod uses them, and pod removal waits for unmount.
	if c.podCreated {
		start := time.Now()
		err := client.CoreV1().Pods(c.ns).Delete(ctx, c.podName, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
		switch {
		case err != nil && !apierrors.IsNotFound(err):
			errs = append(errs, fmt.Errorf("delete pod: %w", err))
		default:
			if err := waitGone(ctx, podLW(client, c.ns, c.podName), &corev1.Pod{}, c.ns+"/"+c.podName); err != nil {
				errs = append(errs, fmt.Errorf("pod %s not removed (unmount stuck?): %w", c.podName, err))
			} else if c.reachedRunning {
				c.record("pod_delete", time.Since(start))
			}
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, name := range c.createdPVCs {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			if err := c.deletePVC(ctx, name); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(name)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// deletePVC deletes a claim and waits for it, and for its PV when reclaimPolicy is Delete, to disappear.
// The PV going away means the volume was detached and DeleteVolume succeeded.
func (c *cycle) deletePVC(ctx context.Context, name string) error {
	client := c.e.client
	pvc, err := client.CoreV1().PersistentVolumeClaims(c.ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get PVC %s: %w", name, err)
	}
	pvName := pvc.Spec.VolumeName

	start := time.Now()
	err = client.CoreV1().PersistentVolumeClaims(c.ns).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete PVC %s: %w", name, err)
	}
	if err := waitGone(ctx, pvcLW(client, c.ns, name), &corev1.PersistentVolumeClaim{}, c.ns+"/"+name); err != nil {
		return fmt.Errorf("PVC %s not removed: %w", name, err)
	}
	if pvName == "" {
		return nil
	}
	c.record("pvc_delete", time.Since(start))

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		c.record("pv_release", time.Since(start))
		return nil
	}
	if err != nil {
		return fmt.Errorf("get PV %s: %w", pvName, err)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return nil
	}
	if err := waitGone(ctx, pvLW(client, pvName), &corev1.PersistentVolume{}, pvName); err != nil {
		return fmt.Errorf("PV %s not released (detach/DeleteVolume stuck?): %w", pvName, err)
	}
	c.record("pv_release", time.Since(start))
	return nil
}

func (c *cycle) podLogs(tail int64) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := &corev1.PodLogOptions{}
	if tail > 0 {
		opts.TailLines = &tail
	}
	b, err := c.e.client.CoreV1().Pods(c.ns).GetLogs(c.podName, opts).DoRaw(ctx)
	return string(b), err
}

func (c *cycle) logTail() string {
	logs, err := c.podLogs(10)
	if err != nil {
		return fmt.Sprintf("<logs unavailable: %v>", err)
	}
	return strings.Join(strings.Fields(strings.ReplaceAll(strings.TrimSpace(logs), "\n", " | ")), " ")
}

// diagnose explains why a pod is stuck: unmet conditions, waiting containers, pending PVCs and
// the latest warning events (FailedAttachVolume, FailedMount, ProvisioningFailed, ...).
func (c *cycle) diagnose() string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := c.e.client
	var parts []string

	if pod, err := client.CoreV1().Pods(c.ns).Get(ctx, c.podName, metav1.GetOptions{}); err == nil {
		parts = append(parts, "phase="+string(pod.Status.Phase))
		for _, cond := range pod.Status.Conditions {
			if cond.Status != corev1.ConditionTrue && cond.Message != "" {
				parts = append(parts, fmt.Sprintf("%s: %s", cond.Type, cond.Message))
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if w := cs.State.Waiting; w != nil {
				parts = append(parts, fmt.Sprintf("container %s waiting: %s %s", cs.Name, w.Reason, w.Message))
			}
		}
	}
	parts = append(parts, lastWarnings(ctx, c, c.podName)...)

	for _, name := range c.createdPVCs {
		pvc, err := client.CoreV1().PersistentVolumeClaims(c.ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil && pvc.Status.Phase != corev1.ClaimBound {
			parts = append(parts, fmt.Sprintf("PVC %s %s", name, pvc.Status.Phase))
			parts = append(parts, lastWarnings(ctx, c, name)...)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " [" + strings.Join(parts, "; ") + "]"
}

func lastWarnings(ctx context.Context, c *cycle, object string) []string {
	events, err := c.e.client.CoreV1().Events(c.ns).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + object,
	})
	if err != nil {
		return nil
	}
	var out []string
	for _, ev := range events.Items {
		if ev.Type == corev1.EventTypeWarning {
			out = append(out, fmt.Sprintf("event %s: %s", ev.Reason, strings.TrimSpace(ev.Message)))
		}
	}
	if len(out) > 3 {
		out = out[len(out)-3:]
	}
	return out
}
