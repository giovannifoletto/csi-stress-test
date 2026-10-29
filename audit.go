package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// auditCounts are gauges that should stay flat during a healthy run; one that keeps growing is a
// leak in the driver or its sidecars (attacher, provisioner) that per-cycle checks cannot see.
type auditCounts struct {
	orphanVA        int // VolumeAttachments of the driver whose PV or node no longer exists
	vaErrors        int // VolumeAttachments of the driver reporting an attach or detach error
	pvReleased      int // PVs of the StorageClass in phase Released (not reclaimed)
	pvFailed        int // PVs of the StorageClass in phase Failed (e.g. DeleteVolume failed)
	pvStuckDeleting int // PVs of the StorageClass deleted more than --teardown-timeout ago
	testPVs         int // PVs bound to the test namespaces
}

type auditField struct {
	name string
	val  int
}

func (c auditCounts) fields() []auditField {
	return []auditField{
		{"orphan_va", c.orphanVA},
		{"va_errors", c.vaErrors},
		{"pv_released", c.pvReleased},
		{"pv_failed", c.pvFailed},
		{"pv_stuck_deleting", c.pvStuckDeleting},
	}
}

type auditor struct {
	client       kubernetes.Interface
	driver       string // "" skips the VolumeAttachment checks
	storageClass string // "" restricts the PV checks to test PVs
	testNS       map[string]bool
	stuckAfter   time.Duration
	baseline     auditCounts
}

func newAuditor(client kubernetes.Interface, driver, storageClass string, namespaces []string, stuckAfter time.Duration) *auditor {
	a := &auditor{client: client, driver: driver, storageClass: storageClass, testNS: map[string]bool{}, stuckAfter: stuckAfter}
	for _, ns := range namespaces {
		a.testNS[ns] = true
	}
	return a
}

func (a *auditor) collect(ctx context.Context) (auditCounts, error) {
	var c auditCounts
	pvs, err := a.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return c, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	pvExists := make(map[string]bool, len(pvs.Items))
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		pvExists[pv.Name] = true
		isTest := pv.Spec.ClaimRef != nil && a.testNS[pv.Spec.ClaimRef.Namespace]
		if isTest {
			c.testPVs++
		}
		if !isTest && (a.storageClass == "" || pv.Spec.StorageClassName != a.storageClass) {
			continue
		}
		switch pv.Status.Phase {
		case corev1.VolumeReleased:
			c.pvReleased++
		case corev1.VolumeFailed:
			c.pvFailed++
		}
		if dt := pv.DeletionTimestamp; dt != nil && time.Since(dt.Time) > a.stuckAfter {
			c.pvStuckDeleting++
		}
	}

	if a.driver == "" {
		return c, nil
	}
	nodes, err := a.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return c, fmt.Errorf("list Nodes: %w", err)
	}
	nodeExists := make(map[string]bool, len(nodes.Items))
	for _, n := range nodes.Items {
		nodeExists[n.Name] = true
	}
	vas, err := a.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return c, fmt.Errorf("list VolumeAttachments: %w", err)
	}
	for _, va := range vas.Items {
		if va.Spec.Attacher != a.driver {
			continue
		}
		pv := va.Spec.Source.PersistentVolumeName
		if !nodeExists[va.Spec.NodeName] || (pv != nil && !pvExists[*pv]) {
			c.orphanVA++
		}
		if va.Status.AttachError != nil || va.Status.DetachError != nil {
			c.vaErrors++
		}
	}
	return c, nil
}

// format renders the counts, marking the ones above the baseline taken at start.
func (a *auditor) format(c auditCounts) (string, bool) {
	var parts []string
	grew := false
	base := a.baseline.fields()
	for i, f := range c.fields() {
		s := fmt.Sprintf("%s=%d", f.name, f.val)
		if d := f.val - base[i].val; d > 0 {
			s += fmt.Sprintf("(+%d)", d)
			grew = true
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " "), grew
}

// start records the baseline, so leftovers that predate the run are not reported as growth.
func (a *auditor) start(ctx context.Context) {
	c, err := a.collect(ctx)
	if err != nil {
		fmt.Printf("WARN leak audit disabled: %v\n", err)
		return
	}
	a.baseline = c
	line, _ := a.format(c)
	fmt.Printf("[%s] AUDIT baseline %s test_pvs=%d\n", time.Now().Format("15:04:05"), line, c.testPVs)
}

func (a *auditor) report(ctx context.Context) {
	c, err := a.collect(ctx)
	if err != nil {
		if ctx.Err() == nil {
			fmt.Printf("[%s] WARN leak audit failed: %v\n", time.Now().Format("15:04:05"), err)
		}
		return
	}
	line, grew := a.format(c)
	suffix := ""
	if grew {
		suffix = " WARN above baseline"
	}
	fmt.Printf("[%s] AUDIT %s test_pvs=%d%s\n", time.Now().Format("15:04:05"), line, c.testPVs, suffix)
}

func (a *auditor) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.report(ctx)
		}
	}
}

// final waits up to wait for the test PVs to disappear (namespace deletion releases them
// asynchronously), then prints the last audit with the test PVs left behind.
func (a *auditor) final(ctx context.Context, wait time.Duration) {
	deadline := time.Now().Add(wait)
	for {
		c, err := a.collect(ctx)
		if err != nil {
			fmt.Printf("WARN final leak audit failed: %v\n", err)
			return
		}
		if c.testPVs == 0 || time.Now().After(deadline) {
			line, grew := a.format(c)
			fmt.Printf("Final leak audit: %s\n", line)
			if c.testPVs > 0 {
				fmt.Printf("WARNING: %d PVs of the test namespaces still exist (detach or DeleteVolume stuck?)\n", c.testPVs)
			}
			if grew {
				fmt.Println("WARNING: leak counters are above the baseline taken at start")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
