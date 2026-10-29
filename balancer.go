package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// nodeBalancer hands every cycle the eligible node with the fewest in-flight test pods (ties broken
// by fewest total assignments), so pods and therefore volume attachments spread evenly over all hosts.
// It also keeps in-flight test volumes plus the volumes other workloads have attached through the
// same driver (VolumeAttachments, refreshed every interval) under each node's CSINode attach limit.
type nodeBalancer struct {
	client      kubernetes.Interface
	selector    labels.Selector
	driver      string
	testNS      map[string]bool // PVs claimed from these namespaces belong to the test
	ignoreLimit bool

	mu       sync.Mutex
	nodes    map[string]*nodeSlot
	lastWarn time.Time
}

type nodeSlot struct {
	eligible bool
	reason   string // why the node is not eligible
	maxVols  int    // CSINode allocatable count; 0 = unknown/unlimited
	external int    // volumes of the driver attached to the node by other workloads
	pods     int
	vols     int // in-flight test volumes, reserved from acquire until detached
	peak     int // highest vols+external seen when a pod was assigned
	assigned uint64
}

func newNodeBalancer(ctx context.Context, client kubernetes.Interface, selector labels.Selector, driver string,
	namespaces []string, ignoreLimit bool) (*nodeBalancer, error) {
	b := &nodeBalancer{client: client, selector: selector, driver: driver, testNS: map[string]bool{},
		ignoreLimit: ignoreLimit, nodes: map[string]*nodeSlot{}}
	for _, ns := range namespaces {
		b.testNS[ns] = true
	}
	if err := b.refresh(ctx, false); err != nil {
		return nil, err
	}
	if b.eligibleCount() == 0 {
		b.printNodes()
		return nil, fmt.Errorf("no eligible nodes (selector %q)", selector.String())
	}
	return b, nil
}

func (b *nodeBalancer) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.refresh(ctx, true); err != nil && ctx.Err() == nil {
				fmt.Printf("[%s] WARN node refresh failed: %v\n", time.Now().Format("15:04:05"), err)
			}
		}
	}
}

// refresh re-reads nodes and CSINode objects, keeping in-flight counters of known nodes.
func (b *nodeBalancer) refresh(ctx context.Context, logChanges bool) error {
	nodes, err := b.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: b.selector.String()})
	if err != nil {
		return err
	}

	limits := map[string]int{}
	registered := map[string]bool{}
	csiKnown := false
	if b.driver != "" {
		if csiNodes, err := b.client.StorageV1().CSINodes().List(ctx, metav1.ListOptions{}); err == nil {
			for _, cn := range csiNodes.Items {
				for _, d := range cn.Spec.Drivers {
					if d.Name != b.driver {
						continue
					}
					registered[cn.Name] = true
					if d.Allocatable != nil && d.Allocatable.Count != nil {
						limits[cn.Name] = int(*d.Allocatable.Count)
					}
				}
			}
			// A provisioner that no CSINode lists is not a CSI driver (e.g. local-path), so
			// registration says nothing about where its volumes can be used.
			csiKnown = len(registered) > 0
		}
	}
	external, extErr := b.externalAttachments(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	if extErr != nil {
		// Not fatal (e.g. no RBAC on VolumeAttachments): keep the last known counts.
		fmt.Printf("[%s] WARN cannot count other workloads' attachments: %v\n", time.Now().Format("15:04:05"), extErr)
		external = map[string]int{}
		for name, slot := range b.nodes {
			external[name] = slot.external
		}
	}

	seen := map[string]bool{}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		seen[n.Name] = true
		reason := nodeIneligibility(n)
		if reason == "" && csiKnown && !registered[n.Name] {
			reason = "CSI driver " + b.driver + " not registered"
		}
		b.update(n.Name, reason, limits[n.Name], external[n.Name], logChanges)
	}
	for name, slot := range b.nodes {
		if !seen[name] && slot.eligible {
			b.update(name, "node removed or no longer matches selector", slot.maxVols, slot.external, logChanges)
		}
	}
	return nil
}

// externalAttachments counts, per node, the driver's VolumeAttachments that do not belong to test PVs.
func (b *nodeBalancer) externalAttachments(ctx context.Context) (map[string]int, error) {
	out := map[string]int{}
	if b.driver == "" {
		return out, nil
	}
	vas, err := b.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeAttachments: %w", err)
	}
	pvs, err := b.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	testPV := map[string]bool{}
	for i := range pvs.Items {
		if ref := pvs.Items[i].Spec.ClaimRef; ref != nil && b.testNS[ref.Namespace] {
			testPV[pvs.Items[i].Name] = true
		}
	}
	for _, va := range vas.Items {
		if va.Spec.Attacher != b.driver {
			continue
		}
		if pv := va.Spec.Source.PersistentVolumeName; pv != nil && testPV[*pv] {
			continue
		}
		out[va.Spec.NodeName]++
	}
	return out, nil
}

// update must be called with b.mu held.
func (b *nodeBalancer) update(name, reason string, maxVols, external int, logChanges bool) {
	slot, ok := b.nodes[name]
	if !ok {
		slot = &nodeSlot{}
		b.nodes[name] = slot
	}
	eligible := reason == ""
	if logChanges && (!ok || slot.eligible != eligible) {
		state := "eligible"
		if !eligible {
			state = "ineligible: " + reason
		}
		fmt.Printf("[%s] Node %s is now %s\n", time.Now().Format("15:04:05"), name, state)
	}
	slot.eligible = eligible
	slot.reason = reason
	slot.maxVols = maxVols
	slot.external = external
}

// nodeIneligibility returns why the test pod cannot run on n, or "" if it can.
func nodeIneligibility(n *corev1.Node) string {
	if n.Spec.Unschedulable {
		return "cordoned"
	}
	ready := false
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return "not Ready"
	}
	// The test pod has no tolerations.
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return fmt.Sprintf("taint %s:%s", t.Key, t.Effect)
		}
	}
	return ""
}

func (b *nodeBalancer) eligibleCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range b.nodes {
		if s.eligible {
			n++
		}
	}
	return n
}

// acquire blocks until a node can take a pod with vols volumes, then reserves it. It also returns
// the node's occupancy before the reservation: in-flight test volumes plus other workloads' ones.
func (b *nodeBalancer) acquire(ctx context.Context, vols int) (string, int, error) {
	for {
		if node, occupancy := b.tryAcquire(vols); node != "" {
			return node, occupancy, nil
		}
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (b *nodeBalancer) tryAcquire(vols int) (string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	best := ""
	var bestSlot *nodeSlot
	for name, s := range b.nodes {
		if !s.eligible || (!b.ignoreLimit && s.maxVols > 0 && s.external+s.vols+vols > s.maxVols) {
			continue
		}
		if bestSlot == nil ||
			s.pods < bestSlot.pods ||
			(s.pods == bestSlot.pods && s.assigned < bestSlot.assigned) ||
			(s.pods == bestSlot.pods && s.assigned == bestSlot.assigned && name < best) {
			best, bestSlot = name, s
		}
	}
	if bestSlot == nil {
		if time.Since(b.lastWarn) > time.Minute {
			b.lastWarn = time.Now()
			fmt.Printf("[%s] WARN no node has capacity for %d more volumes; workers are waiting\n",
				time.Now().Format("15:04:05"), vols)
		}
		return "", 0
	}
	occupancy := bestSlot.vols + bestSlot.external
	bestSlot.pods++
	bestSlot.vols += vols
	bestSlot.assigned++
	bestSlot.peak = max(bestSlot.peak, occupancy+vols)
	return best, occupancy
}

func (b *nodeBalancer) release(node string, vols int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.nodes[node]; ok {
		s.pods--
		s.vols -= vols
	}
}

func (b *nodeBalancer) printNodes() {
	b.mu.Lock()
	defer b.mu.Unlock()

	names := make([]string, 0, len(b.nodes))
	for n := range b.nodes {
		names = append(names, n)
	}
	sort.Strings(names)

	fmt.Println("Node balancer:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tSTATUS\tATTACH LIMIT\tOTHER VOLS\tASSIGNED\tIN-FLIGHT PODS\tIN-FLIGHT VOLS\tPEAK VOLS")
	for _, n := range names {
		s := b.nodes[n]
		status := "eligible"
		if !s.eligible {
			status = "skipped (" + strings.TrimSpace(s.reason) + ")"
		}
		limit := "-"
		if s.maxVols > 0 {
			limit = fmt.Sprint(s.maxVols)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\n", n, status, limit, s.external, s.assigned, s.pods, s.vols, s.peak)
	}
	tw.Flush()
}
