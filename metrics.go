package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

type metricDef struct {
	name string
	unit string // "s" for latencies, "MiB/s" for throughput
	help string
}

var metricDefs = []metricDef{
	{"pvc_bind", "s", "PVC create -> Bound (includes scheduling wait with WaitForFirstConsumer)"},
	{"pod_schedule", "s", "pod create -> bound to a node (includes CreateVolume with WaitForFirstConsumer)"},
	{"attach_mount", "s", "pod scheduled -> Running (ControllerPublish + NodeStage/Publish + container start)"},
	{"startup_total", "s", "first PVC create -> pod Running"},
	{"pod_delete", "s", "pod delete -> gone (NodeUnpublish/Unstage)"},
	{"pvc_delete", "s", "PVC delete -> gone"},
	{"pv_release", "s", "PVC delete -> PV gone (ControllerUnpublish + DeleteVolume)"},
	{"io_write", "MiB/s", "fsync'd sequential write throughput per volume"},
	{"io_read", "MiB/s", "O_DIRECT sequential read throughput per volume"},
}

type nodeStats struct {
	success, failure uint64
	attach           []float64
}

// occSample pairs a cycle's attach_mount latency with the node's volume count when it was assigned.
type occSample struct {
	occupancy int
	attach    float64
}

type metrics struct {
	start        time.Time
	inFlight     atomic.Int64
	inFlightVols atomic.Int64

	mu          sync.Mutex
	samples     map[string][]float64 // seconds for latencies, MiB/s for throughput
	success     uint64
	failure     uint64
	aborted     uint64
	noIO        uint64
	failByPhase map[string]uint64
	nodes       map[string]*nodeStats
	occupancy   []occSample
	peakNodeVol int // highest NodeVols+PVCs of any cycle

	// Samples since the last progress line, so the progress shows the current behaviour instead
	// of an average since the start that hides slow degradation. Only kept when windowed is set.
	windowed   bool
	window     map[string][]float64
	winSuccess uint64
	winFailure uint64
	winStart   time.Time
}

func newMetrics() *metrics {
	return &metrics{
		start:       time.Now(),
		samples:     map[string][]float64{},
		failByPhase: map[string]uint64{},
		nodes:       map[string]*nodeStats{},
		window:      map[string][]float64{},
		winStart:    time.Now(),
	}
}

func (m *metrics) recordCycle(res *cycleResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch res.Status {
	case statusSuccess:
		m.success++
		m.winSuccess++
		if len(res.IO) == 0 {
			m.noIO++
		}
	case statusFailure:
		m.failure++
		m.winFailure++
		m.failByPhase[res.FailedPhase]++
	case statusAborted:
		m.aborted++
	}

	add := func(name string, v float64) {
		m.samples[name] = append(m.samples[name], v)
		if m.windowed {
			m.window[name] = append(m.window[name], v)
		}
	}
	for name, vals := range res.LatenciesMs {
		for _, v := range vals {
			add(name, v/1000)
		}
	}
	for _, s := range res.IO {
		if s.WriteMiBps > 0 {
			add("io_write", s.WriteMiBps)
		}
		// A buffered read only measures the page cache, so it is not reported.
		if s.DirectRead && s.ReadMiBps > 0 {
			add("io_read", s.ReadMiBps)
		}
	}

	if res.NodeVols != nil && res.Status != statusAborted {
		m.peakNodeVol = max(m.peakNodeVol, *res.NodeVols+res.PVCs)
		for _, v := range res.LatenciesMs["attach_mount"] {
			m.occupancy = append(m.occupancy, occSample{occupancy: *res.NodeVols, attach: v / 1000})
		}
	}

	if res.Node != "" && res.Status != statusAborted {
		ns, ok := m.nodes[res.Node]
		if !ok {
			ns = &nodeStats{}
			m.nodes[res.Node] = ns
		}
		if res.Status == statusSuccess {
			ns.success++
		} else {
			ns.failure++
		}
		for _, v := range res.LatenciesMs["attach_mount"] {
			ns.attach = append(ns.attach, v/1000)
		}
	}
}

// percentiles returns nearest-rank percentiles of vals (which it sorts in place).
func percentiles(vals []float64, ps ...float64) []float64 {
	sort.Float64s(vals)
	out := make([]float64, len(ps))
	if len(vals) == 0 {
		return out
	}
	for i, p := range ps {
		idx := int(math.Ceil(p*float64(len(vals)))) - 1
		if idx < 0 {
			idx = 0
		}
		out[i] = vals[idx]
	}
	return out
}

func formatValue(v float64, unit string) string {
	if unit == "s" {
		if v < 1 {
			return fmt.Sprintf("%.0fms", v*1000)
		}
		return fmt.Sprintf("%.2fs", v)
	}
	return fmt.Sprintf("%.1f", v)
}

var progressMetrics = []string{"pvc_bind", "attach_mount", "pod_delete", "pv_release", "io_write"}

func metricUnit(name string) string {
	if strings.HasPrefix(name, "io_") {
		return "MiB/s"
	}
	return "s"
}

// printProgress prints the totals so far and the percentiles of the cycles completed since the
// previous call, then starts a new window.
func (m *metrics) printProgress(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] ok=%d failed=%d aborted=%d in-flight=%d pods/%d vols | last %v: ok=%d failed=%d",
		now.Format("15:04:05"), m.success, m.failure, m.aborted, m.inFlight.Load(), m.inFlightVols.Load(),
		now.Sub(m.winStart).Round(time.Second), m.winSuccess, m.winFailure)
	for _, name := range progressMetrics {
		vals := m.window[name]
		if len(vals) == 0 {
			continue
		}
		p := percentiles(vals, 0.5, 0.95)
		fmt.Fprintf(&b, " | %s p50=%s p95=%s", name, formatValue(p[0], metricUnit(name)), formatValue(p[1], metricUnit(name)))
	}
	fmt.Fprintln(w, b.String())

	m.window = map[string][]float64{}
	m.winSuccess, m.winFailure = 0, 0
	m.winStart = now
}

// printSummary prints the final report; elapsedLabel names what elapsed measures.
func (m *metrics) printSummary(w io.Writer, elapsedLabel string, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Fprintf(w, "%s: %v\nSuccessful Lifecycles: %d\nFailed Lifecycles: %d\nAborted at shutdown: %d\n",
		elapsedLabel, elapsed.Round(time.Second), m.success, m.failure, m.aborted)
	if m.noIO > 0 {
		fmt.Fprintf(w, "Successful cycles without a completed I/O iteration (hold too short or I/O stalled): %d\n", m.noIO)
	}
	if len(m.failByPhase) > 0 {
		phases := make([]string, 0, len(m.failByPhase))
		for p := range m.failByPhase {
			phases = append(phases, p)
		}
		sort.Strings(phases)
		fmt.Fprint(w, "Failures by phase:")
		for _, p := range phases {
			fmt.Fprintf(w, " %s=%d", p, m.failByPhase[p])
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tUNIT\tCOUNT\tMIN\tP50\tP95\tP99\tMAX\tDESCRIPTION")
	for _, d := range metricDefs {
		vals := append([]float64(nil), m.samples[d.name]...)
		if len(vals) == 0 {
			fmt.Fprintf(tw, "%s\t%s\t0\t-\t-\t-\t-\t-\t%s\n", d.name, d.unit, d.help)
			continue
		}
		p := percentiles(vals, 0, 0.5, 0.95, 0.99, 1)
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", d.name, d.unit, len(vals),
			formatValue(p[0], d.unit), formatValue(p[1], d.unit), formatValue(p[2], d.unit),
			formatValue(p[3], d.unit), formatValue(p[4], d.unit), d.help)
	}
	tw.Flush()

	m.printOccupancy(w)

	if len(m.nodes) == 0 {
		return
	}
	names := make([]string, 0, len(m.nodes))
	for n := range m.nodes {
		names = append(names, n)
	}
	sort.Strings(names)

	fmt.Fprintln(w, "\nPer-node results:")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tOK\tFAILED\tATTACH_MOUNT P50\tP95\tMAX")
	for _, n := range names {
		ns := m.nodes[n]
		p := percentiles(append([]float64(nil), ns.attach...), 0.5, 0.95, 1)
		if len(ns.attach) == 0 {
			fmt.Fprintf(tw, "%s\t%d\t%d\t-\t-\t-\n", n, ns.success, ns.failure)
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\n", n, ns.success, ns.failure,
			formatValue(p[0], "s"), formatValue(p[1], "s"), formatValue(p[2], "s"))
	}
	tw.Flush()
}

// printOccupancy shows how attach_mount evolves as nodes fill up, in about ten occupancy ranges.
// Must be called with m.mu held.
func (m *metrics) printOccupancy(w io.Writer) {
	if len(m.occupancy) == 0 {
		return
	}
	maxOcc := 0
	for _, s := range m.occupancy {
		maxOcc = max(maxOcc, s.occupancy)
	}
	width := (maxOcc + 10) / 10 // ceil((maxOcc+1)/10)
	buckets := make([][]float64, maxOcc/width+1)
	for _, s := range m.occupancy {
		buckets[s.occupancy/width] = append(buckets[s.occupancy/width], s.attach)
	}

	fmt.Fprintf(w, "\nPeak volumes on a single node (in-flight test volumes + other workloads'): %d\n", m.peakNodeVol)
	fmt.Fprintln(w, "attach_mount by volumes already on the node:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE VOLS\tCOUNT\tP50\tP95\tMAX")
	for i, vals := range buckets {
		label := fmt.Sprint(i * width)
		if width > 1 {
			label = fmt.Sprintf("%d-%d", i*width, (i+1)*width-1)
		}
		if len(vals) == 0 {
			fmt.Fprintf(tw, "%s\t0\t-\t-\t-\n", label)
			continue
		}
		p := percentiles(vals, 0.5, 0.95, 1)
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n", label, len(vals),
			formatValue(p[0], "s"), formatValue(p[1], "s"), formatValue(p[2], "s"))
	}
	tw.Flush()
}

// printBuckets prints one row per time window (including empty ones, so gaps stand out), to spot
// latency drift, throughput decay or failure bursts over a long run.
func printBuckets(w io.Writer, buckets map[time.Time]*metrics, size time.Duration) {
	if len(buckets) == 0 {
		return
	}
	var first, last time.Time
	for t := range buckets {
		if first.IsZero() || t.Before(first) {
			first = t
		}
		if t.After(last) {
			last = t
		}
	}

	label := size.String()
	if strings.HasSuffix(label, "m0s") {
		label = strings.TrimSuffix(label, "0s")
	}
	if strings.HasSuffix(label, "h0m") {
		label = strings.TrimSuffix(label, "0m")
	}
	fmt.Fprintf(w, "\nPer-%s windows (p50/p95):\n", label)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "WINDOW START\tOK\tFAILED"
	for _, name := range progressMetrics {
		header += "\t" + strings.ToUpper(name)
	}
	fmt.Fprintln(tw, header)
	for t := first; !t.After(last); t = t.Add(size) {
		row := t.Format(time.RFC3339)
		m := buckets[t]
		if m == nil {
			fmt.Fprintf(tw, "%s\t0\t0%s\n", row, strings.Repeat("\t-", len(progressMetrics)))
			continue
		}
		row += fmt.Sprintf("\t%d\t%d", m.success, m.failure)
		for _, name := range progressMetrics {
			vals := m.samples[name]
			if len(vals) == 0 {
				row += "\t-"
				continue
			}
			p := percentiles(vals, 0.5, 0.95)
			row += "\t" + formatValue(p[0], metricUnit(name)) + "/" + formatValue(p[1], metricUnit(name))
		}
		fmt.Fprintln(tw, row)
	}
	tw.Flush()
}

// resultsWriter appends one JSON object per cycle.
type resultsWriter struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

func newResultsWriter(path string) (*resultsWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &resultsWriter{f: f, enc: json.NewEncoder(f)}, nil
}

func (r *resultsWriter) Write(res *cycleResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Encode(res); err != nil {
		fmt.Printf("WARN: writing results file: %v\n", err)
	}
}

func (r *resultsWriter) Close() error {
	return r.f.Close()
}

// loadResults passes every record of the given JSONL results files to visit.
// Malformed lines (e.g. a truncated last line after a hard kill) are skipped with a warning.
// It returns the earliest and latest cycle start and the number of records loaded.
func loadResults(paths []string, visit func(*cycleResult)) (first, last time.Time, n int, err error) {
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return first, last, n, err
		}
		sc := bufio.NewScanner(f)
		// Records carry per-volume I/O samples and can exceed the 64KiB default.
		sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
		for line := 1; sc.Scan(); line++ {
			raw := bytes.TrimSpace(sc.Bytes())
			if len(raw) == 0 {
				continue
			}
			var res cycleResult
			if err := json.Unmarshal(raw, &res); err != nil {
				fmt.Fprintf(os.Stderr, "WARN: %s:%d: %v\n", path, line, err)
				continue
			}
			visit(&res)
			n++
			if first.IsZero() || res.Start.Before(first) {
				first = res.Start
			}
			if res.Start.After(last) {
				last = res.Start
			}
		}
		err = sc.Err()
		f.Close()
		if err != nil {
			return first, last, n, fmt.Errorf("%s: %w", path, err)
		}
	}
	return first, last, n, nil
}
