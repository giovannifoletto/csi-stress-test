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

type metrics struct {
	start    time.Time
	inFlight atomic.Int64

	mu          sync.Mutex
	samples     map[string][]float64 // seconds for latencies, MiB/s for throughput
	success     uint64
	failure     uint64
	aborted     uint64
	noIO        uint64
	failByPhase map[string]uint64
	nodes       map[string]*nodeStats
}

func newMetrics() *metrics {
	return &metrics{
		start:       time.Now(),
		samples:     map[string][]float64{},
		failByPhase: map[string]uint64{},
		nodes:       map[string]*nodeStats{},
	}
}

func (m *metrics) recordCycle(res *cycleResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch res.Status {
	case statusSuccess:
		m.success++
		if len(res.IO) == 0 {
			m.noIO++
		}
	case statusFailure:
		m.failure++
		m.failByPhase[res.FailedPhase]++
	case statusAborted:
		m.aborted++
	}

	for name, vals := range res.LatenciesMs {
		for _, v := range vals {
			m.samples[name] = append(m.samples[name], v/1000)
		}
	}
	for _, s := range res.IO {
		if s.WriteMiBps > 0 {
			m.samples["io_write"] = append(m.samples["io_write"], s.WriteMiBps)
		}
		// A buffered read only measures the page cache, so it is not reported.
		if s.DirectRead && s.ReadMiBps > 0 {
			m.samples["io_read"] = append(m.samples["io_read"], s.ReadMiBps)
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

func (m *metrics) printProgress(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s] ok=%d failed=%d aborted=%d in-flight=%d",
		time.Now().Format("15:04:05"), m.success, m.failure, m.aborted, m.inFlight.Load())
	for _, name := range []string{"pvc_bind", "attach_mount", "pod_delete", "pv_release", "io_write"} {
		vals := m.samples[name]
		if len(vals) == 0 {
			continue
		}
		unit := "s"
		if strings.HasPrefix(name, "io_") {
			unit = "MiB/s"
		}
		p := percentiles(append([]float64(nil), vals...), 0.5, 0.95)
		fmt.Fprintf(&b, " | %s p50=%s p95=%s", name, formatValue(p[0], unit), formatValue(p[1], unit))
	}
	fmt.Fprintln(w, b.String())
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

// loadResults feeds every record of the given JSONL results files into m.
// Malformed lines (e.g. a truncated last line after a hard kill) are skipped with a warning.
// It returns the earliest and latest cycle start and the number of records loaded.
func loadResults(m *metrics, paths []string) (first, last time.Time, n int, err error) {
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
			m.recordCycle(&res)
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
