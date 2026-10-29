package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPodScript(t *testing.T) {
	cfg := Config{Workload: workloadIO, IOSizeMiB: 7}
	if s := podScript(cfg); !strings.Contains(s, "MIB=7") || strings.Contains(s, "__IO_MIB__") {
		t.Errorf("io script not templated: %q", s)
	}
	cfg.Workload = workloadSleep
	if s := podScript(cfg); s != "exec sleep infinity\n" {
		t.Errorf("unexpected sleep script %q", s)
	}
	cfg.Workload, cfg.Command = workloadCommand, "touch /data/vol0/x"
	if s := podScript(cfg); s != "set -e\ntouch /data/vol0/x\nexec sleep infinity\n" {
		t.Errorf("unexpected command script %q", s)
	}
}

func TestLoadResults(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.jsonl")
	b := filepath.Join(dir, "b.jsonl")
	writeFile := func(path, content string) {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(a, `{"id":"c1","namespace":"ns-1","node":"node-a","pvcs":2,"start":"2026-10-29T10:00:00Z","status":"success",`+
		`"latencies_ms":{"pvc_bind":[1000,2000],"attach_mount":[3000]},`+
		`"io":[{"volume":"vol0","iterations":1,"write_mibps":50,"read_mibps":80,"direct_read":true},`+
		`{"volume":"vol1","iterations":1,"write_mibps":40,"read_mibps":900,"direct_read":false}]}`+"\n\n")
	writeFile(b, `{"id":"c2","namespace":"ns-2","node":"node-b","pvcs":1,"start":"2026-10-29T10:05:00Z","status":"failure",`+
		`"failed_phase":"attach_mount","error":"timeout","latencies_ms":{"pvc_bind":[500]}}`+"\n"+
		`{"id":"truncated","namespace":`)

	m := newMetrics()
	first, last, n, err := loadResults([]string{a, b}, m.recordCycle)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || m.success != 1 || m.failure != 1 || m.failByPhase["attach_mount"] != 1 {
		t.Errorf("unexpected counters n=%d success=%d failure=%d byPhase=%v", n, m.success, m.failure, m.failByPhase)
	}
	if got := len(m.samples["pvc_bind"]); got != 3 {
		t.Errorf("pvc_bind samples: got %d want 3", got)
	}
	if len(m.samples["io_write"]) != 2 || len(m.samples["io_read"]) != 1 {
		t.Errorf("io samples: write=%v read=%v", m.samples["io_write"], m.samples["io_read"])
	}
	if len(m.nodes) != 2 || m.nodes["node-a"].success != 1 || m.nodes["node-b"].failure != 1 {
		t.Errorf("unexpected node stats %+v", m.nodes)
	}
	if got := last.Sub(first); got != 5*time.Minute {
		t.Errorf("span: got %v want 5m", got)
	}

	if _, _, _, err := loadResults([]string{filepath.Join(dir, "missing.jsonl")}, newMetrics().recordCycle); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseIOSamples(t *testing.T) {
	logs := "noise\n" +
		"IO vol=vol0 bytes=10485760 write_ns=1000000000 read_ns=500000000 direct=1\n" +
		"IO vol=vol1 bytes=10485760 write_ns=2000000000 read_ns=1000000000 direct=0\n" +
		"IO vol=vol0 bytes=10485760 write_ns=1000000000 read_ns=500000000 direct=1\n" +
		"IO vol=broken bytes=x write_ns=1 read_ns=1 direct=1\n"

	got := parseIOSamples(logs)
	if len(got) != 2 {
		t.Fatalf("expected 2 volumes, got %+v", got)
	}
	v0 := got[0]
	if v0.Volume != "vol0" || v0.Iterations != 2 || !v0.DirectRead ||
		math.Abs(v0.WriteMiBps-10) > 1e-9 || math.Abs(v0.ReadMiBps-20) > 1e-9 {
		t.Errorf("unexpected vol0 sample %+v", v0)
	}
	if v1 := got[1]; v1.DirectRead || math.Abs(v1.WriteMiBps-5) > 1e-9 {
		t.Errorf("unexpected vol1 sample %+v", v1)
	}
}

func TestPercentiles(t *testing.T) {
	vals := []float64{5, 1, 4, 2, 3, 10, 9, 8, 7, 6}
	p := percentiles(vals, 0, 0.5, 0.95, 1)
	want := []float64{1, 5, 10, 10}
	for i := range want {
		if p[i] != want[i] {
			t.Errorf("percentile %d: got %v want %v", i, p[i], want[i])
		}
	}
	if p := percentiles(nil, 0.5); p[0] != 0 {
		t.Errorf("empty input should yield 0, got %v", p[0])
	}
}

func TestBalancerCapacity(t *testing.T) {
	b := &nodeBalancer{nodes: map[string]*nodeSlot{
		"n1": {eligible: true, maxVols: 10, external: 4},
	}}
	node, occ := b.tryAcquire(5)
	if node != "n1" || occ != 4 {
		t.Fatalf("first acquire: got %q occ=%d", node, occ)
	}
	// 4 external + 5 in flight + 2 > 10
	if node, _ := b.tryAcquire(2); node != "" {
		t.Errorf("acquire above the attach limit should block, got %q", node)
	}
	b.ignoreLimit = true
	node, occ = b.tryAcquire(2)
	if node != "n1" || occ != 9 {
		t.Errorf("acquire with ignoreLimit: got %q occ=%d", node, occ)
	}
	b.release("n1", 2)
	if s := b.nodes["n1"]; s.vols != 5 || s.pods != 1 || s.peak != 11 {
		t.Errorf("unexpected slot after release %+v", s)
	}
}

func TestOccupancyAndBuckets(t *testing.T) {
	m := newMetrics()
	start := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 25; i++ {
		occ := i
		m.recordCycle(&cycleResult{Node: "n1", PVCs: 1, NodeVols: &occ, Status: statusSuccess,
			Start: start.Add(time.Duration(i) * 10 * time.Minute), LatenciesMs: map[string][]float64{"attach_mount": {float64(1000 + i*100)}}})
	}
	if m.peakNodeVol != 25 || len(m.occupancy) != 25 {
		t.Fatalf("peak=%d occupancy samples=%d", m.peakNodeVol, len(m.occupancy))
	}
	var out strings.Builder
	m.printOccupancy(&out)
	// maxOcc 24 -> width 3 -> ranges 0-2 ... 24-26
	if !strings.Contains(out.String(), "0-2  ") || !strings.Contains(out.String(), "24-26") {
		t.Errorf("unexpected occupancy table:\n%s", out.String())
	}

	buckets := map[time.Time]*metrics{}
	for _, h := range []int{0, 2} { // hour 1 is empty and must still be printed
		bm := newMetrics()
		bm.recordCycle(&cycleResult{Status: statusSuccess, LatenciesMs: map[string][]float64{"attach_mount": {2000}}})
		buckets[start.Add(time.Duration(h)*time.Hour)] = bm
	}
	out.Reset()
	printBuckets(&out, buckets, time.Hour)
	if got := strings.Count(out.String(), "2026-10-29T"); got != 3 {
		t.Errorf("expected 3 window rows, got %d:\n%s", got, out.String())
	}
}

func TestAuditFormat(t *testing.T) {
	a := &auditor{baseline: auditCounts{orphanVA: 1}}
	line, grew := a.format(auditCounts{orphanVA: 1, pvReleased: 2})
	if !grew || !strings.Contains(line, "orphan_va=1 ") || !strings.Contains(line, "pv_released=2(+2)") {
		t.Errorf("unexpected audit line %q grew=%v", line, grew)
	}
	if _, grew := a.format(auditCounts{orphanVA: 1}); grew {
		t.Error("counts equal to the baseline must not be reported as growth")
	}
}
