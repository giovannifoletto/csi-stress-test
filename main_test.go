package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	first, last, n, err := loadResults(m, []string{a, b})
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

	if _, _, _, err := loadResults(newMetrics(), []string{filepath.Join(dir, "missing.jsonl")}); err == nil {
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
