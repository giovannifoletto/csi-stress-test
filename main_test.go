package main

import (
	"math"
	"testing"
)

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
