package ingest

import (
	"math"
	"testing"
)

// usableElapsed guards every conversion of meta.elapsed_seconds into
// timing data; a value it wrongly accepts becomes a bogus duration or a
// CreatedAt far in the future.
func TestUsableElapsed(t *testing.T) {
	cases := []struct {
		name    string
		elapsed float64
		want    bool
	}{
		{"typical call", 1.75, true},
		{"upper bound inclusive", maxElapsedSeconds, true},
		{"zero is absent", 0, false},
		{"negative is corrupt", -3, false},
		{"beyond the bound is corrupt", maxElapsedSeconds + 1, false},
		{"NaN is corrupt", math.NaN(), false},
		{"+Inf is corrupt", math.Inf(1), false},
		{"-Inf is corrupt", math.Inf(-1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := usableElapsed(tc.elapsed); got != tc.want {
				t.Fatalf("usableElapsed(%v) = %v, want %v", tc.elapsed, got, tc.want)
			}
		})
	}
}
