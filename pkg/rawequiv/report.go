package rawequiv

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/papercomputeco/tapes/ingest"
)

// Report accumulates outcomes and renders the verdict.
type Report struct {
	// Window describes what was scanned, echoed back so a pasted report says
	// what it covered.
	Window Window `json:"window"`

	// Counts is every class with a non-zero tally.
	Counts map[Class]int `json:"counts"`

	// Total is the number of rows examined.
	Total int `json:"total"`

	// Divergences holds the detail for blocking rows, bounded by MaxReported.
	Divergences []Outcome `json:"divergences,omitempty"`

	// DivergencesTruncated reports that more blocking rows existed than are
	// listed. The counts stay exact regardless.
	DivergencesTruncated bool `json:"divergences_truncated,omitempty"`

	// StampCoverage counts, over rows that were actually re-reduced, which
	// source would have supplied each capture-side stamp under mode=raw.
	StampCoverage StampCoverageReport `json:"stamp_coverage"`

	// Tolerated echoes the fields excluded from the comparison, so a report
	// carries its own definition of equivalence rather than depending on the
	// reader knowing it.
	Tolerated []ToleratedNote `json:"tolerated"`

	maxReported int
}

// Window records the scan parameters.
type Window struct {
	Since   string `json:"since,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Session string `json:"session,omitempty"`
}

// ToleratedNote is a serializable TolerableField.
type ToleratedNote struct {
	Field string `json:"field"`
	Why   string `json:"why"`
}

// StampCoverageReport counts stamp sources across re-reduced rows.
type StampCoverageReport struct {
	Reduced   int                        `json:"reduced"`
	Duration  map[ingest.StampSource]int `json:"duration"`
	CreatedAt map[ingest.StampSource]int `json:"created_at"`
}

// NewReport creates an empty report. maxReported bounds how many blocking
// outcomes are retained for detail; the counts are always exact.
func NewReport(window Window, maxReported int) *Report {
	notes := make([]ToleratedNote, 0, len(TolerableFields))
	for _, f := range TolerableFields {
		notes = append(notes, ToleratedNote{Field: f.String(), Why: f.Why})
	}
	return &Report{
		Window: window,
		Counts: map[Class]int{},
		StampCoverage: StampCoverageReport{
			Duration:  map[ingest.StampSource]int{},
			CreatedAt: map[ingest.StampSource]int{},
		},
		Tolerated:   notes,
		maxReported: maxReported,
	}
}

// Add folds one outcome into the report.
func (r *Report) Add(o Outcome) {
	r.Total++
	r.Counts[o.Class]++

	if o.Stamps != nil {
		r.StampCoverage.Reduced++
		r.StampCoverage.Duration[o.Stamps.Duration]++
		r.StampCoverage.CreatedAt[o.Stamps.CreatedAt]++
	}

	if !o.Class.Blocking() {
		return
	}
	if len(r.Divergences) < r.maxReported {
		r.Divergences = append(r.Divergences, o)
		return
	}
	r.DivergencesTruncated = true
}

// Blocking reports the number of rows that fail the run.
func (r *Report) Blocking() int {
	n := 0
	for class, count := range r.Counts {
		if class.Blocking() {
			n += count
		}
	}
	return n
}

// classOrder fixes the reporting order so two runs read the same way.
var classOrder = []Class{
	ClassEquivalent,
	ClassDivergent,
	ClassUndecodable,
	ClassUnreducible,
	ClassNoReducer,
	ClassSkippedNoRaw,
	ClassSkippedDropped,
	ClassSkippedNoReduction,
}

// WriteText renders the human-readable report.
func (r *Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "raw-response equivalence over %d wire turn(s)\n", r.Total)
	if r.Window.Since != "" || r.Window.Session != "" || r.Window.Limit > 0 {
		fmt.Fprintf(w, "  window: since=%s limit=%d session=%s\n",
			orNone(r.Window.Since), r.Window.Limit, orNone(r.Window.Session))
	}
	fmt.Fprintln(w)

	for _, class := range classOrder {
		if n, ok := r.Counts[class]; ok && n > 0 {
			fmt.Fprintf(w, "  %-22s %d\n", string(class), n)
		}
	}
	fmt.Fprintln(w)

	if r.StampCoverage.Reduced > 0 {
		fmt.Fprintf(w, "capture-side stamps mode=raw could restore (%d re-reduced turn(s)):\n",
			r.StampCoverage.Reduced)
		fmt.Fprintf(w, "  usage.total_duration_ns  %s\n", renderSources(r.StampCoverage.Duration))
		fmt.Fprintf(w, "  created_at               %s\n", renderSources(r.StampCoverage.CreatedAt))
		if r.StampCoverage.Duration[ingest.StampSourceFallback] > 0 {
			fmt.Fprintf(w, "  note: %d turn(s) carry no usable meta.elapsed_seconds; under mode=raw\n"+
				"        their duration would land empty and the derived span duration NULL.\n",
				r.StampCoverage.Duration[ingest.StampSourceFallback])
		}
		fmt.Fprintln(w)
	}

	if len(r.Divergences) > 0 {
		fmt.Fprintln(w, "blocking rows:")
		for _, o := range r.Divergences {
			fmt.Fprintf(w, "\n  raw_turn %d  class=%s provider=%s harness=%s model=%s\n",
				o.ID, o.Class, orNone(o.Provider), orNone(o.HarnessID), orNone(o.Model))
			if o.RequestID != "" {
				fmt.Fprintf(w, "    request_id: %s\n", o.RequestID)
			}
			if o.HarnessSessionID != "" {
				fmt.Fprintf(w, "    session:    %s\n", o.HarnessSessionID)
			}
			if o.Truncated {
				fmt.Fprintln(w, "    note:       stored body was truncated and salvaged;"+
					" the adapter saw the whole stream live")
			}
			if o.Reason != "" {
				fmt.Fprintf(w, "    reason:     %s\n", o.Reason)
			}
			for _, d := range o.Diffs {
				fmt.Fprintf(w, "    %-28s %s\n", d.Path, describeDiff(d))
			}
			if o.DiffsTruncated {
				fmt.Fprintln(w, "    (further differences omitted)")
			}
		}
		if r.DivergencesTruncated {
			fmt.Fprintf(w, "\n  (%d further blocking row(s) omitted; counts above are exact)\n",
				r.Blocking()-len(r.Divergences))
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "equivalence excludes, by design:")
	for _, t := range r.Tolerated {
		fmt.Fprintf(w, "  %s\n", t.Field)
	}
	fmt.Fprintln(w)

	if n := r.Blocking(); n > 0 {
		fmt.Fprintf(w, "VERDICT: %d row(s) would not survive mode=raw. Do not ratchet.\n", n)
		return
	}
	if r.Counts[ClassEquivalent] == 0 {
		fmt.Fprintln(w, "VERDICT: no comparable turns in this window. Nothing was proven.")
		return
	}
	fmt.Fprintf(w, "VERDICT: %d turn(s) re-reduce identically. This window supports the ratchet.\n",
		r.Counts[ClassEquivalent])
}

// describeDiff renders one difference as a single line.
func describeDiff(d FieldDiff) string {
	switch d.Kind {
	case DiffMissingInRecomputed:
		return fmt.Sprintf("%s (stored %s, absent server-side)", d.Kind, d.Stored)
	case DiffMissingInStored:
		return fmt.Sprintf("%s (server-side %s, absent in stored)", d.Kind, d.Recomputed)
	case DiffType, DiffValue, DiffLength:
		return fmt.Sprintf("%s stored=%s server-side=%s", d.Kind, d.Stored, d.Recomputed)
	default:
		return fmt.Sprintf("%s stored=%s server-side=%s", d.Kind, d.Stored, d.Recomputed)
	}
}

// renderSources renders a stamp-source tally in a stable order.
func renderSources(m map[ingest.StampSource]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[ingest.StampSource(k)]))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

func orNone(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
