package devcmder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
)

const checkInvariantsLongDesc string = `Assert structural invariants on trace wire.

Reads composite session-traces JSON (the GET /v1/sessions/{id}/traces
response, as written by ` + "`tapes dev trace-fixtures`" + `) and checks the
Tier-3 structural invariants from the re-derive test plan — the
properties the console and downstream consumers rely on but zod schemas
don't express:

  1. schema == the served projection generation (api.ProjectionSchema)
  2. within each trace, spans are seq-ordered and the first span is the
     root agent span (kind=agent, no parent)
  3. span ids are unique within a trace; no two tool spans share an id
  4. every session-scoped link resolves to a real (trace, span) — no
     dangling causality edges
  5. kind_counts sums to the number of spans with a non-empty call_kind
     (kindFold counts every non-empty call_kind, event spans included)
  6. no dropped-model field leaked back onto the wire (root_count,
     metrics, metadata grab-bag, span id/start_ns/children_ids, …)

Runs over a fixture directory (every session-traces-<s>.json, skipping
the .slim previews) or an explicit file. Exits non-zero on any
violation.

Example:
  tapes dev dump-corpus --postgres "$DSN" --all --out ./corpus-local/
  for c in ./corpus-local/*.jsonl.gz; do
    tapes dev trace-fixtures --corpus "$c" --out /tmp/wire
  done
  tapes dev check-invariants /tmp/wire`

type checkInvariantsCommander struct{}

func newCheckInvariantsCmd() *cobra.Command {
	cmder := &checkInvariantsCommander{}
	cmd := &cobra.Command{
		Use:   "check-invariants <wire-path>...",
		Short: "Assert structural invariants on trace wire",
		Long:  checkInvariantsLongDesc,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return cmder.run(cmd, args)
		},
	}
	return cmd
}

func (c *checkInvariantsCommander) run(cmd *cobra.Command, paths []string) error {
	files, err := collectComposites(paths)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no composite session-traces JSON found under %s", strings.Join(paths, ", "))
	}

	failed := 0
	for _, file := range files {
		violations, err := checkComposite(file)
		if err != nil {
			cmd.Printf("✘ %s: %v\n", filepath.Base(file), err)
			failed++
			continue
		}
		if len(violations) > 0 {
			failed++
			cmd.Printf("✘ %s\n", filepath.Base(file))
			for _, v := range violations {
				cmd.Printf("    %s\n", v)
			}
			continue
		}
		cmd.Printf("✓ %s\n", filepath.Base(file))
	}

	cmd.Printf("checked %d composite file(s), %d failed\n", len(files), failed)
	if failed > 0 {
		return fmt.Errorf("%d file(s) violated structural invariants", failed)
	}
	return nil
}

// collectComposites resolves paths to the full composite fixtures:
// session-traces-<s>.json, excluding the .slim previews and the
// summaries/details files.
func collectComposites(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, p)
			continue
		}
		entries, err := os.ReadDir(p)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if isCompositeFixture(e.Name()) {
				out = append(out, filepath.Join(p, e.Name()))
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func isCompositeFixture(name string) bool {
	return strings.HasPrefix(name, "session-traces-") &&
		strings.HasSuffix(name, ".json") &&
		!strings.HasSuffix(name, ".slim.json")
}

// checkComposite runs every invariant over one composite response file.
func checkComposite(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return checkCompositeBytes(raw)
}

// checkCompositeBytes runs every invariant over one composite response
// and returns the human-readable violations (empty when clean).
func checkCompositeBytes(raw []byte) ([]string, error) {
	var resp api.SessionTracesResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return slices.Concat(
		checkSchema(&resp),
		checkSpanOrderAndAgent(&resp),
		checkSpanIDUniqueness(&resp),
		checkLinksResolve(&resp),
		checkKindCounts(&resp),
		checkNoDroppedFields(raw),
	), nil
}

// (1) schema stamps the served projection generation.
func checkSchema(resp *api.SessionTracesResponse) []string {
	if resp.Schema != api.ProjectionSchema {
		return []string{fmt.Sprintf("schema = %q, want %q", resp.Schema, api.ProjectionSchema)}
	}
	return nil
}

// (2) spans are seq-ordered within a trace and the first is the root
// agent span.
func checkSpanOrderAndAgent(resp *api.SessionTracesResponse) []string {
	var v []string
	for _, t := range resp.Traces {
		if len(t.Spans) == 0 {
			v = append(v, fmt.Sprintf("trace %s has no spans", t.Trace.TraceID))
			continue
		}
		for i := 1; i < len(t.Spans); i++ {
			if t.Spans[i].Seq < t.Spans[i-1].Seq {
				v = append(v, fmt.Sprintf("trace %s spans not seq-ordered at index %d (%d < %d)",
					t.Trace.TraceID, i, t.Spans[i].Seq, t.Spans[i-1].Seq))
			}
		}
		if first := t.Spans[0]; first.Kind != "agent" || first.ParentSpanID != "" {
			v = append(v, fmt.Sprintf("trace %s first span is kind=%q parent=%q, want root agent span",
				t.Trace.TraceID, first.Kind, first.ParentSpanID))
		}
	}
	return v
}

// (3) span ids are unique within a trace, and no two tool spans share an
// id anywhere (span_id == tool_use_id for tool spans, so a dup means a
// tool_use projected to two spans).
func checkSpanIDUniqueness(resp *api.SessionTracesResponse) []string {
	var v []string
	toolSeen := map[string]string{} // span_id -> trace_id of first tool span
	for _, t := range resp.Traces {
		seen := map[string]bool{}
		for _, s := range t.Spans {
			if seen[s.SpanID] {
				v = append(v, fmt.Sprintf("trace %s has duplicate span_id %s", t.Trace.TraceID, s.SpanID))
			}
			seen[s.SpanID] = true
			if s.Kind == "tool" {
				if prev, dup := toolSeen[s.SpanID]; dup {
					v = append(v, fmt.Sprintf("tool span_id %s appears in traces %s and %s", s.SpanID, prev, t.Trace.TraceID))
				} else {
					toolSeen[s.SpanID] = t.Trace.TraceID
				}
			}
		}
	}
	return v
}

// (4) every session-scoped link resolves to a real (trace, span).
func checkLinksResolve(resp *api.SessionTracesResponse) []string {
	spanKey := func(traceID, spanID string) string { return traceID + "\x00" + spanID }
	spans := map[string]bool{}
	for _, t := range resp.Traces {
		for _, s := range t.Spans {
			spans[spanKey(s.TraceID, s.SpanID)] = true
		}
	}
	var v []string
	for i, l := range resp.Links {
		if !spans[spanKey(l.FromTraceID, l.FromSpanID)] {
			v = append(v, fmt.Sprintf("link[%d] (%s) from-endpoint %s/%s is dangling", i, l.Kind, l.FromTraceID, l.FromSpanID))
		}
		if !spans[spanKey(l.ToTraceID, l.ToSpanID)] {
			v = append(v, fmt.Sprintf("link[%d] (%s) to-endpoint %s/%s is dangling", i, l.Kind, l.ToTraceID, l.ToSpanID))
		}
	}
	return v
}

// (5) kind_counts sums to the number of spans with a non-empty call_kind.
func checkKindCounts(resp *api.SessionTracesResponse) []string {
	want := 0
	for _, t := range resp.Traces {
		for _, s := range t.Spans {
			if s.CallKind != "" {
				want++
			}
		}
	}
	got := 0
	for kind, n := range resp.Session.Rollup.KindCounts {
		if n < 0 {
			return []string{fmt.Sprintf("kind_counts[%q] = %d is negative", kind, n)}
		}
		got += n
	}
	if got != want {
		return []string{fmt.Sprintf("kind_counts sums to %d, want %d (spans with non-empty call_kind)", got, want)}
	}
	return nil
}

// droppedFields are keys the wire reshape removed; their reappearance is
// a regression. Keyed by the object level they must be absent from.
var (
	droppedTopLevel   = []string{"root_count", "nodes", "stems"}
	droppedTraceLevel = []string{"session_id", "harness", "id"}
	droppedSpanLevel  = []string{"metrics", "metadata", "id", "start_ns", "children_ids", "node_hash"}
)

// (6) no dropped-model field leaked back onto the wire. Walks the raw
// JSON generically so a field the typed structs would silently ignore is
// still caught.
func checkNoDroppedFields(raw []byte) []string {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return []string{fmt.Sprintf("re-decode for dropped-field scan: %v", err)}
	}
	var v []string
	for _, k := range droppedTopLevel {
		if _, ok := doc[k]; ok {
			v = append(v, fmt.Sprintf("dropped field %q present at response top level", k))
		}
	}
	traces := []map[string]json.RawMessage{}
	if err := json.Unmarshal(doc["traces"], &traces); err != nil {
		return v // no traces array to scan; other invariants already flag structure
	}
	for ti, t := range traces {
		if trace, ok := decodeObject(t["trace"]); ok {
			for _, k := range droppedTraceLevel {
				if _, present := trace[k]; present {
					v = append(v, fmt.Sprintf("traces[%d].trace has dropped field %q", ti, k))
				}
			}
		}
		spans := []map[string]json.RawMessage{}
		if err := json.Unmarshal(t["spans"], &spans); err != nil {
			continue
		}
		for si, s := range spans {
			for _, k := range droppedSpanLevel {
				if _, present := s[k]; present {
					v = append(v, fmt.Sprintf("traces[%d].spans[%d] has dropped field %q", ti, si, k))
				}
			}
		}
	}
	return v
}

func decodeObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false
	}
	return m, true
}
