package derive_test

// Golden-equivalence gate for the derive-worker live-memory bound
// (PCC-767). The deriver retains only the unique content it has not seen
// before; to keep the retained set from pinning the multi-MB raw-request
// buffers it was parsed from (jsonv2 is zero-copy, so a retained string
// aliases the whole backing array), the retain path clones every string
// and byte field it keeps. Cloning copies identical bytes, so the derived
// projection MUST be byte-for-byte unchanged.
//
// These specs freeze that projection. Each corpus is re-derived (node
// chains + transcript reconciliation, exactly as deriveCorpus does) and
// projected through EmitSpans, then serialized to a canonical, line-per-
// entity form and compared against a committed golden (gzipped, like the
// corpus fixtures themselves). The golden is generated from the
// pre-change deriver and frozen; a single differing byte after the clone
// change is a regression in the change, never a fixture to re-pin. The
// failure message decompresses and localizes the first differing line, so
// the regression is readable without unpacking the fixture. Regenerate
// intentionally with UPDATE_DERIVE_GOLDEN=1 only when the classifier or
// projection changes on purpose, and explain the diff in the commit
// message.

import (
	"bytes"
	"compress/gzip"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// goldenNode is the content-addressed identity and derived structure of
// one retained node, in capture order: every field cloning could corrupt.
type goldenNode struct {
	Hash            string `json:"hash"`
	ParentHash      string `json:"parent_hash"`
	Kind            string `json:"kind"`
	ThreadID        string `json:"thread_id"`
	ParentToolUseID string `json:"parent_tool_use_id"`
}

// goldenSpan captures a span's identity, structure, and full payload —
// the Input/Output content blocks carry the cloned strings, so any byte
// drift in a clone surfaces here.
type goldenSpan struct {
	SpanID       string             `json:"span_id"`
	ParentSpanID string             `json:"parent_span_id"`
	Kind         string             `json:"kind"`
	CallKind     string             `json:"call_kind"`
	ThreadID     string             `json:"thread_id"`
	Model        string             `json:"model"`
	StopReason   string             `json:"stop_reason"`
	NodeHash     string             `json:"node_hash"`
	Seq          int64              `json:"seq"`
	Input        []llm.ContentBlock `json:"input"`
	Output       []llm.ContentBlock `json:"output"`
}

type goldenLink struct {
	FromTraceID string `json:"from_trace_id"`
	FromSpanID  string `json:"from_span_id"`
	FromIO      string `json:"from_io"`
	ToTraceID   string `json:"to_trace_id"`
	ToSpanID    string `json:"to_span_id"`
	ToIO        string `json:"to_io"`
	Kind        string `json:"kind"`
}

// canonicalJSON marshals one entity and canonicalizes it (RFC 8785: keys
// sorted, whitespace normalized) so map iteration order in ToolInput
// can never make an unchanged projection look different.
func canonicalJSON(v any) string {
	data, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	j := jsontext.Value(data)
	Expect(j.Canonicalize()).To(Succeed())
	return string(j)
}

// canonicalProjection renders the full derived projection as a stable,
// line-per-entity document: one canonical JSON object per node, per span
// (grouped under its turn), and per link. Line granularity keeps a
// failure diff localized to the exact entity that drifted.
func canonicalProjection(set *derive.DerivedSet, spans *derive.SpanSet) []byte {
	var b strings.Builder
	for i, dn := range set.Nodes {
		ph := ""
		if dn.Node.ParentHash != nil {
			ph = *dn.Node.ParentHash
		}
		fmt.Fprintf(&b, "node[%d] %s\n", i, canonicalJSON(goldenNode{
			Hash:            dn.Node.Hash,
			ParentHash:      ph,
			Kind:            dn.Node.Kind,
			ThreadID:        dn.Node.ThreadID,
			ParentToolUseID: dn.Node.ParentToolUseID,
		}))
	}
	for ti, t := range spans.Turns {
		fmt.Fprintf(&b, "turn[%d] %s\n", ti, canonicalJSON(struct {
			TraceID         string `json:"trace_id"`
			Synthetic       string `json:"synthetic"`
			UserPrompt      string `json:"user_prompt"`
			ResponsePreview string `json:"response_preview"`
		}{t.TraceID, t.Synthetic, t.UserPrompt, t.ResponsePreview}))
		for si, s := range t.Spans {
			fmt.Fprintf(&b, "turn[%d].span[%d] %s\n", ti, si, canonicalJSON(goldenSpan{
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				Kind:         s.Kind,
				CallKind:     s.CallKind,
				ThreadID:     s.ThreadID,
				Model:        s.Model,
				StopReason:   s.StopReason,
				NodeHash:     s.NodeHash,
				Seq:          s.Seq,
				Input:        s.Input,
				Output:       s.Output,
			}))
		}
		for li, l := range t.Links {
			fmt.Fprintf(&b, "turn[%d].link[%d] %s\n", ti, li, canonicalJSON(toGoldenLink(l)))
		}
	}
	for i, l := range spans.Links {
		fmt.Fprintf(&b, "link[%d] %s\n", i, canonicalJSON(toGoldenLink(l)))
	}
	return []byte(b.String())
}

func toGoldenLink(l *derive.SpanLink) goldenLink {
	return goldenLink{
		FromTraceID: l.FromTraceID,
		FromSpanID:  l.FromSpanID,
		FromIO:      l.FromIO,
		ToTraceID:   l.ToTraceID,
		ToSpanID:    l.ToSpanID,
		ToIO:        l.ToIO,
		Kind:        l.Kind,
	}
}

// assertGolden compares the rendered projection to a committed (gzipped)
// golden, or rewrites it when UPDATE_DERIVE_GOLDEN is set. A mismatch
// reports the first differing line so the regression is readable.
func assertGolden(name string, got []byte) {
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_DERIVE_GOLDEN") != "" {
		writeGzip(path, got)
		return
	}
	want := readGzip(path)
	if !bytes.Equal(got, want) {
		Fail(firstLineDiff(want, got))
	}
}

func writeGzip(path string, data []byte) {
	f, err := os.Create(path)
	Expect(err).NotTo(HaveOccurred())
	defer f.Close()
	zw := gzip.NewWriter(f)
	_, err = zw.Write(data)
	Expect(err).NotTo(HaveOccurred())
	Expect(zw.Close()).To(Succeed())
}

func readGzip(path string) []byte {
	f, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred(),
		"missing golden %s — regenerate with UPDATE_DERIVE_GOLDEN=1", path)
	defer f.Close()
	zr, err := gzip.NewReader(f)
	Expect(err).NotTo(HaveOccurred())
	defer zr.Close()
	data, err := io.ReadAll(zr)
	Expect(err).NotTo(HaveOccurred())
	return data
}

// firstLineDiff localizes the first divergence between two line-oriented
// documents into a short, readable failure message.
func firstLineDiff(want, got []byte) string {
	wl := strings.Split(string(want), "\n")
	gl := strings.Split(string(got), "\n")
	for i := 0; i < len(wl) || i < len(gl); i++ {
		var w, g string
		if i < len(wl) {
			w = wl[i]
		}
		if i < len(gl) {
			g = gl[i]
		}
		if w != g {
			return fmt.Sprintf(
				"derived projection diverged from golden at line %d\n  want: %s\n   got: %s\n"+
					"(want %d lines, got %d lines) — a clone must preserve every byte; "+
					"do not re-pin unless the projection changed on purpose",
				i+1, truncLine(w), truncLine(g), len(wl), len(gl))
		}
	}
	return "projections differ but no line diverged (trailing bytes?)"
}

func truncLine(s string) string {
	const maxLen = 240
	if len(s) > maxLen {
		return s[:maxLen] + "…"
	}
	return s
}

var _ = Describe("derive projection equivalence golden (PCC-767)", func() {
	DescribeTable("re-derivation is byte-identical to the committed projection",
		func(load func() (*derive.DerivedSet, *derive.ReconcileStats), golden string) {
			set, _ := load()
			spans := derive.EmitSpans(set)
			assertGolden(golden, canonicalProjection(set, spans))
		},
		Entry("cb9a87e5 — plan mode, 2 subagents", deriveAdvanced, "golden-derive-cb9a87e5.txt.gz"),
		Entry("9fec0da7 — compaction, multi-model", deriveSuperAdvanced, "golden-derive-9fec0da7.txt.gz"),
		Entry("0440f43d — 19 sessions, scale", deriveResume, "golden-derive-0440f43d.txt.gz"),
	)
})
