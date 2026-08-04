package rawequiv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/rawequiv"
)

// The cases here are built from the committed L1 wire recordings rather than
// from hand-written JSON, because the property under test is about real
// provider framing: an SSE stream reduced two ways. A synthetic body would
// exercise the comparison and prove nothing about the reduction.
//
// No new fixture files are added. Each recording is turned into the raw_turns
// row it would have produced under dual-send — verbatim bytes in
// raw_response, the adapter's live reduction in response — entirely in memory.

// recordingMeta is the subset of a recording's meta.json these tests read.
type recordingMeta struct {
	Status          int     `json:"status"`
	ContentType     string  `json:"content_type"`
	ContentEncoding string  `json:"content_encoding"`
	DurationMS      float64 `json:"duration_ms"`
	TsRequest       string  `json:"ts_request"`
	TsComplete      string  `json:"ts_complete"`
}

// recording is one committed turn bundle.
type recording struct {
	name string
	resp []byte
	meta recordingMeta
}

// recordingsDir resolves fixtures/recordings relative to this source file, so
// the tests do not depend on the working directory.
func recordingsDir() string {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "runtime.Caller failed")
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "recordings")
}

// loadRecordings reads every committed turn bundle. It fails rather than skips
// on an empty corpus: a silently empty corpus turns every spec below into a
// vacuous pass, which is the failure mode a fixture-driven gate must not have.
func loadRecordings() []recording {
	root := recordingsDir()
	sets, err := os.ReadDir(root)
	Expect(err).NotTo(HaveOccurred(), "reading %s", root)

	var out []recording
	for _, set := range sets {
		if !set.IsDir() {
			continue
		}
		turns, err := os.ReadDir(filepath.Join(root, set.Name()))
		Expect(err).NotTo(HaveOccurred())
		for _, turn := range turns {
			if !turn.IsDir() {
				continue
			}
			dir := filepath.Join(root, set.Name(), turn.Name())

			resp, err := os.ReadFile(filepath.Join(dir, "response.sse"))
			if err != nil {
				continue
			}
			metaRaw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
			Expect(err).NotTo(HaveOccurred())

			var meta recordingMeta
			Expect(json.Unmarshal(metaRaw, &meta)).To(Succeed())

			out = append(out, recording{
				name: set.Name() + "/" + turn.Name(),
				resp: resp,
				meta: meta,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	Expect(out).NotTo(BeEmpty(), "no wire recordings found under %s", root)
	return out
}

// turnMeta builds the capture meta block the row would carry.
func (r recording) turnMeta() ingest.TurnMeta {
	return ingest.TurnMeta{
		RequestID:       "fixture-" + r.name,
		ContentType:     r.meta.ContentType,
		ContentEncoding: r.meta.ContentEncoding,
		Model:           "claude-sonnet-4-6",
		ElapsedSeconds:  r.meta.DurationMS / 1000,
		TsRequest:       r.meta.TsRequest,
	}
}

// adapterReduction reproduces what a dual-send producer stores in
// raw_turns.response: the shared reducer's output over the same bytes, plus
// the two capture-side stamps only the live producer can take.
//
// It deliberately uses the same pkg/capture reducer the server side uses,
// because that is the real deployment — extproc and ingest share the library.
// What differs between the two paths, and what these specs exist to hold, is
// everything around it: the storage round-trip, the encoding layer, and the
// clock.
func (r recording) adapterReduction() *llm.ChatResponse {
	decoded, _, err := capture.DecodeContentEncoding(r.resp, r.meta.ContentEncoding)
	Expect(err).NotTo(HaveOccurred(), "decoding %s", r.name)

	resp, err := capture.NewAnthropicReducer().Reduce(
		context.Background(), nil, bytes.NewReader(decoded), r.meta.ContentType)
	Expect(err).NotTo(HaveOccurred(), "reducing %s", r.name)
	Expect(resp).NotTo(BeNil())

	// The producer measured the call and stamped its own clock. Both values
	// are deliberately unlike anything a re-reduction could produce, so a
	// spec that passes proves the tolerance is doing the work rather than the
	// two sides coincidentally agreeing.
	if resp.Usage == nil {
		resp.Usage = &llm.Usage{}
	}
	resp.Usage.TotalDurationNs = int64(r.meta.DurationMS * float64(time.Millisecond))
	if ts, err := time.Parse(time.RFC3339Nano, r.meta.TsComplete); err == nil {
		resp.CreatedAt = ts.UTC()
	}
	return resp
}

// row assembles the raw_turns row this recording would have produced under
// dual-send.
func (r recording) row() rawequiv.Row {
	stored, err := json.Marshal(r.adapterReduction())
	Expect(err).NotTo(HaveOccurred())

	return rawequiv.Row{
		ID:                  1,
		RequestID:           "fixture-" + r.name,
		Provider:            capture.ProviderAnthropic,
		HarnessID:           "claude-code",
		HarnessSessionID:    "fixture-session",
		RawResponse:         r.resp,
		RawResponseEncoding: r.meta.ContentEncoding,
		StoredReduction:     stored,
		Meta:                r.turnMeta(),
	}
}

// storedTree decodes a row's stored reduction for mutation.
func storedTree(row rawequiv.Row) map[string]any {
	var tree map[string]any
	Expect(json.Unmarshal(row.StoredReduction, &tree)).To(Succeed())
	return tree
}

// withStored re-encodes a mutated reduction back onto a row.
func withStored(row rawequiv.Row, tree map[string]any) rawequiv.Row {
	encoded, err := json.Marshal(tree)
	Expect(err).NotTo(HaveOccurred())
	row.StoredReduction = encoded
	return row
}
