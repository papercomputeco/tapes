package spanembed_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
)

const testOrg = "11111111-1111-1111-1111-111111111111"

// fakeStore implements spanembed.Source and spanembed.Sink in memory.
type fakeStore struct {
	candidates []spanembed.Candidate
	records    map[string]spanembed.Record
	pruned     int64
	listErr    error
	upsertErr  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: map[string]spanembed.Record{}}
}

func (f *fakeStore) ListCandidates(_ context.Context, after spanembed.Key, limit int) ([]spanembed.Candidate, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []spanembed.Candidate
	for _, c := range f.candidates {
		key := c.Key()
		if [3]string{key.OrgID, key.TraceID, key.SpanID} == [3]string{after.OrgID, after.TraceID, after.SpanID} {
			continue
		}
		if keyLess(after, key) {
			// reflect any embedding this pass already wrote, the way
			// a second DB read would
			if rec, ok := f.records[key.TraceID+"|"+key.SpanID]; ok {
				c.ExistingHash = rec.ContentHash
				c.ExistingModel = rec.Model
			}
			out = append(out, c)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func keyLess(a, b spanembed.Key) bool {
	if a.OrgID != b.OrgID {
		return a.OrgID < b.OrgID
	}
	if a.TraceID != b.TraceID {
		return a.TraceID < b.TraceID
	}
	return a.SpanID < b.SpanID
}

func (f *fakeStore) Upsert(_ context.Context, rec spanembed.Record) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.records[rec.TraceID+"|"+rec.SpanID] = rec
	return nil
}

func (f *fakeStore) PruneOrphans(context.Context) (int64, error) {
	return f.pruned, nil
}

// countingEmbedder returns fixed-size vectors and records the texts it
// was asked to embed.
type countingEmbedder struct {
	dims  int
	texts []string
	fail  error
}

func (e *countingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if e.fail != nil {
		return nil, e.fail
	}
	e.texts = append(e.texts, text)
	return make([]float32, e.dims), nil
}

func (e *countingEmbedder) Close() error { return nil }

func textBlocks(texts ...string) json.RawMessage {
	blocks := make([]map[string]string, 0, len(texts))
	for _, t := range texts {
		blocks = append(blocks, map[string]string{"type": "text", "text": t})
	}
	raw, err := json.Marshal(blocks)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func mainSpan(trace, span string, input, output json.RawMessage) spanembed.Candidate {
	return spanembed.Candidate{
		OrgID:   testOrg,
		TraceID: trace,
		SpanID:  span,
		Input:   input,
		Output:  output,
	}
}

var _ = Describe("Pass", func() {
	var (
		store    *fakeStore
		embedder *countingEmbedder
		ctx      context.Context
	)

	newPass := func(cfg spanembed.PassConfig) *spanembed.Pass {
		pass, err := spanembed.NewPass(store, store, embedder, cfg, logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return pass
	}

	BeforeEach(func() {
		store = newFakeStore()
		embedder = &countingEmbedder{dims: 4}
		ctx = context.Background()
	})

	Describe("NewPass", func() {
		It("requires an explicit model", func() {
			_, err := spanembed.NewPass(store, store, embedder, spanembed.PassConfig{Dimensions: 4}, logger.NewNoop())
			Expect(err).To(MatchError(ContainSubstring("model")))
		})

		It("requires explicit dimensions", func() {
			_, err := spanembed.NewPass(store, store, embedder, spanembed.PassConfig{Model: "m"}, logger.NewNoop())
			Expect(err).To(MatchError(ContainSubstring("dimensions")))
		})
	})

	Describe("selection", func() {
		It("embeds the delta text of un-embedded spans", func() {
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_1", textBlocks("fix the retry backoff"), textBlocks("done, capped at 30s")),
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(1))
			Expect(embedder.texts).To(ConsistOf("fix the retry backoff\ndone, capped at 30s"))

			rec := store.records["trc_a|llm_1"]
			Expect(rec.Model).To(Equal("m"))
			Expect(rec.ContentHash).To(Equal(spanembed.ContentHash("fix the retry backoff\ndone, capped at 30s")))
			Expect(rec.OrgID).To(Equal(testOrg))
		})

		It("skips spans whose delta renders to no text", func() {
			toolOnly := json.RawMessage(`[{"type":"tool_use","tool_use_id":"tu_1","tool_name":"Bash"}]`)
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_1", nil, toolOnly),
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Empty).To(Equal(1))
			Expect(report.Embedded).To(BeZero())
			Expect(store.records).To(BeEmpty())
		})
	})

	Describe("idempotency", func() {
		It("re-running embeds nothing when content and model are unchanged", func() {
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_1", textBlocks("prompt"), textBlocks("answer")),
			}
			pass := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4})

			first, err := pass.Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(first.Embedded).To(Equal(1))

			second, err := pass.Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(second.Embedded).To(BeZero())
			Expect(second.UpToDate).To(Equal(1))
			Expect(embedder.texts).To(HaveLen(1))
		})

		It("re-embeds when the span's content changed", func() {
			store.candidates = []spanembed.Candidate{
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_1",
					Input:         textBlocks("new content"),
					ExistingHash:  spanembed.ContentHash("old content"),
					ExistingModel: "m",
				},
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(1))
		})

		It("re-embeds when the configured model changed", func() {
			store.candidates = []spanembed.Candidate{
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_1",
					Input:         textBlocks("same content"),
					ExistingHash:  spanembed.ContentHash("same content"),
					ExistingModel: "old-model",
				},
			}
			report, err := newPass(spanembed.PassConfig{Model: "new-model", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(1))
			Expect(store.records["trc_a|llm_1"].Model).To(Equal("new-model"))
		})
	})

	Describe("error discipline", func() {
		It("counts per-span embed failures and keeps going", func() {
			embedder.fail = errors.New("backend down")
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_1", textBlocks("one"), nil),
				mainSpan("trc_a", "llm_2", textBlocks("two"), nil),
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(Equal(2))
			Expect(report.Embedded).To(BeZero())
		})

		It("aborts with a clear error when the model's dimensionality disagrees with the configuration", func() {
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_1", textBlocks("text"), nil),
			}
			_, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 8}).Run(ctx)
			Expect(err).To(MatchError(ContainSubstring("returned 4 dimensions but 8 are configured")))
		})

		It("aborts on candidate-listing failures", func() {
			store.listErr = errors.New("db gone")
			_, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).To(MatchError(ContainSubstring("db gone")))
		})
	})

	Describe("pagination", func() {
		It("walks every page", func() {
			for i := range 7 {
				store.candidates = append(store.candidates,
					mainSpan("trc_a", fmt.Sprintf("llm_%02d", i), textBlocks(fmt.Sprintf("text %d", i)), nil))
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4, BatchSize: 3}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(7))
			Expect(store.records).To(HaveLen(7))
		})
	})
})

var _ = Describe("RenderSpanText", func() {
	It("concatenates input then output text blocks", func() {
		input := textBlocks("question")
		output := textBlocks("first", "second")
		Expect(spanembed.RenderSpanText(input, output)).To(Equal("question\nfirst\nsecond"))
	})

	It("ignores non-text blocks", func() {
		output := json.RawMessage(`[
			{"type":"thinking","thinking":"private"},
			{"type":"tool_use","tool_use_id":"tu_1","tool_name":"Bash","tool_input":{"command":"ls"}},
			{"type":"text","text":"visible"}
		]`)
		Expect(spanembed.RenderSpanText(nil, output)).To(Equal("visible"))
	})

	It("renders undecodable payloads as empty", func() {
		Expect(spanembed.RenderSpanText(json.RawMessage(`{not json`), nil)).To(Equal(""))
	})
})
