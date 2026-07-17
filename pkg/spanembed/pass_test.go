package spanembed_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
)

const testOrg = "11111111-1111-1111-1111-111111111111"

// fakeStore implements spanembed.Source and spanembed.Sink in memory.
type fakeStore struct {
	candidates []spanembed.Candidate
	records    map[string]spanembed.ChunkRecord
	failures   map[string]spanembed.FailureRecord
	attempts   map[string]int
	pruned     int64
	listErr    error
	upsertErr  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		records:  map[string]spanembed.ChunkRecord{},
		failures: map[string]spanembed.FailureRecord{},
		attempts: map[string]int{},
	}
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
			// reflect any embedding or failure this pass already wrote,
			// the way a second DB read (the real LEFT JOINs) would
			if rec, ok := f.records[key.TraceID+"|"+key.SpanID]; ok {
				c.ExistingHash = rec.ContentHash
				c.ExistingModel = rec.Model
			}
			if fail, ok := f.failures[key.TraceID+"|"+key.SpanID]; ok {
				c.ExistingFailHash = fail.ContentHash
				c.ExistingFailModel = fail.Model
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

func (f *fakeStore) UpsertSpanChunks(_ context.Context, rec spanembed.ChunkRecord) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	key := rec.TraceID + "|" + rec.SpanID
	f.records[key] = rec
	delete(f.failures, key) // a successful write clears any failure marker
	return nil
}

func (f *fakeStore) RecordFailure(_ context.Context, rec spanembed.FailureRecord) error {
	key := rec.TraceID + "|" + rec.SpanID
	f.failures[key] = rec
	f.attempts[key]++
	return nil
}

func (f *fakeStore) PruneOrphans(context.Context) (int64, error) {
	return f.pruned, nil
}

// countingEmbedder returns fixed-size vectors and records the texts it
// was asked to embed. It can simulate the model rejecting oversized input,
// either above a rune threshold or unconditionally, so the chunking and
// failure paths can be exercised without a real provider.
type countingEmbedder struct {
	dims           int
	texts          []string
	fail           error
	oversizeOver   int  // reject text longer than this many runes as oversize (0 = never)
	tokenScale     int  // reported tokens per rune in an oversize error (default 1)
	alwaysOversize bool // reject every input as oversize
}

func (e *countingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if e.fail != nil {
		return nil, e.fail
	}
	runes := len([]rune(text))
	if e.alwaysOversize || (e.oversizeOver > 0 && runes > e.oversizeOver) {
		scale := max(e.tokenScale, 1)
		tokens := runes * scale
		return nil, &embeddings.APIError{
			Status:          400,
			Code:            "context_length_exceeded",
			Message:         fmt.Sprintf("maximum context length is 8192 tokens, however you requested %d tokens", tokens),
			RequestedTokens: tokens,
		}
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

	Describe("poison gate", func() {
		It("skips a span that already failed under the same content and model", func() {
			store.candidates = []spanembed.Candidate{
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_big",
					Input:             textBlocks("enormous prompt"),
					ExistingFailHash:  spanembed.ContentHash("enormous prompt"),
					ExistingFailModel: "m",
				},
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Poisoned).To(Equal(1))
			Expect(report.Embedded).To(BeZero())
			Expect(embedder.texts).To(BeEmpty())
		})

		It("retries a poisoned span once its content changes", func() {
			store.candidates = []spanembed.Candidate{
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_big",
					Input:             textBlocks("smaller prompt now"),
					ExistingFailHash:  spanembed.ContentHash("the old enormous prompt"),
					ExistingFailModel: "m",
				},
			}
			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Poisoned).To(BeZero())
			Expect(report.Embedded).To(Equal(1))
		})
	})

	Describe("oversize chunking", func() {
		It("splits an oversized span and stores one embedding per piece", func() {
			embedder = &countingEmbedder{dims: 4, oversizeOver: 10}
			big := strings.Repeat("a", 30) // no newlines: exact halving
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_big", textBlocks(big), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(1))
			Expect(report.Chunked).To(Equal(1))
			Expect(report.Failed).To(BeZero())

			rec := store.records["trc_a|llm_big"]
			Expect(len(rec.Embeddings)).To(BeNumerically(">", 1))
			// Every piece embedded, and the pieces reassemble to the original.
			Expect(strings.Join(embedder.texts, "")).To(Equal(big))
			for _, piece := range embedder.texts {
				Expect(len([]rune(piece))).To(BeNumerically("<=", 10))
			}
		})

		It("uses the reported token count to choose the number of pieces", func() {
			// 4000 runes reported as 32000 tokens -> ceil(32000/8000) = 4 pieces,
			// each 1000 runes and under the oversize threshold.
			embedder = &countingEmbedder{dims: 4, oversizeOver: 1500, tokenScale: 8}
			big := strings.Repeat("x", 4000)
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_big", textBlocks(big), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Embedded).To(Equal(1))
			Expect(store.records["trc_a|llm_big"].Embeddings).To(HaveLen(4))
		})

		It("records a deterministic failure when the text cannot be split small enough", func() {
			embedder = &countingEmbedder{dims: 4, alwaysOversize: true}
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_big", textBlocks(strings.Repeat("a", 40)), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(Equal(1))
			Expect(report.Embedded).To(BeZero())
			Expect(store.records).To(BeEmpty())

			fail, ok := store.failures["trc_a|llm_big"]
			Expect(ok).To(BeTrue())
			Expect(fail.Reason).To(Equal("oversize"))
			Expect(store.attempts["trc_a|llm_big"]).To(Equal(1))
		})
	})

	Describe("failure classification", func() {
		It("records a non-oversize 4xx as a deterministic failure", func() {
			embedder.fail = &embeddings.APIError{Status: 400, Message: "Invalid value for 'dimensions'."}
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_1", textBlocks("hi"), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(Equal(1))
			Expect(store.failures["trc_a|llm_1"].Reason).To(Equal("api_400"))
		})

		It("records a span over MaxTextBytes as too_large instead of chunking it", func() {
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_huge", textBlocks(strings.Repeat("a", 100)), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4, MaxTextBytes: 50}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(Equal(1))
			Expect(report.Embedded).To(BeZero())
			Expect(report.FailuresByReason).To(HaveKeyWithValue("too_large", 1))
			Expect(store.failures["trc_a|llm_huge"].Reason).To(Equal("too_large"))
			Expect(embedder.texts).To(BeEmpty()) // embedder never called for an over-cap span
		})

		It("retries a transient failure without recording it", func() {
			embedder.fail = &embeddings.APIError{Status: 503, Message: "service unavailable"}
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_1", textBlocks("hi"), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(Equal(1))
			Expect(store.failures).To(BeEmpty())
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

	Describe("report accounting", func() {
		It("counts span outcomes, oversize splits, and chunk rows in the Report", func() {
			embedder = &countingEmbedder{dims: 4, oversizeOver: 10}
			store.candidates = []spanembed.Candidate{
				mainSpan("trc_a", "llm_norm", textBlocks("short"), nil),
				mainSpan("trc_a", "llm_big", textBlocks(strings.Repeat("a", 30)), nil),
				mainSpan("trc_a", "llm_empty", nil, json.RawMessage(`[{"type":"tool_use","tool_use_id":"t","tool_name":"Bash"}]`)),
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_dup",
					Input:        textBlocks("unchanged"),
					ExistingHash: spanembed.ContentHash("unchanged"), ExistingModel: "m",
				},
				{
					OrgID: testOrg, TraceID: "trc_a", SpanID: "llm_pois",
					Input:            textBlocks("bad"),
					ExistingFailHash: spanembed.ContentHash("bad"), ExistingFailModel: "m",
				},
			}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(report.Embedded).To(Equal(2))
			Expect(report.UpToDate).To(Equal(1))
			Expect(report.Empty).To(Equal(1))
			Expect(report.Poisoned).To(Equal(1))
			Expect(report.Chunked).To(Equal(1))   // only llm_big split
			Expect(report.Oversized).To(Equal(1)) // only llm_big oversize
			Expect(report.OversizeTokens).To(HaveLen(1))
			Expect(report.ChunkRows).To(BeNumerically(">=", 3)) // 1 (norm) + >=2 (big)
		})

		It("records deterministic failures by reason in the Report", func() {
			embedder = &countingEmbedder{dims: 4, alwaysOversize: true}
			store.candidates = []spanembed.Candidate{mainSpan("trc_a", "llm_huge", textBlocks(strings.Repeat("a", 40)), nil)}

			report, err := newPass(spanembed.PassConfig{Model: "m", Dimensions: 4}).Run(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(report.Failed).To(Equal(1))
			Expect(report.FailuresByReason).To(HaveKeyWithValue("oversize", 1))
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

	It("strips harness tag spans so boilerplate cannot dominate the vector", func() {
		input := json.RawMessage(`[
			{"type":"text","text":"<system-reminder>\nSessionStart hook: several KB of skill instructions\n</system-reminder>Explain autovacuum tuning"},
			{"type":"text","text":"<system-reminder>only noise</system-reminder>"}
		]`)
		output := textBlocks("Set scale factors low.")
		Expect(spanembed.RenderSpanText(input, output)).To(Equal("Explain autovacuum tuning\nSet scale factors low."))
	})
})
