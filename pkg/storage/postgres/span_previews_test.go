package postgres_test

// Stored previews: the deriver writes input_preview / output_preview in
// the same upsert as the payload, as a pure function of it, and outside
// content_hash — so a preview read never detoasts the payload and writing a
// preview never advances the change-feed cursor.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

var _ = Describe("span previews [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
	)

	const (
		harnessID        = "claude-code"
		harnessSessionID = "cccccccc-3333-4333-8333-cccccccccccc"
		sessionRowID     = "01900000-0000-7000-8000-00000000000c"
		toolUseID        = "toolu_preview_1"
	)

	// Multi-byte on purpose: the bound is in runes, and a byte-counting
	// regression would cut the preview short of it.
	long := strings.Repeat("é", derive.PreviewRunes+200)
	imageBytes := strings.Repeat("iVBORw0KGgo", 512)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		for _, stmt := range []string{
			"TRUNCATE TABLE derive_queue",
			"TRUNCATE TABLE raw_turns RESTART IDENTITY",
			"TRUNCATE TABLE sessions CASCADE",
		} {
			_, err = driver.DB().Exec(ctx, stmt)
			Expect(err).NotTo(HaveOccurred())
		}

		_, err = driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, '00000000-0000-0000-0000-000000000000', 'test', $2, $3, NOW(), NOW())`,
			sessionRowID, harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	})

	putTurn := func(requestID string, messages, response string) {
		_, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        harnessID,
			HarnessSessionID: harnessSessionID,
			RequestID:        requestID,
			// stream + a tool set is the classifier's tell for a main
			// (conversation-spine) call, the only kind that opens tool spans.
			RawRequest: json.RawMessage(
				`{"model":"claude-test","max_tokens":4096,"stream":true,` +
					`"tools":[{"name":"Bash","description":"run","input_schema":{"type":"object"}}],` +
					`"messages":[` + messages + `]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":` + response + `},"stop_reason":"end_turn"}`),
			SessionEnvelope: json.RawMessage(fmt.Sprintf(
				`{"harness_id":%q,"harness_session_id":%q}`, harnessID, harnessSessionID)),
		})
		Expect(err).NotTo(HaveOccurred())
	}

	// seedSession stores a two-call conversation: a user prompt carrying a
	// long text block and an image, an assistant tool_use with long nested
	// arguments, and the long tool_result that answers it.
	seedSession := func() {
		userText, err := json.Marshal(long)
		Expect(err).NotTo(HaveOccurred())
		user := `{"role":"user","content":[` +
			`{"type":"text","text":` + string(userText) + `},` +
			`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + imageBytes + `"}}]}`
		toolInput := `{"command":` + string(userText) + `,"env":{"note":` + string(userText) + `}}`
		// The reduced response carries llm.ContentBlock's field names; the
		// request history echoes the same call in Anthropic's wire shape.
		assistantReduced := `[{"type":"text","text":"running it"},` +
			`{"type":"tool_use","tool_use_id":"` + toolUseID + `","tool_name":"Bash","tool_input":` + toolInput + `}]`
		assistantWire := `[{"type":"text","text":"running it"},` +
			`{"type":"tool_use","id":"` + toolUseID + `","name":"Bash","input":` + toolInput + `}]`
		toolResult := `{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + toolUseID + `","content":` + string(userText) + `}]}`

		putTurn("req-preview-1", user, assistantReduced)
		putTurn("req-preview-2",
			user+`,{"role":"assistant","content":`+assistantWire+`},`+toolResult,
			`[{"type":"text","text":"done"}]`)
	}

	type spanRow struct {
		spanID, kind  string
		input, output json.RawMessage
		inputPreview  json.RawMessage
		outputPreview json.RawMessage
		contentHash   string
		deriveSeq     int64
	}

	readSpans := func() map[string]spanRow {
		rows, err := driver.DB().Query(ctx, `
			SELECT span_id, kind, input, output, input_preview, output_preview, content_hash, derive_seq
			FROM spans_20260615 WHERE session_id = $1 ORDER BY trace_id, seq, span_id`, sessionRowID)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		out := map[string]spanRow{}
		for rows.Next() {
			var r spanRow
			Expect(rows.Scan(&r.spanID, &r.kind, &r.input, &r.output, &r.inputPreview, &r.outputPreview, &r.contentHash, &r.deriveSeq)).To(Succeed())
			out[r.spanID] = r
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return out
	}

	decode := func(raw json.RawMessage) []llm.ContentBlock {
		var blocks []llm.ContentBlock
		Expect(json.Unmarshal(raw, &blocks)).To(Succeed())
		return blocks
	}

	// findSpan returns the first span whose payload satisfies pred.
	findSpan := func(spans map[string]spanRow, pred func(spanRow) bool) spanRow {
		for _, r := range spans {
			if pred(r) {
				return r
			}
		}
		Fail("no span matched")
		return spanRow{}
	}

	It("writes previews on upsert", func() {
		seedSession()
		_, err := driver.RederiveSession(ctx, "", "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())

		spans := readSpans()
		Expect(spans).NotTo(BeEmpty())
		for id, r := range spans {
			Expect(r.inputPreview).NotTo(BeNil(), "span %s must carry an input preview", id)
			Expect(r.outputPreview).NotTo(BeNil(), "span %s must carry an output preview", id)
			// Compared as decoded blocks: JSONB canonicalizes key order and
			// whitespace on the way in, so the stored bytes are not the
			// function's bytes even though the value is.
			Expect(decode(r.inputPreview)).To(Equal(decode(derive.PreviewBlocks(r.input))),
				"span %s input preview must be the pure projection of its input", id)
			Expect(decode(r.outputPreview)).To(Equal(decode(derive.PreviewBlocks(r.output))),
				"span %s output preview must be the pure projection of its output", id)
		}

		// The llm call whose fresh input carried the long prompt and the image.
		llmSpan := findSpan(spans, func(r spanRow) bool {
			return r.kind == "llm" && strings.Contains(string(r.input), "image_base64")
		})
		Expect(string(llmSpan.input)).To(ContainSubstring(imageBytes), "the payload keeps the image bytes")
		Expect(string(llmSpan.inputPreview)).NotTo(ContainSubstring("image_base64"))
		Expect(string(llmSpan.inputPreview)).NotTo(ContainSubstring(imageBytes[:64]))
		Expect(len(llmSpan.inputPreview)).To(BeNumerically("<", len(llmSpan.input)))

		inBlocks := decode(llmSpan.inputPreview)
		var sawText, sawImage bool
		for _, b := range inBlocks {
			switch b.Type {
			case "text":
				sawText = true
				Expect(utf8.RuneCountInString(b.Text)).To(Equal(derive.PreviewRunes + 1))
				Expect(b.Text).To(HaveSuffix("…"))
			case "image":
				sawImage = true
				Expect(b.ImageBase64).To(BeEmpty())
				Expect(b.MediaType).To(Equal("image/png"))
			}
		}
		Expect(sawText).To(BeTrue())
		Expect(sawImage).To(BeTrue())

		// The tool span: nested tool_use arguments in, the long result out.
		toolSpan := findSpan(spans, func(r spanRow) bool { return r.kind == "tool" })
		toolIn := decode(toolSpan.inputPreview)
		Expect(toolIn).To(HaveLen(1))
		Expect(toolIn[0].Type).To(Equal("tool_use"))
		Expect(utf8.RuneCountInString(toolIn[0].ToolInput["command"].(string))).To(Equal(derive.PreviewRunes + 1))
		note := toolIn[0].ToolInput["env"].(map[string]any)["note"].(string)
		Expect(utf8.RuneCountInString(note)).To(Equal(derive.PreviewRunes + 1))
		Expect(note).To(HaveSuffix("…"))

		toolOut := decode(toolSpan.outputPreview)
		Expect(toolOut).To(HaveLen(1))
		Expect(toolOut[0].Type).To(Equal("tool_result"))
		Expect(utf8.RuneCountInString(toolOut[0].ToolOutput)).To(Equal(derive.PreviewRunes + 1))
		Expect(utf8.RuneCountInString(decode(toolSpan.output)[0].ToolOutput)).To(Equal(derive.PreviewRunes+200),
			"the payload itself is untouched")

		// The record readers surface the stored columns as-is.
		_, recs, _, err := driver.ListSessionSpanModel(ctx, sessionRowID)
		Expect(err).NotTo(HaveOccurred())
		Expect(recs).To(HaveLen(len(spans)))
		for _, rec := range recs {
			Expect(rec.HasPreview).To(BeTrue())
			Expect(string(rec.InputPreview)).To(Equal(string(spans[rec.SpanID].inputPreview)))
			Expect(string(rec.OutputPreview)).To(Equal(string(spans[rec.SpanID].outputPreview)))
		}
		for rec, err := range driver.IterateSessionSpans(ctx, sessionRowID, storage.SpanCursor{}) {
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.HasPreview).To(BeTrue())
			Expect(string(rec.InputPreview)).To(Equal(string(spans[rec.SpanID].inputPreview)))
		}
	})

	It("leaves derive_seq untouched by previews", func() {
		seedSession()
		_, err := driver.RederiveSession(ctx, "", "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		first := readSpans()
		Expect(first).NotTo(BeEmpty())

		// A second pass over unchanged raw rewrites every row, previews
		// included. Nothing changed, so no cursor may move.
		_, err = driver.RederiveSession(ctx, "", "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		second := readSpans()
		Expect(second).To(HaveLen(len(first)))
		for id, a := range first {
			b, ok := second[id]
			Expect(ok).To(BeTrue(), "span %s must survive the re-derive", id)
			Expect(b.contentHash).To(Equal(a.contentHash))
			Expect(b.deriveSeq).To(Equal(a.deriveSeq), "span %s: cursor must not advance on an idempotent rewrite", id)
			Expect(string(b.inputPreview)).To(Equal(string(a.inputPreview)))
		}

		// And the previews are not hashed: the same payload with and without
		// them digests identically, so a backfill that fills a NULL preview
		// cannot move a cursor either.
		params := gensqlc.UpsertSpanParams{
			Kind: "llm", Name: "call", Status: "ok",
			StartedAt: pgtype.Timestamptz{Valid: true},
			Input:     []byte(`[{"type":"text","text":"hello"}]`),
			Output:    []byte(`[{"type":"text","text":"world"}]`),
			Fidelity:  postgres.FidelityRaw,
		}
		bare := postgres.SpanContentHashForTest(params)
		params.InputPreview = derive.PreviewBlocks(params.Input)
		params.OutputPreview = derive.PreviewBlocks(params.Output)
		Expect(postgres.SpanContentHashForTest(params)).To(Equal(bare))
	})

	It("reads a row derived before previews existed as pending", func() {
		seedSession()
		_, err := driver.RederiveSession(ctx, "", "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())

		// The shape the backfill has not reached yet.
		_, err = driver.DB().Exec(ctx,
			`UPDATE spans_20260615 SET input_preview = NULL, output_preview = NULL WHERE session_id = $1`, sessionRowID)
		Expect(err).NotTo(HaveOccurred())

		_, recs, _, err := driver.ListSessionSpanModel(ctx, sessionRowID)
		Expect(err).NotTo(HaveOccurred())
		Expect(recs).NotTo(BeEmpty())
		for _, rec := range recs {
			Expect(rec.HasPreview).To(BeFalse())
			Expect(rec.InputPreview).To(BeNil())
			Expect(rec.OutputPreview).To(BeNil())
		}
	})
})
