package extproc

import (
	"bytes"
	"compress/gzip"
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/capture/fixtures"
)

// goodAnthropicSSE is a well-formed Anthropic Messages stream drawn from the
// shared capture fixtures package, so the sidecar exercises the exact bytes
// tapes' own reducer tests do — no hand-copied inline mirror to drift.
var goodAnthropicSSE = mustAnthropicFixture("messages_stream.sse")

func mustAnthropicFixture(name string) []byte {
	b, err := fixtures.ReadFile("anthropic/" + name)
	if err != nil {
		panic(err)
	}
	return b
}

// Adversarial probes for the upstream shapes that can drive the
// deployed Anthropic reducer (capture.NewAnthropicReducer()) into an
// empty-Content response. Each scenario feeds the reducer a shape that
// the diagnostic helpers should recognize as empty, and asserts which
// reason they emit. The probes serve two ends: locking in the
// classification of each known failure mode, and making the
// diagnostic's byte-preview output verifiable against a fixture.
var _ = Describe("Reducer adversarial probes", func() {
	ctx := context.Background()
	r := capture.NewAnthropicReducer()

	It("baseline: well-formed SSE produces non-empty Content", func() {
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader(goodAnthropicSSE), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		_, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeFalse(), "the happy path must not look like the failure we're chasing")
		Expect(resp.Message.Content).To(HaveLen(1))
		Expect(resp.Message.Content[0].Text).To(ContainSubstring("Hello"))
	})

	It("hypothesis A — empty response body (Envoy emits EOS with no bytes)", func() {
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader(nil), "text/event-stream")
		Expect(err).NotTo(HaveOccurred(), "the deployed reducer absorbs empty bodies silently — there is no error to short-circuit on, so the empty-Content path is the only signal")
		reason, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("empty_content"))
	})

	It("hypothesis B — gzip-compressed SSE (Envoy forwarded compressed bytes)", func() {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, err := zw.Write(goodAnthropicSSE)
		Expect(err).NotTo(HaveOccurred())
		Expect(zw.Close()).To(Succeed())

		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader(buf.Bytes()), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		reason, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeTrue(), "the SSE parser can't see events through gzip framing")
		Expect(reason).To(Equal("empty_content"))

		// resp_preview should make gzip immediately obvious in logs.
		Expect(respBodyPreview(buf.Bytes(), 32)).To(HavePrefix(`\x1f\x8b`))
	})

	It("hypothesis C — JSON error envelope at application/json (Anthropic 5xx)", func() {
		body := []byte(`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader(body), "application/json")
		// reduceOneShot path: json.Unmarshal succeeds against the looser
		// oneshotResponse struct (it just doesn't find any content blocks),
		// so the reducer returns a zero-Content response without erroring.
		Expect(err).NotTo(HaveOccurred())
		reason, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("empty_content"))
		// respBodyPreview quote-escapes for log-safety, so the recognizable
		// "type":"error" payload reads back as \"type\":\"error\" — still
		// trivially grep-able in production logs and unambiguously distinct
		// from a real SSE preview ("event: message_start...").
		Expect(respBodyPreview(body, 80)).To(ContainSubstring(`\"type\":\"error\"`))
	})

	It("hypothesis D — SSE body mis-tagged as application/json (content-type wrong)", func() {
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader(goodAnthropicSSE), "application/json")
		// reduceOneShot's json.Unmarshal of raw SSE bytes fails outright —
		// that's a DropReducerError in the dispatcher, distinct from the
		// empty-Content surface. The assertion pins the classification
		// so a future reducer change cannot silently merge the two
		// paths.
		Expect(err).To(HaveOccurred(), "JSON-parse failure on SSE body short-circuits before dispatch — drops with reducer_error, not empty-Content")
		_ = resp
	})

	It("hypothesis E — message_start only, then EOF (Anthropic drops the stream after headers)", func() {
		body := `event: message_start
data: {"type":"message_start","message":{"id":"msg_X","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

`
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader([]byte(body)), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		reason, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("empty_content"))
		Expect(resp.StopReason).To(Equal("incomplete"))
	})

	It("hypothesis F — Envoy AI Gateway transforms SSE into a non-Anthropic shape (e.g. OpenAI-compat)", func() {
		// Envoy AI Gateway can rewrite responses between provider formats.
		// If for some reason it's sending an OpenAI Chat Completions chunk
		// stream as text/event-stream, the Anthropic reducer would walk
		// events but find no Anthropic-shaped content_block_* frames.
		openaiChunk := `data: {"id":"chatcmpl-X","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hello"}}]}

data: [DONE]

`
		resp, err := r.Reduce(ctx, bytes.NewReader([]byte("{}")), bytes.NewReader([]byte(openaiChunk)), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		reason, empty := reducerEmptyReason(resp)
		Expect(empty).To(BeTrue(), "Anthropic reducer can't extract content from OpenAI-shape chunks")
		Expect(reason).To(Equal("empty_content"))
	})
})
