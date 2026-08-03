package extproc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// anthropicTurnSSE is a minimal but complete Anthropic Messages stream: it
// starts, emits one text block, and terminates with message_stop, so the
// shared reducer produces content, a stop reason, and done=true — the shape
// ingest's validateReducedResponse accepts.
const anthropicTurnSSE = `event: message_start
data: {"type":"message_start","message":{"id":"msg_raw_lane","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"usage":{"input_tokens":11,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello from the raw lane"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}

event: message_stop
data: {"type":"message_stop"}

`

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	Expect(err).NotTo(HaveOccurred())
	Expect(zw.Close()).To(Succeed())
	return buf.Bytes()
}

// oversizeAnthropicSSE builds a well-formed Anthropic stream of at least n
// bytes by padding a single text delta. It reduces normally — the point is
// the size of the wire body, not a malformed frame.
func oversizeAnthropicSSE(n int) []byte {
	var buf bytes.Buffer
	buf.WriteString("event: message_start\n")
	buf.WriteString(`data: {"type":"message_start","message":{"id":"msg_big","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`)
	buf.WriteString("\n\nevent: content_block_start\n")
	buf.WriteString(`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	buf.WriteString("\n\nevent: content_block_delta\n")
	buf.WriteString(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"`)
	buf.Write(bytes.Repeat([]byte("a"), n))
	buf.WriteString("\"}}\n\nevent: content_block_stop\n")
	buf.WriteString(`data: {"type":"content_block_stop","index":0}`)
	buf.WriteString("\n\nevent: message_delta\n")
	buf.WriteString(`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`)
	buf.WriteString("\n\nevent: message_stop\n")
	buf.WriteString(`data: {"type":"message_stop"}`)
	buf.WriteString("\n\n")
	return buf.Bytes()
}

// manyDeltaAnthropicSSE builds a well-formed Anthropic stream carrying total
// bytes of text split across deltas of at most chunk bytes each, so the
// reducer's line scanner accumulates all of it into the reduction — unlike
// oversizeAnthropicSSE's single giant delta, which overflows the scanner and
// reduces to a stub.
func manyDeltaAnthropicSSE(total, chunk int) []byte {
	var buf bytes.Buffer
	buf.WriteString("event: message_start\n")
	buf.WriteString(`data: {"type":"message_start","message":{"id":"msg_many","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`)
	buf.WriteString("\n\nevent: content_block_start\n")
	buf.WriteString(`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	buf.WriteString("\n\n")
	for remaining := total; remaining > 0; remaining -= chunk {
		n := min(chunk, remaining)
		buf.WriteString("event: content_block_delta\n")
		buf.WriteString(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"`)
		buf.Write(bytes.Repeat([]byte("a"), n))
		buf.WriteString("\"}}\n\n")
	}
	buf.WriteString("event: content_block_stop\n")
	buf.WriteString(`data: {"type":"content_block_stop","index":0}`)
	buf.WriteString("\n\nevent: message_delta\n")
	buf.WriteString(`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`)
	buf.WriteString("\n\nevent: message_stop\n")
	buf.WriteString(`data: {"type":"message_stop"}`)
	buf.WriteString("\n\n")
	return buf.Bytes()
}

func zstdBytes(b []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	Expect(err).NotTo(HaveOccurred())
	defer enc.Close()
	return enc.EncodeAll(b, nil)
}

// reducedStub is a minimal reduction that reads as "present" to ingest's
// reducedResponseAbsent check.
func reducedStub() *llm.ChatResponse {
	return &llm.ChatResponse{
		Model:      "claude-sonnet-4-6",
		Done:       true,
		StopReason: "end_turn",
		Message: llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "ok"}},
		},
	}
}

// envelopeRecorder is a stand-in tapes-ingest that keeps the exact bytes it
// was POSTed. Asserting on the wire body rather than on a Go struct is the
// point: the whole change is a wire-shape change, and a struct-level
// assertion would pass even if the JSON tags were wrong.
type envelopeRecorder struct {
	srv *httptest.Server

	mu     sync.Mutex
	bodies [][]byte
}

func newEnvelopeRecorder() *envelopeRecorder {
	r := &envelopeRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		r.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	return r
}

func (r *envelopeRecorder) Close() { r.srv.Close() }

func (r *envelopeRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *envelopeRecorder) last() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bodies[len(r.bodies)-1]
}

// turnInput is one synthetic upstream exchange to push through the processor.
type turnInput struct {
	reqBody         []byte
	respBody        []byte
	contentType     string
	contentEncoding string
}

func anthropicTurn() turnInput {
	return turnInput{
		reqBody:     []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"stream":true}`),
		respBody:    []byte(anthropicTurnSSE),
		contentType: "text/event-stream",
	}
}

// runTurn drives one turn through the real processor state machine in the
// given mode and returns the envelope tapes-ingest received, decoded as a
// generic map so absent keys are distinguishable from zero values.
func runTurn(mode RawResponseMode, in turnInput) (map[string]json.RawMessage, *Processor) {
	GinkgoHelper()

	rec := newEnvelopeRecorder()
	DeferCleanup(rec.Close)

	proc, err := NewProcessor(Config{
		IngestURL:       rec.srv.URL,
		MaxInflight:     4,
		RawResponseMode: mode,
	})
	Expect(err).NotTo(HaveOccurred())
	proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())

	stream := &fakeStream{
		ctx: context.Background(),
		toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{
				":method":      http.MethodPost,
				":path":        "/v1/messages",
				"x-request-id": "raw-lane-test",
			}),
			reqBodyReq(in.reqBody, true),
			respHeaderReqWithEncoding("200", in.contentType, in.contentEncoding),
			respBodyReq(in.respBody, true),
		},
	}
	Expect(proc.Process(stream)).To(Succeed())

	Eventually(rec.count).Should(Equal(1))

	var envelope map[string]json.RawMessage
	Expect(json.Unmarshal(rec.last(), &envelope)).To(Succeed())
	return envelope, proc
}

var _ = Describe("Raw response lane", func() {
	Describe("mode parsing", func() {
		It("defaults to off for an unset value", func() {
			mode, err := ParseRawResponseMode("")
			Expect(err).NotTo(HaveOccurred())
			Expect(mode).To(Equal(RawResponseOff))
		})

		It("accepts the three modes case-insensitively", func() {
			for in, want := range map[string]RawResponseMode{
				"off":  RawResponseOff,
				"DUAL": RawResponseDual,
				" raw": RawResponseRaw,
			} {
				mode, err := ParseRawResponseMode(in)
				Expect(err).NotTo(HaveOccurred(), "input %q", in)
				Expect(mode).To(Equal(want), "input %q", in)
			}
		})

		It("rejects an unknown mode rather than silently disabling the lane", func() {
			mode, err := ParseRawResponseMode("both")
			Expect(err).To(MatchError(ContainSubstring("unknown raw response mode")))
			Expect(mode).To(Equal(RawResponseOff))
		})
	})

	// The two size limits are ingest's own constants now, so there is
	// nothing left here to pin: a change to either is a change to the
	// single declaration both sides read. What still needs stating is the
	// raw-only interlock's admission set, which is extproc's decision and
	// deliberately narrower than what ingest can decode.
	Describe("the raw-only interlock's admission set", func() {
		It("admits single-layer identity and gzip, however they are spelled", func() {
			for _, ce := range []string{"", "identity", "gzip", "x-gzip", "GZIP", " gzip "} {
				Expect(ingestCanDecodeEncoding(ce)).To(BeTrue(), "encoding %q", ce)
			}
		})

		It("withholds raw-only for encodings the interlock has not been widened to", func() {
			// ingest decodes zstd and stacked layers since it moved onto
			// capture.DecodeContentEncoding. The interlock has not been
			// widened to match, so these still fall back to dual-send.
			// Widening it changes what goes on the wire and is its own change.
			for _, ce := range []string{"zstd", "br", "gzip, gzip"} {
				Expect(ingestCanDecodeEncoding(ce)).To(BeFalse(), "encoding %q", ce)
			}
		})
	})

	Describe("the attach decision", func() {
		It("attaches nothing when the mode is off", func() {
			d := decideRawLane(RawResponseOff, 1024, 512, "", false)
			Expect(d.attachRaw).To(BeFalse())
			Expect(d.omitReduction).To(BeFalse())
			Expect(d.skipReason).To(BeEmpty())
		})

		It("attaches bytes and keeps the reduction in dual mode", func() {
			d := decideRawLane(RawResponseDual, 1024, 512, "gzip", false)
			Expect(d.attachRaw).To(BeTrue())
			Expect(d.omitReduction).To(BeFalse())
		})

		It("drops the reduction in raw mode", func() {
			d := decideRawLane(RawResponseRaw, 1024, 512, "gzip", false)
			Expect(d.attachRaw).To(BeTrue())
			Expect(d.omitReduction).To(BeTrue())
			Expect(d.fallbackReason).To(BeEmpty())
		})

		It("keeps the reduction when ingest could not decode the bytes", func() {
			d := decideRawLane(RawResponseRaw, 1024, 512, "zstd", false)
			Expect(d.attachRaw).To(BeTrue())
			Expect(d.omitReduction).To(BeFalse())
			Expect(d.fallbackReason).To(Equal(rawFallbackEncoding))
		})

		It("keeps the reduction when the content was salvaged from a truncated body", func() {
			// extproc recovers content from a truncated gzip stream;
			// ingest's plain gzip.Reader + io.ReadAll would error, so
			// raw-only would lose the turn's content entirely.
			d := decideRawLane(RawResponseRaw, 1024, 512, "gzip", true)
			Expect(d.attachRaw).To(BeTrue())
			Expect(d.omitReduction).To(BeFalse())
			Expect(d.fallbackReason).To(Equal(rawFallbackSalvaged))
		})

		Context("around the caps", func() {
			It("still attaches bytes over the 8 MiB storage cap so ingest can mark the row degraded", func() {
				// This is the deliberate one. Pre-dropping here would
				// make raw_response_dropped unreachable and the row
				// indistinguishable from a producer that never captured.
				d := decideRawLane(RawResponseDual, ingest.MaxRawResponseBytes+1, 1024, "", false)
				Expect(d.attachRaw).To(BeTrue())
				Expect(d.skipReason).To(BeEmpty())
			})

			It("withholds bytes that would not survive the transport", func() {
				d := decideRawLane(RawResponseDual, ingest.MaxIngestBodyBytes, 1024, "", false)
				Expect(d.attachRaw).To(BeFalse())
				Expect(d.skipReason).To(Equal(rawSkipTransportBudget))
			})

			It("never goes raw-only when the bytes were withheld", func() {
				// Raw-only without bytes is a turn with no response at
				// all — strictly worse than the historical shape.
				d := decideRawLane(RawResponseRaw, ingest.MaxIngestBodyBytes, 1024, "", false)
				Expect(d.attachRaw).To(BeFalse())
				Expect(d.omitReduction).To(BeFalse())
			})

			It("accounts for the request when sizing the response", func() {
				// A large request eats the same body budget, so a raw
				// response that fits alone may not fit alongside it.
				rawLen := 6 << 20
				Expect(rawResponseFits(rawLen, 1024)).To(BeTrue())
				Expect(rawResponseFits(rawLen, ingest.MaxIngestBodyBytes)).To(BeFalse())
			})
		})
	})

	Describe("the dispatched envelope", func() {
		It("carries no raw_response in the default mode", func() {
			envelope, _ := runTurn(RawResponseOff, anthropicTurn())

			Expect(envelope).NotTo(HaveKey("raw_response"))
			Expect(envelope).NotTo(HaveKey("raw_response_encoding"))
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
		})

		It("carries both halves in dual mode", func() {
			in := anthropicTurn()
			envelope, _ := runTurn(RawResponseDual, in)

			var raw []byte
			Expect(json.Unmarshal(envelope["raw_response"], &raw)).To(Succeed())
			// Verbatim: byte-for-byte what the upstream sent.
			Expect(raw).To(Equal(in.respBody))
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
		})

		It("sends the bytes still compressed, with the encoding that describes them", func() {
			in := anthropicTurn()
			in.respBody = gzipBytes([]byte(anthropicTurnSSE))
			in.contentEncoding = "gzip"

			envelope, _ := runTurn(RawResponseDual, in)

			var raw []byte
			Expect(json.Unmarshal(envelope["raw_response"], &raw)).To(Succeed())
			// Not the decoded bytes: tapes stores the column
			// byte-faithfully and decodes only to reduce.
			Expect(raw).To(Equal(in.respBody))
			Expect(raw).NotTo(Equal([]byte(anthropicTurnSSE)))

			var encoding string
			Expect(json.Unmarshal(envelope["raw_response_encoding"], &encoding)).To(Succeed())
			Expect(encoding).To(Equal("gzip"))
		})

		It("omits the reduction in raw mode", func() {
			in := anthropicTurn()
			envelope, _ := runTurn(RawResponseRaw, in)

			var raw []byte
			Expect(json.Unmarshal(envelope["raw_response"], &raw)).To(Succeed())
			Expect(raw).To(Equal(in.respBody))

			// null unmarshals into ingest's non-pointer
			// llm.ChatResponse as a no-op, leaving the zero value that
			// reducedResponseAbsent recognizes as "reduce this yourself".
			Expect(envelope["response"]).To(MatchJSON("null"))
		})

		It("never sends raw_response_dropped — ingest computes that itself", func() {
			envelope, _ := runTurn(RawResponseDual, anthropicTurn())
			Expect(envelope).NotTo(HaveKey("raw_response_dropped"))
		})

		It("falls back to dual when ingest could not decode the encoding", func() {
			in := anthropicTurn()
			in.respBody = zstdBytes([]byte(anthropicTurnSSE))
			in.contentEncoding = "zstd"

			envelope, proc := runTurn(RawResponseRaw, in)

			Expect(envelope).To(HaveKey("raw_response"))
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
			Expect(scrapeProcessorMetrics(proc)).To(ContainSubstring(
				`tapes_extproc_raw_response_fallback_total{provider="anthropic",reason="encoding_not_decodable"} 1`))
		})

		It("meters the shape it dispatched", func() {
			_, proc := runTurn(RawResponseRaw, anthropicTurn())
			Expect(scrapeProcessorMetrics(proc)).To(ContainSubstring(
				`tapes_extproc_raw_response_attached_total{provider="anthropic",shape="raw_only"} 1`))
		})
	})

	// The degraded tier only exists if over-cap bytes actually reach
	// ingest. A producer that pre-dropped them would leave
	// raw_response_dropped permanently false and fidelity:degraded
	// permanently unreachable — the marker would be dead code and the row
	// would look identical to one from a producer that never captured.
	Describe("the drop-and-mark path", func() {
		It("puts over-cap bytes on the wire so ingest can mark the row degraded", func() {
			in := anthropicTurn()
			in.respBody = oversizeAnthropicSSE(ingest.MaxRawResponseBytes + (1 << 20))

			envelope, _ := runTurn(RawResponseDual, in)

			var raw []byte
			Expect(json.Unmarshal(envelope["raw_response"], &raw)).To(Succeed())

			// Over the cap ingest stores at, so ingest will drop the
			// bytes and set raw_response_dropped — fidelity:degraded.
			Expect(len(raw)).To(BeNumerically(">", ingest.MaxRawResponseBytes))
			Expect(raw).To(Equal(in.respBody))

			// …and still small enough to be accepted by the transport,
			// which is what makes the drop a marked drop rather than a
			// 413 that loses the whole turn.
			body, err := json.Marshal(envelope)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(body)).To(BeNumerically("<=", ingest.MaxIngestBodyBytes))

			// The reduction rides along, so the turn is still readable
			// even though its verbatim bytes will not be stored.
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
		})

		It("withholds bytes too large for the transport, keeping the turn", func() {
			in := anthropicTurn()
			in.respBody = oversizeAnthropicSSE(ingest.MaxIngestBodyBytes + (1 << 20))

			envelope, proc := runTurn(RawResponseDual, in)

			// No bytes — but the turn still lands, with its reduction.
			Expect(envelope).NotTo(HaveKey("raw_response"))
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
			Expect(scrapeProcessorMetrics(proc)).To(ContainSubstring(
				`tapes_extproc_raw_response_skipped_total{provider="anthropic",reason="transport_budget"} 1`))
		})
	})

	Describe("the transport backstop", func() {
		It("counts a stripped turn as skipped, never as attached", func() {
			in := anthropicTurn()
			// Sized to pass the pre-dispatch estimate — base64 of ~8.4 MiB
			// of SSE fits the reserve-adjusted budget — while the marshalled
			// envelope exceeds the transport limit, because the reduction
			// carries the same ~8 MiB of text the verbatim bytes do. The
			// deltas must stay small (64 KiB) so the reducer's line scanner
			// actually accumulates the text instead of erroring out with a
			// stub reduction. The post-marshal backstop then strips, and the
			// attach counter must reflect the shape the turn actually
			// shipped with: one skip, zero attaches, not
			// attached-then-skipped.
			in.respBody = manyDeltaAnthropicSSE(8<<20, 64<<10)

			envelope, proc := runTurn(RawResponseDual, in)

			Expect(envelope).NotTo(HaveKey("raw_response"))
			Expect(envelope["response"]).NotTo(MatchJSON("null"))
			metrics := scrapeProcessorMetrics(proc)
			Expect(metrics).To(ContainSubstring(
				`tapes_extproc_raw_response_skipped_total{provider="anthropic",reason="oversize_stripped"} 1`))
			Expect(metrics).NotTo(ContainSubstring("tapes_extproc_raw_response_attached_total"))
		})

		// rawResponseFits runs on an estimate, before the envelope
		// exists. This is the exact check on the marshalled bytes.
		It("strips the bytes rather than letting the turn 413", func() {
			d := NewDispatcher("http://example.invalid", 1, nil)

			env := TurnEnvelope{
				Provider:            "anthropic",
				Request:             json.RawMessage(`{}`),
				Response:            reducedStub(),
				RawResponse:         bytes.Repeat([]byte("x"), ingest.MaxIngestBodyBytes),
				RawResponseEncoding: "identity",
			}
			payload, err := json.Marshal(env)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(payload)).To(BeNumerically(">", ingest.MaxIngestBodyBytes))

			out, stripped := d.enforceBodyLimit(env, payload)
			Expect(len(out)).To(BeNumerically("<=", ingest.MaxIngestBodyBytes))
			Expect(stripped.RawResponse).To(BeNil())
			Expect(stripped.RawResponseEncoding).To(BeEmpty())
			// The reduction survives: the turn still lands.
			Expect(stripped.Response).NotTo(BeNil())
		})

		It("restores the reduction when it strips a raw-only envelope", func() {
			d := NewDispatcher("http://example.invalid", 1, nil)

			reduced := reducedStub()
			env := TurnEnvelope{
				Provider:        "anthropic",
				Request:         json.RawMessage(`{}`),
				Response:        nil,
				RawResponse:     bytes.Repeat([]byte("x"), ingest.MaxIngestBodyBytes),
				reducedFallback: reduced,
			}
			payload, err := json.Marshal(env)
			Expect(err).NotTo(HaveOccurred())

			_, stripped := d.enforceBodyLimit(env, payload)
			Expect(stripped.RawResponse).To(BeNil())
			// Without this the turn would carry no response at all and
			// ingest would reject it.
			Expect(stripped.Response).To(Equal(reduced))
		})

		It("leaves an envelope under the limit untouched", func() {
			d := NewDispatcher("http://example.invalid", 1, nil)

			env := TurnEnvelope{
				Provider:    "anthropic",
				Request:     json.RawMessage(`{}`),
				Response:    reducedStub(),
				RawResponse: []byte("small"),
			}
			payload, err := json.Marshal(env)
			Expect(err).NotTo(HaveOccurred())

			out, same := d.enforceBodyLimit(env, payload)
			Expect(out).To(Equal(payload))
			Expect(same.RawResponse).To(Equal([]byte("small")))
		})
	})
})
