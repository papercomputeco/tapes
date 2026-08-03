package ingest_test

// The wire lane's byte-faithful response column and the server-side reduction
// that makes a raw-only payload land the same row a pre-reduced one would.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// anthropicSSE is one complete Anthropic Messages stream: the bytes an
// upstream actually puts on the wire for a short turn.
const anthropicSSE = "event: message_start\n" +
	`data: {"type":"message_start","message":{"id":"msg_FIXTURE","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":7,"output_tokens":1}}}` + "\n\n" +
	"event: content_block_start\n" +
	`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}` + "\n\n" +
	"event: content_block_stop\n" +
	`data: {"type":"content_block_stop","index":0}` + "\n\n" +
	"event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}` + "\n\n" +
	"event: message_stop\n" +
	`data: {"type":"message_stop"}` + "\n\n"

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	Expect(err).NotTo(HaveOccurred())
	Expect(zw.Close()).To(Succeed())
	return buf.Bytes()
}

func zstdBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	Expect(err).NotTo(HaveOccurred())
	_, err = zw.Write(b)
	Expect(err).NotTo(HaveOccurred())
	Expect(zw.Close()).To(Succeed())
	return buf.Bytes()
}

// stampCount sums the tapes_ingest_rawonly_stamp_total series matching the
// given field and source. Either may be "" to mean "any", so passing both
// empty totals the whole metric — the way to assert a path stamped nothing
// at all.
func stampCount(server *ingest.Server, field, source string) float64 {
	families, err := server.Metrics().Registry().Gather()
	Expect(err).NotTo(HaveOccurred())

	var total float64
	for _, family := range families {
		if family.GetName() != "tapes_ingest_rawonly_stamp_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			var gotField, gotSource string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "field":
					gotField = label.GetValue()
				case "source":
					gotSource = label.GetValue()
				}
			}
			if field != "" && gotField != field {
				continue
			}
			if source != "" && gotSource != source {
				continue
			}
			total += metric.GetCounter().GetValue()
		}
	}
	return total
}

var _ = Describe("Raw response capture", func() {
	var (
		server  *ingest.Server
		driver  *captureDriver
		baseURL string
		client  *http.Client
	)

	BeforeEach(func() {
		server, driver, baseURL = newTestServer()
		client = &http.Client{Timeout: 5 * time.Second}
	})

	AfterEach(func() {
		if server != nil {
			Expect(server.Close()).To(Succeed())
		}
	})

	// post sends a turn and waits for the raw row to land.
	post := func(payload ingest.TurnPayload) {
		body, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())
		resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)

		Eventually(driver.CountRaw).
			WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
			Should(Equal(1))
	}

	anthropicRequest := mustJSON(map[string]any{
		"model":      "claude-3-5-sonnet-20241022",
		"max_tokens": 64,
		"stream":     true,
		"messages":   []map[string]string{{"role": "user", "content": "hi"}},
	})

	It("stores the verbatim response bytes and their encoding", func() {
		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			Response:            reducedResponse("claude-3-5-sonnet-20241022", "Hello world", nil),
			RawResponse:         []byte(anthropicSSE),
			RawResponseEncoding: "identity",
		})

		rec := driver.RawTurns()[0]
		// Byte-for-byte: the column's whole purpose is that a future deriver
		// can re-reduce from exactly what the upstream sent.
		Expect(string(rec.RawResponse)).To(Equal(anthropicSSE))
		Expect(rec.RawResponseEncoding).To(Equal("identity"))
		Expect(rec.RawResponseDropped).To(BeFalse())
	})

	It("leaves the marker clear when no raw response was sent", func() {
		post(ingest.TurnPayload{
			Provider:   "anthropic",
			RawRequest: anthropicRequest,
			Response:   reducedResponse("claude-3-5-sonnet-20241022", "Hello world", nil),
		})

		rec := driver.RawTurns()[0]
		Expect(rec.RawResponse).To(BeEmpty())
		// Absent, not dropped — the distinction the marker exists to preserve.
		Expect(rec.RawResponseDropped).To(BeFalse())
	})

	It("drops and marks a raw response over the cap, keeping the rest of the turn", func() {
		oversize := bytes.Repeat([]byte("x"), ingest.MaxRawResponseBytes+1)

		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			Response:            reducedResponse("claude-3-5-sonnet-20241022", "Hello world", nil),
			RawResponse:         oversize,
			RawResponseEncoding: "identity",
		})

		rec := driver.RawTurns()[0]
		Expect(rec.RawResponse).To(BeEmpty())
		Expect(rec.RawResponseDropped).To(BeTrue())
		// The turn itself is not lost — only its verbatim bytes.
		Expect(rec.Provider).To(Equal("anthropic"))
		Expect(rec.Response).NotTo(BeEmpty())
	})

	It("marks a turn whose producer withheld the bytes", func() {
		// The row looks identical on the wire to one from a producer that
		// never captured raw bytes — same absent raw_response. The marker
		// is the only thing separating "a limit took them" from "there
		// were none", which are opposite operational facts.
		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			Response:            reducedResponse("claude-3-5-sonnet-20241022", "Hello world", nil),
			RawResponseWithheld: true,
		})

		rec := driver.RawTurns()[0]
		Expect(rec.RawResponse).To(BeEmpty())
		Expect(rec.RawResponseDropped).To(BeTrue())
		// The turn is otherwise whole; only its verbatim bytes are gone.
		Expect(rec.Response).NotTo(BeEmpty())
	})

	It("keeps the bytes when a producer both sends and claims to withhold them", func() {
		// A contradicted claim loses to the evidence. Trusting it would
		// discard verbatim capture on the strength of a flag the payload
		// itself refutes, and would leave a marked row still carrying a
		// raw_response — the one shape the marker must never produce.
		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         []byte(anthropicSSE),
			RawResponseEncoding: "identity",
			RawResponseWithheld: true,
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		Expect(string(rec.RawResponse)).To(Equal(anthropicSSE))
		Expect(rec.RawResponseDropped).To(BeFalse())
	})

	It("reduces a raw-only payload server-side", func() {
		post(ingest.TurnPayload{
			Provider:    "anthropic",
			RawRequest:  anthropicRequest,
			RawResponse: []byte(anthropicSSE),
			Meta:        ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		Expect(rec.RawResponse).NotTo(BeEmpty())

		// The adapter sent no reduction, so ingest performed it: the stored
		// reduced response must be the one the shared reducer produces.
		var reduced llm.ChatResponse
		Expect(json.Unmarshal(rec.Response, &reduced)).To(Succeed())
		Expect(reduced.Message.GetText()).To(Equal("Hello world"))
		Expect(reduced.Model).To(Equal("claude-3-5-sonnet-20241022"))
	})

	It("reduces a gzip-encoded raw-only payload while storing the compressed bytes", func() {
		compressed := gzipBytes([]byte(anthropicSSE))

		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         compressed,
			RawResponseEncoding: "gzip",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		// Stored as received. Decompressing before storage would make
		// "verbatim" a claim the column could not back up, since
		// re-compression is not guaranteed byte-identical.
		Expect(rec.RawResponse).To(Equal(compressed))
		Expect(rec.RawResponseEncoding).To(Equal("gzip"))

		var reduced llm.ChatResponse
		Expect(json.Unmarshal(rec.Response, &reduced)).To(Succeed())
		Expect(reduced.Message.GetText()).To(Equal("Hello world"))
	})

	It("keeps the adapter's own reduction when a payload carries both", func() {
		post(ingest.TurnPayload{
			Provider:   "anthropic",
			RawRequest: anthropicRequest,
			// Deliberately different from what the SSE reduces to. An adapter
			// consumed the live stream and may have seen framing the stored
			// bytes no longer show, so re-reducing here could only lose
			// information.
			Response:            reducedResponse("claude-3-5-sonnet-20241022", "adapter reduction", nil),
			RawResponse:         []byte(anthropicSSE),
			RawResponseEncoding: "identity",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		var reduced llm.ChatResponse
		Expect(json.Unmarshal(rec.Response, &reduced)).To(Succeed())
		Expect(reduced.Message.GetText()).To(Equal("adapter reduction"))
	})

	// The capture-side facts the upstream bytes do not carry. Under
	// dual-send the producer reduced live and had both; under raw-only the
	// reduction happens here, from stored bytes, and ingest has to put them
	// back or the row means something different than it used to.
	Describe("capture-side fields on a server-side reduction", func() {
		// The duration a real dual-send turn reduced to live in the
		// PCC-1029 clearing validation. Reducing that same turn's stored
		// bytes offline produced no duration at all — this is the value
		// that has to survive the crossing.
		const liveElapsedSeconds = 1.747314975

		It("stamps the duration from the envelope's elapsed_seconds", func() {
			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType:    "text/event-stream",
					ElapsedSeconds: liveElapsedSeconds,
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// The regression this guards: without the stamp the reduction
			// carries no duration and the derived span's duration_ns lands
			// NULL — the PCC-514/570 bug, reintroduced by moving reduction
			// to the server.
			Expect(reduced.Usage).NotTo(BeNil())
			Expect(reduced.Usage.TotalDurationNs).NotTo(BeZero())
			Expect(reduced.Usage.TotalDurationNs).To(BeNumerically("~", 1747314975, 1000))

			Expect(stampCount(server, "duration", "elapsed_seconds")).To(Equal(1.0))
		})

		It("leaves the duration unstamped, and counted, when the envelope has none", func() {
			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta:        ingest.TurnMeta{ContentType: "text/event-stream"},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// Ingest's own clock measures the dispatch hop, not the call, so
			// there is nothing honest to put here. The turn still lands; the
			// counter is what makes the hole visible.
			Expect(reduced.Usage.TotalDurationNs).To(BeZero())
			Expect(stampCount(server, "duration", "fallback")).To(Equal(1.0))
		})

		It("stamps created_at from the envelope's capture time", func() {
			captured := time.Date(2026, 7, 28, 17, 36, 15, 0, time.UTC)

			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType: "text/event-stream",
					CapturedAt:  captured.Format(time.RFC3339),
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// The contract: CreatedAt is when the turn happened. The reducer
			// stamped time.Now() during this test; the envelope's capture
			// time has to win over it.
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally("==", captured))
			Expect(stampCount(server, "created_at", "captured_at")).To(Equal(1.0))
		})

		It("dates the turn from ts_request plus elapsed when captured_at is absent", func() {
			requested := time.Date(2026, 7, 28, 17, 36, 15, 0, time.UTC)

			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType:    "text/event-stream",
					TsRequest:      requested.Format(time.RFC3339Nano),
					ElapsedSeconds: liveElapsedSeconds,
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// ts_request is the request instant; adding the call's duration
			// gives the completion instant CreatedAt denotes. Reusing the
			// field derive.CapturedAt already reads is what keeps CreatedAt
			// and the derived span's StartedAt on the same clock.
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally(
				"~", requested.Add(1747*time.Millisecond), time.Millisecond))
			Expect(stampCount(server, "created_at", "ts_request")).To(Equal(1.0))
		})

		It("dates the turn from ts_request alone when no elapsed is available", func() {
			requested := time.Date(2026, 7, 28, 17, 36, 15, 0, time.UTC)

			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType: "text/event-stream",
					TsRequest:   requested.Format(time.RFC3339Nano),
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// Early by the call's duration, which is a far smaller error than
			// the ingest hop it replaces.
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally("==", requested))
			Expect(stampCount(server, "created_at", "ts_request")).To(Equal(1.0))
		})

		It("prefers captured_at over ts_request when both are present", func() {
			completed := time.Date(2026, 7, 28, 17, 36, 17, 0, time.UTC)

			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType:    "text/event-stream",
					TsRequest:      "2026-07-28T17:36:15Z",
					CapturedAt:     completed.Format(time.RFC3339),
					ElapsedSeconds: liveElapsedSeconds,
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// captured_at is the completion instant outright; it needs no
			// arithmetic, so it wins.
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally("==", completed))
			Expect(stampCount(server, "created_at", "captured_at")).To(Equal(1.0))
			Expect(stampCount(server, "created_at", "ts_request")).To(BeZero())
		})

		It("falls back to ingest time, and counts it, when the envelope has no capture time", func() {
			before := time.Now().UTC().Add(-time.Second)

			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta:        ingest.TurnMeta{ContentType: "text/event-stream"},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// Documents the degradation rather than hiding it: with no
			// producer sending captured_at yet, an Anthropic raw-only row's
			// CreatedAt is ingest time. The counter is the signal that says
			// so, and the reason this is a contract decision and not a bug
			// fix.
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally(">=", before))
			Expect(stampCount(server, "created_at", "fallback")).To(Equal(1.0))
		})

		It("keeps the turn and counts a fallback when captured_at is malformed", func() {
			post(ingest.TurnPayload{
				Provider:    "anthropic",
				RawRequest:  anthropicRequest,
				RawResponse: []byte(anthropicSSE),
				Meta: ingest.TurnMeta{
					ContentType: "text/event-stream",
					CapturedAt:  "yesterday afternoon",
				},
			})

			// A producer bug is not a reason to lose a turn whose bytes are
			// already stored.
			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())
			Expect(reduced.Message.GetText()).To(Equal("Hello world"))
			Expect(stampCount(server, "created_at", "fallback")).To(Equal(1.0))
		})

		It("does not touch a payload that arrived with its own reduction", func() {
			adapterCreatedAt := time.Date(2026, 7, 28, 17, 36, 15, 0, time.UTC)
			adapterReduction := reducedResponse(
				"claude-3-5-sonnet-20241022", "adapter reduction",
				&llm.Usage{TotalDurationNs: 42},
			)
			adapterReduction.CreatedAt = adapterCreatedAt

			post(ingest.TurnPayload{
				Provider:            "anthropic",
				RawRequest:          anthropicRequest,
				Response:            adapterReduction,
				RawResponse:         []byte(anthropicSSE),
				RawResponseEncoding: "identity",
				Meta: ingest.TurnMeta{
					ContentType:    "text/event-stream",
					ElapsedSeconds: liveElapsedSeconds,
					CapturedAt:     time.Now().UTC().Format(time.RFC3339),
				},
			})

			var reduced llm.ChatResponse
			Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())

			// Dual-send is untouched by all of this: the adapter measured the
			// live stream, so its duration and timestamp stand even where the
			// envelope also carries them.
			Expect(reduced.Usage.TotalDurationNs).To(Equal(int64(42)))
			Expect(reduced.CreatedAt.UTC()).To(BeTemporally("==", adapterCreatedAt))
			Expect(stampCount(server, "", "")).To(BeZero())
		})
	})

	It("reduces a zstd-encoded raw-only payload while storing the compressed bytes", func() {
		// This is the case the raw-only lane existed for and could not
		// serve: Codex traffic is zstd, so until ingest could decode it
		// every zstd turn fell back to dual-send permanently. The bytes
		// were already being stored — what was missing was the ability to
		// get anything back out of them.
		compressed := zstdBytes([]byte(anthropicSSE))

		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         compressed,
			RawResponseEncoding: "zstd",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		Expect(rec.RawResponse).To(Equal(compressed))
		Expect(rec.RawResponseEncoding).To(Equal("zstd"))
		Expect(rec.RawResponseDropped).To(BeFalse())

		var reduced llm.ChatResponse
		Expect(json.Unmarshal(rec.Response, &reduced)).To(Succeed())
		Expect(reduced.Message.GetText()).To(Equal("Hello world"))
	})

	It("reduces a raw-only payload whose stream was cut short", func() {
		// The producer forwards the truncated compressed bytes as they
		// arrived, so these are exactly what ingest is handed. Refusing
		// them would discard a turn that is all there bar its trailer.
		full := gzipBytes([]byte(anthropicSSE))

		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         full[:len(full)-8],
			RawResponseEncoding: "gzip",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		var reduced llm.ChatResponse
		Expect(json.Unmarshal(driver.RawTurns()[0].Response, &reduced)).To(Succeed())
		Expect(reduced.Message.GetText()).To(Equal("Hello world"))
	})

	It("still stores the bytes when the encoding is one it cannot decode", func() {
		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         []byte("\x28\xb5\x2f\xfd not really zstd"),
			RawResponseEncoding: "zstd",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		// zstd is decodable now, but these bytes are not valid zstd. A
		// reduction ingest cannot perform is still not a reason to lose
		// the bytes — the recovery path re-reads this column, so a fix to
		// the decoder or the reducer can still reach this row.
		Expect(rec.RawResponse).NotTo(BeEmpty())
		Expect(rec.RawResponseEncoding).To(Equal("zstd"))
		Expect(rec.RawResponseDropped).To(BeFalse())
	})
})
