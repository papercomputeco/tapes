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

	It("still stores the bytes when the encoding is one it cannot decode", func() {
		post(ingest.TurnPayload{
			Provider:            "anthropic",
			RawRequest:          anthropicRequest,
			RawResponse:         []byte("\x28\xb5\x2f\xfd not really zstd"),
			RawResponseEncoding: "zstd",
			Meta:                ingest.TurnMeta{ContentType: "text/event-stream"},
		})

		rec := driver.RawTurns()[0]
		// A reduction ingest cannot perform is not a reason to lose the bytes:
		// they are exactly what a later build with a zstd decoder re-derives
		// from.
		Expect(rec.RawResponse).NotTo(BeEmpty())
		Expect(rec.RawResponseEncoding).To(Equal("zstd"))
		Expect(rec.RawResponseDropped).To(BeFalse())
	})
})
