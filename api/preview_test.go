package api

// Preview mode is served from the previews the deriver stored, never from
// the payload: the reader hands the handler records with Input and Output
// nil, and what goes on the wire is the stored column as it is. The stored
// previews in these fixtures are deliberately not what derive.PreviewBlocks
// would produce from the payload, so a handler that fell back to computing
// one would be told apart from one that served the column. Comparisons are
// on decoded JSON: Postgres canonicalizes JSONB key order, so preview bytes
// are not part of the contract.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// storedPreview is the preview column a fixture span carries: something
// the payload would never truncate to.
func storedPreview(side, text string) json.RawMessage {
	return mustJSON([]map[string]string{{"type": "text", "text": "stored " + side + " preview of " + text}})
}

// withStoredPreviews stamps every span of the model with stored previews,
// the shape a row derived after the preview columns existed has.
func withStoredPreviews(d *pagedSpanModel) *pagedSpanModel {
	for traceID, spans := range d.spans {
		for i := range spans {
			spans[i].InputPreview = storedPreview("input", spans[i].SpanID)
			spans[i].OutputPreview = storedPreview("output", spans[i].SpanID)
			spans[i].HasPreview = true
		}
		d.spans[traceID] = spans
	}
	return d
}

func decodeJSON(raw json.RawMessage) any {
	var v any
	ExpectWithOffset(1, json.Unmarshal(raw, &v)).To(Succeed(), string(raw))
	return v
}

func getTrace(server *Server, traceID, query string) (*http.Response, []byte) {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/traces/"+traceID+query, nil)
	response, err := server.app.Test(request, fiber.TestConfig{Timeout: 30 * time.Second})
	Expect(err).NotTo(HaveOccurred())
	body, err := io.ReadAll(response.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.Body.Close()).To(Succeed())
	return response, body
}

var _ = Describe("payload=preview", func() {
	It("serves stored previews", func() {
		driver := withStoredPreviews(newPagedSpanModel(4, 3))
		server := newPagedServer(driver)

		response, body := getPage(server, "?payload=preview")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		page := decodePage(body)

		var seen int
		for _, trace := range page.Traces {
			for _, item := range trace.Spans {
				seen++
				Expect(item.Payload).To(Equal("preview"), "%s/%s", item.TraceID, item.SpanID)
				Expect(decodeJSON(item.Input)).To(Equal(decodeJSON(storedPreview("input", item.SpanID))),
					"%s/%s input is the stored preview", item.TraceID, item.SpanID)
				Expect(decodeJSON(item.Output)).To(Equal(decodeJSON(storedPreview("output", item.SpanID))),
					"%s/%s output is the stored preview", item.TraceID, item.SpanID)
				// The payload never reaches the wire in preview mode.
				Expect(string(item.Input)).NotTo(ContainSubstring("ask "))
				Expect(string(item.Output)).NotTo(ContainSubstring("result "))
			}
		}
		Expect(seen).To(BeNumerically(">", 0))

		// The envelope is the composite's; only the spans' payload fields
		// differ from full mode.
		_, fullBody := getPage(server, "?payload=full")
		full := decodePage(fullBody)
		Expect(page.Session).To(Equal(full.Session))
		Expect(page.Links).To(Equal(full.Links))
		Expect(page.Traces).To(HaveLen(len(full.Traces)))
		for i := range full.Traces {
			Expect(page.Traces[i].Trace).To(Equal(full.Traces[i].Trace))
			Expect(page.Traces[i].Spans).To(HaveLen(len(full.Traces[i].Spans)))
		}
	})

	It("renders the same wire items through the reference builder", func() {
		// BuildSessionTraces is what `tapes dev trace-fixtures` renders
		// preview fixtures through; it must serve stored previews the same
		// way the streamed handler does.
		driver := withStoredPreviews(newPagedSpanModel(3, 2))
		server := newPagedServer(driver)

		_, body := getPage(server, "?payload=preview")
		streamed := decodePage(body)

		reference, err := json.Marshal(driver.reference(PayloadPreview))
		Expect(err).NotTo(HaveOccurred())
		Expect(decodeJSON(body)).To(Equal(decodeJSON(reference)))
		Expect(streamed.Traces).NotTo(BeEmpty())
	})

	It("marks spans without a stored preview as preview_pending", func() {
		// A row derived before the preview columns existed and not yet
		// backfilled: no preview, and the payload is not consulted.
		driver := newPagedSpanModel(2, 2)
		server := newPagedServer(driver)

		response, body := getPage(server, "?payload=preview")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		page := decodePage(body)

		var seen int
		for _, trace := range page.Traces {
			for _, item := range trace.Spans {
				seen++
				Expect(item.Payload).To(Equal("preview_pending"), "%s/%s", item.TraceID, item.SpanID)
				Expect(string(item.Input)).To(Equal("[]"))
				Expect(string(item.Output)).To(Equal("[]"))
			}
		}
		Expect(seen).To(Equal(4))

		// Mixed within one trace: each span is marked on its own row.
		mixed := withStoredPreviews(newPagedSpanModel(1, 2))
		traceID := mixed.turns[0].TraceID
		mixed.spans[traceID][1].InputPreview = nil
		mixed.spans[traceID][1].OutputPreview = nil
		mixed.spans[traceID][1].HasPreview = false
		_, body = getPage(newPagedServer(mixed), "?payload=preview")
		page = decodePage(body)
		Expect(page.Traces).To(HaveLen(1))
		Expect(page.Traces[0].Spans).To(HaveLen(2))
		Expect(page.Traces[0].Spans[0].Payload).To(Equal("preview"))
		Expect(page.Traces[0].Spans[1].Payload).To(Equal("preview_pending"))
		Expect(string(page.Traces[0].Spans[1].Input)).To(Equal("[]"))
	})

	It("full mode is unchanged", func() {
		// The byte-identity spec in traces_stream_test.go pins full mode
		// against the reference; this pins that stored previews on the row
		// do not leak into it, and that no payload marker appears.
		bare := newPagedSpanModel(3, 3)
		previewed := withStoredPreviews(newPagedSpanModel(3, 3))

		_, wantBody := getPage(newPagedServer(bare), "?payload=full")
		_, gotBody := getPage(newPagedServer(previewed), "?payload=full")
		Expect(string(gotBody)).To(Equal(string(wantBody)))
		Expect(string(gotBody)).NotTo(ContainSubstring(`"payload"`))
		Expect(string(gotBody)).NotTo(ContainSubstring("stored input preview"))

		want, err := json.Marshal(previewed.reference(PayloadFull))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(gotBody)).To(Equal(string(want)))

		// Absent and unknown modes are full.
		_, defaulted := getPage(newPagedServer(previewed), "")
		Expect(string(defaulted)).To(Equal(string(wantBody)))
		_, unknown := getPage(newPagedServer(previewed), "?payload=whatever")
		Expect(string(unknown)).To(Equal(string(wantBody)))
	})

	It("serves stored previews on trace detail", func() {
		driver := withStoredPreviews(newPagedSpanModel(3, 2))
		server := newPagedServer(driver)
		traceID := driver.turns[0].TraceID

		response, body := getTrace(server, traceID, "?payload=preview")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		var detail StandaloneTraceDetail
		Expect(json.Unmarshal(body, &detail)).To(Succeed(), string(body))
		Expect(detail.SessionID).To(Equal(pagedSessionID))
		Expect(detail.Trace.TraceID).To(Equal(traceID))
		Expect(detail.Spans).To(HaveLen(2))
		for _, item := range detail.Spans {
			Expect(item.Payload).To(Equal("preview"))
			Expect(decodeJSON(item.Input)).To(Equal(decodeJSON(storedPreview("input", item.SpanID))))
			Expect(decodeJSON(item.Output)).To(Equal(decodeJSON(storedPreview("output", item.SpanID))))
			Expect(string(item.Input)).NotTo(ContainSubstring("ask "))
		}
		Expect(detail.Links).To(HaveLen(1), "the seam touching this trace")

		// The pending marker reaches this route too.
		bare := newPagedSpanModel(1, 1)
		response, body = getTrace(newPagedServer(bare), bare.turns[0].TraceID, "?payload=preview")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		var pending StandaloneTraceDetail
		Expect(json.Unmarshal(body, &pending)).To(Succeed())
		Expect(pending.Spans).To(HaveLen(1))
		Expect(pending.Spans[0].Payload).To(Equal("preview_pending"))
		Expect(string(pending.Spans[0].Input)).To(Equal("[]"))

		// And full mode on this route embeds the payload, unmarked.
		response, body = getTrace(server, traceID, "")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		var full StandaloneTraceDetail
		Expect(json.Unmarshal(body, &full)).To(Succeed())
		Expect(full.Spans).To(HaveLen(2))
		for _, item := range full.Spans {
			Expect(item.Payload).To(BeEmpty())
			// Decoded: json.Marshal escapes the payload's "<" on the wire.
			var blocks []map[string]string
			Expect(json.Unmarshal(item.Input, &blocks)).To(Succeed())
			Expect(blocks).To(HaveLen(1))
			Expect(blocks[0]["text"]).To(HavePrefix("ask <"))
		}
	})
})
