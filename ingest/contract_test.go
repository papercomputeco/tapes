package ingest_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
)

type contractCase struct {
	name       string
	body       []byte
	wantStatus int
	// wantRow is whether a raw-turn row should eventually appear. The
	// ingest server persists the immutable raw envelope BEFORE provider
	// parsing, so a turn that fails parsing (422, well-formed envelope) is
	// still captured for a future parser fix to re-derive — only a
	// rejected envelope (400) skips the raw write.
	wantRow bool
}

// mustMarshalPayload encodes the test fixture and panics on failure. The
// inputs are static literals, so a marshal error means a programmer typo,
// not a runtime condition the suite should be lenient about.
func mustMarshalPayload(p ingest.TurnPayload) []byte {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return b
}

func buildValidAnthropicOneshot() []byte {
	req := json.RawMessage(`{"model":"claude-3-5-sonnet-20241022","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)
	return mustMarshalPayload(ingest.TurnPayload{Provider: "anthropic", AgentName: "contract", RawRequest: req, Response: reducedResponse("claude-3-5-sonnet-20241022", "hi", nil)})
}

func buildUnknownProvider() []byte {
	return mustMarshalPayload(ingest.TurnPayload{Provider: "nope", RawRequest: json.RawMessage(`{}`), Response: reducedResponse("", "ok", nil)})
}

func buildMissingProvider() []byte {
	return mustMarshalPayload(ingest.TurnPayload{RawRequest: json.RawMessage(`{}`), Response: reducedResponse("", "ok", nil)})
}

func buildMalformedEnvelope() []byte { return []byte(`{not json`) }

func buildEmptyBody() []byte { return []byte{} }

var _ = Describe("Ingest contract", func() {
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
		Expect(server.Close()).To(Succeed())
	})

	DescribeTable("status code × post-condition matrix",
		func(tc contractCase) {
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(tc.body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(tc.wantStatus), "status for %q", tc.name)

			// On the accepted path the raw-turn row lands; on a rejected
			// path nothing is captured. The node DAG is retired, so the
			// post-condition is asserted against the raw layer.
			if tc.wantRow {
				Eventually(driver.CountRaw).
					WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
					Should(BeNumerically(">", 0))
			} else {
				Consistently(driver.CountRaw, 250*time.Millisecond, 50*time.Millisecond).
					Should(Equal(0))
			}
		},
		Entry("valid anthropic oneshot → 202 + raw row",
			contractCase{name: "valid_anthropic_oneshot", body: buildValidAnthropicOneshot(), wantStatus: http.StatusAccepted, wantRow: true}),
		Entry("unknown provider → 422 + raw row (captured before parse)",
			contractCase{name: "unknown_provider", body: buildUnknownProvider(), wantStatus: http.StatusUnprocessableEntity, wantRow: true}),
		Entry("missing provider field → 422 + raw row (captured before parse)",
			contractCase{name: "missing_provider", body: buildMissingProvider(), wantStatus: http.StatusUnprocessableEntity, wantRow: true}),
		Entry("malformed JSON envelope → 400 + no row",
			contractCase{name: "malformed_envelope", body: buildMalformedEnvelope(), wantStatus: http.StatusBadRequest, wantRow: false}),
		Entry("empty body → 400 + no row",
			contractCase{name: "empty_body", body: buildEmptyBody(), wantStatus: http.StatusBadRequest, wantRow: false}),
	)

	It("returns 422 when reduced response validation fails", func() {
		req := json.RawMessage(`{"model":"claude-3-5-sonnet","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)
		payload := ingest.TurnPayload{Provider: "anthropic", RawRequest: req}
		body, _ := json.Marshal(payload)

		httpResp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer httpResp.Body.Close()
		Expect(httpResp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
		b, _ := io.ReadAll(httpResp.Body)
		Expect(string(b)).To(ContainSubstring("invalid reduced response"))
	})
})
