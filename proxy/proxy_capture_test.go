package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// newAnthropicTestProxy creates a Proxy pointed at the given upstream URL,
// using an in-memory storage driver and the anthropic provider so
// handleStreamingProxy routes through pkg/capture.
func newAnthropicTestProxy(upstreamURL string) (*Proxy, storage.Driver) {
	logger := tapeslogger.NewNoop()
	driver := inmemory.NewDriver()

	p, err := New(
		Config{
			ListenAddr:   ":0",
			UpstreamURL:  upstreamURL,
			ProviderType: "anthropic",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())
	return p, driver
}

var _ = Describe("Anthropic streaming proxy (capture-backed)", func() {
	var (
		p        *Proxy
		driver   storage.Driver
		upstream *httptest.Server
	)

	AfterEach(func() {
		if p != nil {
			p.Close()
		}
		if upstream != nil {
			upstream.Close()
		}
	})

	BeforeEach(func() {
		upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, ok := w.(http.Flusher)
			Expect(ok).To(BeTrue())
			events := []string{
				"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-3-5-sonnet-20241022\",\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":7,\"output_tokens\":1,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0}}}\n\n",
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n",
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			}
			for _, ev := range events {
				fmt.Fprint(w, ev)
				flusher.Flush()
			}
		}))
		p, driver = newAnthropicTestProxy(upstream.URL)
	})

	It("forwards chunks verbatim and lands a canonical ChatResponse via pkg/capture", func() {
		reqBody := `{"model":"claude-3-5-sonnet-20241022","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`

		resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody)), -1)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		bodyStr := string(body)

		// The Anthropic wire format is preserved verbatim to the client.
		Expect(bodyStr).To(ContainSubstring("event: message_start\n"))
		Expect(bodyStr).To(ContainSubstring("event: message_stop\n"))
		Expect(bodyStr).To(ContainSubstring(`"text":"Hello"`))
		Expect(bodyStr).To(ContainSubstring(`"text":" world"`))

		// Drain worker pool to ensure capture.Reduce enqueued a canonical
		// assistant node.
		p.Close()
		p = nil

		ctx := GinkgoT().Context()
		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(2)) // user + assistant

		leaves, err := driver.Leaves(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(leaves).To(HaveLen(1))
		Expect(leaves[0].Bucket.Role).To(Equal("assistant"))
		Expect(leaves[0].Bucket.ExtractText()).To(Equal("Hello world"))
	})
})
