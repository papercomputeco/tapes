package ingest_test

// The app-level body-limit error handler: an oversized POST must be observable
// (one metric sample, one warn line, JSON envelope) while every other error
// keeps Fiber's default handling byte-for-byte.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// syncBuffer is a goroutine-safe log sink: the server logs from its own
// goroutines while specs read the captured output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newWarnCapturingServer mirrors newTestServer but routes warn-and-above slog
// output into the returned buffer so specs can assert on emitted lines.
func newWarnCapturingServer() (*ingest.Server, *captureDriver, string, *syncBuffer) {
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	driver := newCaptureDriver()

	s, err := ingest.New(
		ingest.Config{
			ListenAddr: ":0",
			Project:    "test-project",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	go func() {
		_ = s.RunWithListener(ln)
	}()

	return s, driver, "http://" + ln.Addr().String(), logBuf
}

// postOverLimit POSTs a MaxIngestBodyBytes+1 body over a raw connection: the
// server rejects on the declared Content-Length and closes without reading, so
// a streamed net/http POST would race its body write against the early 413.
func postOverLimit(baseURL, path string) (*http.Response, []byte) {
	return overLimitRequest(http.MethodPost, baseURL, path)
}

func overLimitRequest(method, baseURL, path string) (*http.Response, []byte) {
	size := ingest.MaxIngestBodyBytes + 1
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(context.Background(), "tcp", strings.TrimPrefix(baseURL, "http://"))
	Expect(err).NotTo(HaveOccurred())
	defer conn.Close()
	Expect(conn.SetDeadline(time.Now().Add(10 * time.Second))).To(Succeed())

	head := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: tapes-test\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", method, path, size)
	_, err = io.WriteString(conn, head)
	Expect(err).NotTo(HaveOccurred())
	// The upload may die partway once the server has rejected and closed;
	// the 413 already sits in our receive buffer.
	_, _ = conn.Write(bytes.Repeat([]byte("x"), size))

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp, body
}

var _ = Describe("body-limit rejections", func() {
	var (
		server  *ingest.Server
		driver  *captureDriver
		baseURL string
		logBuf  *syncBuffer
		client  *http.Client
	)

	BeforeEach(func() {
		server, driver, baseURL, logBuf = newWarnCapturingServer()
		client = &http.Client{Timeout: 5 * time.Second}
	})

	AfterEach(func() {
		Expect(server.Close()).To(Succeed())
	})

	It("rejects an over-limit POST with 413 and the llm.ErrorResponse JSON envelope", func() {
		resp, body := postOverLimit(baseURL, "/v1/ingest")

		Expect(resp.StatusCode).To(Equal(http.StatusRequestEntityTooLarge))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

		var envelope llm.ErrorResponse
		Expect(json.Unmarshal(body, &envelope)).To(Succeed(),
			"the 413 body must be the surface's standard JSON error envelope, got: %s", body)
		Expect(envelope.Error).NotTo(BeEmpty())
	})

	It("records exactly one reject_oversize sample and one structured warn line per rejection", func() {
		resp, _ := postOverLimit(baseURL, "/v1/ingest")
		Expect(resp.StatusCode).To(Equal(http.StatusRequestEntityTooLarge))

		scrape, err := client.Get(baseURL + "/metrics")
		Expect(err).NotTo(HaveOccurred())
		defer scrape.Body.Close()
		txt, _ := io.ReadAll(scrape.Body)

		// The whole sample line: value exactly 1, and the full label set —
		// no path/size/org label may ride on the rejection counter.
		Expect(string(txt)).To(ContainSubstring(
			`tapes_ingest_writes_total{provider="unknown",status="reject_oversize"} 1`))
		Expect(strings.Count(string(txt), `status="reject_oversize"`)).To(Equal(1))
		// Zero bodyBytes on the rejection keeps the accepted-size histogram empty.
		Expect(string(txt)).NotTo(ContainSubstring("tapes_ingest_body_bytes_bucket"))

		logs := logBuf.String()
		Expect(strings.Count(logs, "ingest body over limit")).To(Equal(1))
		Expect(logs).To(ContainSubstring(fmt.Sprintf("content_length=%d", ingest.MaxIngestBodyBytes+1)))
		Expect(logs).To(ContainSubstring(fmt.Sprintf("limit=%d", ingest.MaxIngestBodyBytes)))
		Expect(logs).To(ContainSubstring("path=/v1/ingest"))
	})

	It("delegates non-body-limit errors verbatim to fiber.DefaultErrorHandler", func() {
		// A default-handler app with the same route shape: what these requests
		// produce there is byte-for-byte what the ingest server must produce.
		ref := fiber.New(fiber.Config{DisableStartupMessage: true})
		ref.Post("/v1/ingest", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusAccepted) })
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() {
			_ = ref.Listener(ln)
		}()
		defer func() {
			Expect(ref.Shutdown()).To(Succeed())
		}()
		refURL := "http://" + ln.Addr().String()

		do := func(base, method, path string) (int, string, string) {
			req, err := http.NewRequest(method, base+path, nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := client.Do(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
		}

		status, contentType, body := do(baseURL, http.MethodGet, "/nope")
		refStatus, refContentType, refBody := do(refURL, http.MethodGet, "/nope")
		Expect(status).To(Equal(refStatus))
		Expect(contentType).To(Equal(refContentType))
		Expect(body).To(Equal(refBody))
		// Fiber's plain text, untouched: JSON here would mean the custom
		// handler re-encoded an error it must only pass through.
		Expect(body).To(Equal("Cannot GET /nope"))

		status, contentType, body = do(baseURL, http.MethodDelete, "/v1/ingest")
		refStatus, refContentType, refBody = do(refURL, http.MethodDelete, "/v1/ingest")
		Expect(status).To(Equal(refStatus))
		Expect(contentType).To(Equal(refContentType))
		Expect(body).To(Equal(refBody))
		Expect(status).To(Equal(http.StatusMethodNotAllowed))
	})

	It("leaves body-limit rejections on non-ingest routes to the default handler", func() {
		// Only ingest routes are counted: an oversized POST elsewhere keeps
		// Fiber's plain-text 413 and must not record a reject_oversize sample.
		resp, body := postOverLimit(baseURL, "/nope")
		Expect(resp.StatusCode).To(Equal(http.StatusRequestEntityTooLarge))
		Expect(resp.Header.Get("Content-Type")).NotTo(ContainSubstring("application/json"))
		Expect(string(body)).NotTo(ContainSubstring(`"error"`))

		// Same for an unsupported method on an ingest path: only POST counts.
		resp, body = overLimitRequest(http.MethodDelete, baseURL, "/v1/ingest")
		Expect(resp.StatusCode).To(Equal(http.StatusRequestEntityTooLarge))
		Expect(string(body)).NotTo(ContainSubstring(`"error"`))

		scrape, err := client.Get(baseURL + "/metrics")
		Expect(err).NotTo(HaveOccurred())
		defer scrape.Body.Close()
		txt, _ := io.ReadAll(scrape.Body)
		Expect(string(txt)).NotTo(ContainSubstring(`status="reject_oversize"`))
	})

	It("still accepts a just-under-limit turn through handleIngest", func() {
		payload := ingest.TurnPayload{
			Provider: "ollama",
			RawRequest: mustJSON(ollamaRequest{
				Model:    "llama3",
				Messages: []ollamaMessage{{Role: "user", Content: "Hello"}},
			}),
			Response:    reducedResponse("llama3", "Hi", nil),
			RawResponse: bytes.Repeat([]byte("x"), 3),
		}
		base, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())

		// Base64 grows 3 raw bytes into exactly 4 body bytes, so pad the turn
		// to a few bytes under the real MaxIngestBodyBytes — the shipped
		// boundary, not an injected test limit.
		extra := (ingest.MaxIngestBodyBytes - len(base) - 8) / 4
		payload.RawResponse = bytes.Repeat([]byte("x"), 3+3*extra)
		body, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(body)).To(BeNumerically("<=", ingest.MaxIngestBodyBytes))
		Expect(len(body)).To(BeNumerically(">", ingest.MaxIngestBodyBytes-16))

		resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		// The handler, not the error handler, answered: the turn landed.
		Eventually(driver.CountRaw).
			WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
			Should(Equal(1))
		Expect(driver.RawTurns()[0].Provider).To(Equal("ollama"))
	})
})
