package proxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// gatedCaptureDriver holds every PutRawTurn until release is closed, so a
// captured turn stays queued while the client keeps using the connection.
type gatedCaptureDriver struct {
	*captureDriver

	release     chan struct{}
	releaseOnce sync.Once
}

func (d *gatedCaptureDriver) PutRawTurn(ctx context.Context, rec storage.RawTurnRecord) (bool, error) {
	<-d.release
	return d.captureDriver.PutRawTurn(ctx, rec)
}

func (d *gatedCaptureDriver) open() {
	d.releaseOnce.Do(func() { close(d.release) })
}

var _ = Describe("Raw request capture across keep-alive requests", func() {
	var (
		p        *Proxy
		driver   *gatedCaptureDriver
		upstream *httptest.Server
		client   *http.Client
		baseURL  string
	)

	BeforeEach(func() {
		upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(makeOllamaResponseBody("test-model", "assistant", "ok"))
		}))

		driver = &gatedCaptureDriver{captureDriver: newCaptureDriver(), release: make(chan struct{})}
		var err error
		p, err = New(
			Config{ListenAddr: ":0", UpstreamURL: upstream.URL, ProviderType: "ollama"},
			driver,
			tapeslogger.NewNoop(),
		)
		Expect(err).NotTo(HaveOccurred())

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() { _ = p.RunWithListener(ln) }()
		baseURL = "http://" + ln.Addr().String()

		// One connection, reused: the way an agent harness talks to the proxy.
		client = &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1}}
	})

	AfterEach(func() {
		driver.open()
		if p != nil {
			p.Close()
		}
		client.CloseIdleConnections()
		upstream.Close()
	})

	// post reports whether the request went over a reused connection.
	post := func(body []byte) bool {
		var reused bool
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		}
		req, err := http.NewRequestWithContext(
			httptrace.WithClientTrace(context.Background(), trace),
			http.MethodPost, baseURL+"/api/chat", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		Expect(err).NotTo(HaveOccurred())
		_, err = io.Copy(io.Discard, resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		return reused
	}

	It("keeps each queued raw request intact when the next request reuses the connection", func() {
		stream := false
		// Same length, different bytes, so the second body lands exactly on
		// top of the first if the proxy retains fasthttp's request buffer.
		first := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: strings.Repeat("a", 4096)},
		}, &stream)
		second := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: strings.Repeat("b", 4096)},
		}, &stream)
		Expect(first).To(HaveLen(len(second)))

		post(first)
		// The bug needs the second request on the first one's connection;
		// without reuse this test would pass on a fresh buffer.
		Expect(post(second)).To(BeTrue(), "second request did not reuse the connection")

		driver.open()
		p.Close()
		p = nil

		// Workers may store the two turns in either order.
		raws := driver.RawTurns()
		got := make([]string, 0, len(raws))
		for _, raw := range raws {
			got = append(got, string(raw.RawRequest))
		}
		Expect(got).To(ConsistOf(string(first), string(second)))
	})
})
