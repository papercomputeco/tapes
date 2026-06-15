package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
)

// fakeSpanSearcher implements SpanSearcher in memory.
type fakeSpanSearcher struct {
	hits    []spanembed.Hit
	err     error
	lastOrg string
}

func (f *fakeSpanSearcher) Search(_ context.Context, orgID string, _ []float32, _ int) ([]spanembed.Hit, error) {
	f.lastOrg = orgID
	if f.err != nil {
		return nil, f.err
	}
	return f.hits, nil
}

var _ = Describe("handleSearchSpansEndpoint", func() {
	var (
		server   *Server
		searcher *fakeSpanSearcher
	)

	newSpanSearchServer := func(cfg Config) *Server {
		s, err := NewServer(cfg, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return s
	}

	BeforeEach(func() {
		searcher = &fakeSpanSearcher{}
		server = newSpanSearchServer(Config{
			ListenAddr:   ":0",
			Embedder:     testutils.NewMockEmbedder(),
			SpanSearcher: searcher,
		})
	})

	It("returns 503 when span search is not configured", func() {
		bare := newSpanSearchServer(Config{ListenAddr: ":0"})
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=x", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := bare.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusServiceUnavailable))
	})

	It("returns 503 when no embed pass has initialized the store", func() {
		searcher.err = spanembed.ErrNotInitialized
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=x", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusServiceUnavailable))
	})

	It("returns 400 when the query parameter is missing", func() {
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	It("returns 400 when top_k is not a positive integer", func() {
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=x&top_k=-2", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	It("returns hits with their trace/turn context, scoped to the asserted org", func() {
		startedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		searcher.hits = []spanembed.Hit{{
			TraceID:    "trc_req1",
			SpanID:     "llm_req1",
			SessionID:  "5b6f0f8e-2c3a-4ec0-9b6e-000000000001",
			Score:      0.91,
			UserPrompt: "fix the retry backoff",
			Snippet:    "set max-poll-backoff to 30s",
			Model:      "claude-sonnet-4-5",
			StartedAt:  startedAt,
		}}

		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=retry+backoff&top_k=3", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("X-Tapes-Org-Id", "11111111-1111-1111-1111-111111111111")
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

		body, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		var out SpanSearchOutput
		Expect(json.Unmarshal(body, &out)).To(Succeed())

		Expect(out.Query).To(Equal("retry backoff"))
		Expect(out.Count).To(Equal(1))
		Expect(out.Results).To(HaveLen(1))
		Expect(out.Results[0].TraceID).To(Equal("trc_req1"))
		Expect(out.Results[0].SpanID).To(Equal("llm_req1"))
		Expect(out.Results[0].SessionID).To(Equal("5b6f0f8e-2c3a-4ec0-9b6e-000000000001"))
		Expect(out.Results[0].UserPrompt).To(Equal("fix the retry backoff"))
		Expect(out.Results[0].Snippet).To(ContainSubstring("max-poll-backoff"))
		Expect(out.Results[0].StartedAt).To(Equal(startedAt))

		Expect(searcher.lastOrg).To(Equal("11111111-1111-1111-1111-111111111111"))
	})

	It("defaults the org to the nil tenant when no header is sent", func() {
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=x", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(searcher.lastOrg).To(Equal(nilOrgID))
	})

	It("returns 500 on search failures", func() {
		searcher.err = errors.New("pg down")
		req, err := http.NewRequest(http.MethodGet, "/v1/search/spans?query=x", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusInternalServerError))
	})
})
