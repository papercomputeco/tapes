package mcp_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/mcp"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
)

// fakeSpanSearcher implements mcp.SpanSearcher in memory.
type fakeSpanSearcher struct {
	hits []spanembed.Hit
	err  error
}

func (f *fakeSpanSearcher) Search(_ context.Context, _ string, _ []float32, _ int) ([]spanembed.Hit, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.hits, nil
}

var _ = Describe("MCP Server", func() {
	var (
		server   *mcp.Server
		searcher *fakeSpanSearcher
		embedder *testutils.MockEmbedder
	)

	BeforeEach(func() {
		logger := tapeslogger.NewNoop()
		searcher = &fakeSpanSearcher{}
		embedder = testutils.NewMockEmbedder()

		var err error
		server, err = mcp.NewServer(mcp.Config{
			SpanSearcher: searcher,
			Embedder:     embedder,
			Logger:       logger,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("NewServer", func() {
		It("returns an error when span searcher is nil", func() {
			logger := tapeslogger.NewNoop()
			_, err := mcp.NewServer(mcp.Config{
				Embedder: embedder,
				Logger:   logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("span searcher is required"))
		})

		It("returns an error when embedder is nil", func() {
			logger := tapeslogger.NewNoop()
			_, err := mcp.NewServer(mcp.Config{
				SpanSearcher: searcher,
				Logger:       logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("embedder is required"))
		})

		It("returns an error when logger is nil", func() {
			_, err := mcp.NewServer(mcp.Config{
				SpanSearcher: searcher,
				Embedder:     embedder,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("logger is required"))
		})

		It("creates a server with valid config", func() {
			Expect(server).NotTo(BeNil())
		})

		It("returns an HTTP handler", func() {
			handler := server.Handler()
			Expect(handler).NotTo(BeNil())
		})

		It("creates a noop server with no tools", func() {
			s, err := mcp.NewServer(mcp.Config{Noop: true})
			Expect(err).NotTo(HaveOccurred())
			Expect(s).NotTo(BeNil())
		})
	})
})
