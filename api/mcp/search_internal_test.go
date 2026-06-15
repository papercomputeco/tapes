package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
)

// fakeSearcher implements SpanSearcher for the internal handler test.
type fakeSearcher struct {
	hits     []spanembed.Hit
	err      error
	lastOrg  string
	lastTopK int
}

func (f *fakeSearcher) Search(_ context.Context, orgID string, _ []float32, topK int) ([]spanembed.Hit, error) {
	f.lastOrg = orgID
	f.lastTopK = topK
	if f.err != nil {
		return nil, f.err
	}
	return f.hits, nil
}

var _ = Describe("handleSearch", func() {
	var (
		srv      *Server
		searcher *fakeSearcher
	)

	BeforeEach(func() {
		searcher = &fakeSearcher{}
		srv = &Server{
			config: Config{
				SpanSearcher: searcher,
				Embedder:     testutils.NewMockEmbedder(),
				Logger:       tapeslogger.NewNoop(),
			},
		}
	})

	It("returns span-shaped results, not node chains", func() {
		started := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
		searcher.hits = []spanembed.Hit{
			{
				SessionID:  "sess-1",
				TraceID:    "trace-1",
				SpanID:     "span-1",
				Score:      0.91,
				UserPrompt: "how do I do X?",
				Snippet:    "you do X like this",
				Model:      "claude-opus-4",
				StartedAt:  started,
			},
		}

		res, out, err := srv.handleSearch(context.Background(), &mcpsdk.CallToolRequest{}, SearchInput{Query: "x", TopK: 3})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsError).To(BeFalse())

		Expect(out.Query).To(Equal("x"))
		Expect(out.Count).To(Equal(1))
		Expect(out.Results).To(HaveLen(1))

		r := out.Results[0]
		Expect(r.SessionID).To(Equal("sess-1"))
		Expect(r.TraceID).To(Equal("trace-1"))
		Expect(r.SpanID).To(Equal("span-1"))
		Expect(r.Score).To(Equal(float32(0.91)))
		Expect(r.UserPrompt).To(Equal("how do I do X?"))
		Expect(r.Snippet).To(Equal("you do X like this"))
		Expect(r.Model).To(Equal("claude-opus-4"))
		Expect(r.StartedAt).To(Equal(started))

		// topK is threaded through to the span searcher.
		Expect(searcher.lastTopK).To(Equal(3))
		// defaults to the nil-org sentinel like the header-less HTTP path.
		Expect(searcher.lastOrg).To(Equal(nilOrgID))

		// The serialized text block carries the same span shape and has no
		// legacy node-chain fields.
		Expect(res.Content).To(HaveLen(1))
		text := res.Content[0].(*mcpsdk.TextContent).Text
		Expect(text).To(ContainSubstring(`"span_id":"span-1"`))
		Expect(text).To(ContainSubstring(`"trace_id":"trace-1"`))
		Expect(text).NotTo(ContainSubstring(`"branch"`))
		Expect(text).NotTo(ContainSubstring(`"turns"`))
		Expect(text).NotTo(ContainSubstring(`"hash"`))

		// And it round-trips back into the span output shape.
		var decoded SearchOutput
		Expect(json.Unmarshal([]byte(text), &decoded)).To(Succeed())
		Expect(decoded.Results[0].SpanID).To(Equal("span-1"))
	})

	It("defaults topK to 5 when zero", func() {
		_, _, err := srv.handleSearch(context.Background(), &mcpsdk.CallToolRequest{}, SearchInput{Query: "x"})
		Expect(err).NotTo(HaveOccurred())
		Expect(searcher.lastTopK).To(Equal(5))
	})

	It("returns an error result when the embedder fails", func() {
		srv.config.Embedder = &testutils.MockEmbedder{Embeddings: map[string][]float32{}, FailOn: "boom"}
		res, out, err := srv.handleSearch(context.Background(), &mcpsdk.CallToolRequest{}, SearchInput{Query: "boom"})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsError).To(BeTrue())
		Expect(out.Count).To(Equal(0))
	})

	It("returns an error result when span search fails", func() {
		searcher.err = errors.New("search boom")
		res, _, err := srv.handleSearch(context.Background(), &mcpsdk.CallToolRequest{}, SearchInput{Query: "x"})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsError).To(BeTrue())
	})

	It("surfaces ErrNotInitialized from the span store", func() {
		searcher.err = spanembed.ErrNotInitialized
		res, _, err := srv.handleSearch(context.Background(), &mcpsdk.CallToolRequest{}, SearchInput{Query: "x"})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsError).To(BeTrue())
		Expect(res.Content[0].(*mcpsdk.TextContent).Text).To(ContainSubstring("not initialized"))
	})
})
