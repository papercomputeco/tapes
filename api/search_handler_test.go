package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apisearch "github.com/papercomputeco/tapes/api/search"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
	"github.com/papercomputeco/tapes/pkg/vector"
)

var _ = Describe("handleSearchEndpoint", func() {
	var (
		server       *Server
		inMem        *inmemory.Driver
		vectorDriver *testutils.MockVectorDriver
		embedder     *testutils.MockEmbedder
		ctx          context.Context
	)

	BeforeEach(func() {
		logger := tapeslogger.Nop()
		inMem = inmemory.NewDriver()
		vectorDriver = testutils.NewMockVectorDriver()
		embedder = testutils.NewMockEmbedder()
		ctx = context.Background()

		var err error
		server, err = NewServer(
			Config{
				ListenAddr:   ":0",
				VectorDriver: vectorDriver,
				Embedder:     embedder,
			},
			inMem,
			inMem,
			logger,
		)
		Expect(err).NotTo(HaveOccurred())
	})

	Context("when search is not configured", func() {
		It("returns 503 when vector driver and embedder are nil", func() {
			logger := tapeslogger.Nop()
			noSearchServer, err := NewServer(
				Config{ListenAddr: ":0"},
				inMem,
				inMem,
				logger,
			)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=test", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := noSearchServer.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusServiceUnavailable))
		})
	})

	Context("when query parameter is missing", func() {
		It("returns 400", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("query parameter is required"))
		})
	})

	Context("when query parameter is empty", func() {
		It("returns 400", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		})
	})

	Context("when top_k is invalid", func() {
		It("returns 400 for non-integer top_k", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=test&top_k=abc", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("top_k must be a positive integer"))
		})

		It("returns 400 for zero top_k", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=test&top_k=0", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		})

		It("returns 400 for negative top_k", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=test&top_k=-1", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		})
	})

	Context("when search succeeds with no results", func() {
		It("returns 200 with empty results", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=hello", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var output apisearch.Output
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(body, &output)).To(Succeed())

			Expect(output.Query).To(Equal("hello"))
			Expect(output.Count).To(Equal(0))
			Expect(output.Results).To(BeEmpty())
		})
	})

	Context("when search succeeds with results", func() {
		It("returns 200 with search results", func() {
			node1 := merkle.NewNode(testutils.NewTestBucket("user", "Hello"), nil)
			node2 := merkle.NewNode(testutils.NewTestBucket("assistant", "Hi there"), node1)

			_, err := inMem.Put(ctx, node1)
			Expect(err).NotTo(HaveOccurred())
			_, err = inMem.Put(ctx, node2)
			Expect(err).NotTo(HaveOccurred())

			vectorDriver.Results = []vector.QueryResult{
				{
					Document: vector.Document{
						ID:   node2.Hash,
						Hash: node2.Hash,
					},
					Score: 0.95,
				},
			}

			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=greeting&top_k=3", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var output apisearch.Output
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(body, &output)).To(Succeed())

			Expect(output.Query).To(Equal("greeting"))
			Expect(output.Count).To(Equal(1))
			Expect(output.Results).To(HaveLen(1))
			Expect(output.Results[0].Hash).To(Equal(node2.Hash))
			Expect(output.Results[0].Score).To(Equal(float32(0.95)))
			Expect(output.Results[0].Role).To(Equal("assistant"))
			Expect(output.Results[0].Preview).To(Equal("Hi there"))
			Expect(output.Results[0].Branch).To(HaveLen(2))
			Expect(output.Results[0].Branch[0].Text).To(Equal("Hello"))
			Expect(output.Results[0].Branch[1].Text).To(Equal("Hi there"))
			Expect(output.Results[0].Branch[1].Matched).To(BeTrue())
		})
	})

	Context("when vector query fails", func() {
		It("returns 500", func() {
			vectorDriver.FailQuery = true

			req, err := http.NewRequest(http.MethodGet, "/v1/search?query=test", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusInternalServerError))
		})
	})
})
