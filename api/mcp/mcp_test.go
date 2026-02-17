package mcp_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/mcp"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
)

var _ = Describe("MCP Server", func() {
	var (
		server       *mcp.Server
		driver       *inmemory.Driver
		vectorDriver *testutils.MockVectorDriver
		embedder     *testutils.MockEmbedder
	)

	BeforeEach(func() {
		logger := tapeslogger.Nop()
		driver = inmemory.NewDriver()
		vectorDriver = testutils.NewMockVectorDriver()
		embedder = testutils.NewMockEmbedder()

		var err error
		server, err = mcp.NewServer(mcp.Config{
			DagLoader:    driver,
			VectorDriver: vectorDriver,
			Embedder:     embedder,
			Logger:       logger,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("NewServer", func() {
		It("returns an error when storage driver is nil", func() {
			logger := tapeslogger.Nop()
			_, err := mcp.NewServer(mcp.Config{
				VectorDriver: vectorDriver,
				Embedder:     embedder,
				Logger:       logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage driver is required"))
		})

		It("returns an error when vector driver is nil", func() {
			logger := tapeslogger.Nop()
			_, err := mcp.NewServer(mcp.Config{
				DagLoader: driver,
				Embedder:  embedder,
				Logger:    logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("vector driver is required"))
		})

		It("returns an error when embedder is nil", func() {
			logger := tapeslogger.Nop()
			_, err := mcp.NewServer(mcp.Config{
				DagLoader:    driver,
				VectorDriver: vectorDriver,
				Logger:       logger,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("embedder is required"))
		})

		It("returns an error when logger is nil", func() {
			_, err := mcp.NewServer(mcp.Config{
				DagLoader:    driver,
				VectorDriver: vectorDriver,
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
	})
})
