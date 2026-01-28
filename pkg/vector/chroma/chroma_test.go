package chroma_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/chroma"
)

var _ = Describe("ChromaDriver", func() {
	var logger *zap.Logger

	BeforeEach(func() {
		logger = zap.NewNop()
	})

	Describe("NewChromaDriver", func() {
		It("should return an error when URL is empty", func() {
			_, err := chroma.NewChromaDriver(chroma.Config{URL: ""}, logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("chroma URL is required"))
		})

		It("should use default collection name when not specified", func() {
			// This test would require a running Chroma instance
			// Skipping for unit tests - should be covered in integration tests
			Skip("Requires running Chroma instance")
		})
	})

	Describe("Interface compliance", func() {
		It("should implement vector.VectorDriver interface", func() {
			// Compile-time check that ChromaDriver implements VectorDriver
			var _ vector.VectorDriver = (*chroma.ChromaDriver)(nil)
		})
	})
})
