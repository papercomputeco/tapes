package pgvector_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/pgvector"
)

var _ = Describe("Driver", func() {
	Describe("Interface compliance", func() {
		It("should implement vector.Driver interface", func() {
			var _ vector.Driver = (*pgvector.Driver)(nil)
		})
	})

	Describe("NewDriver", func() {
		It("should return an error when connection string is empty", func() {
			_, err := pgvector.NewDriver(pgvector.Config{
				Dimensions: 128,
			}, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connection string must be provided"))
		})

		It("should return an error when dimensions is zero", func() {
			_, err := pgvector.NewDriver(pgvector.Config{
				ConnString: "postgres://localhost:5432/test",
			}, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("dimensions cannot be 0"))
		})
	})
})
