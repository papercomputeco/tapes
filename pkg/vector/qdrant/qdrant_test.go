package qdrant_test

import (
	. "github.com/onsi/ginkgo/v2"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/qdrant"
)

var _ = Describe("Driver", func() {
	Describe("Interface compliance", func() {
		It("should implement vector.Driver interface", func() {
			var _ vector.Driver = (*qdrant.Driver)(nil)
		})
	})
})
