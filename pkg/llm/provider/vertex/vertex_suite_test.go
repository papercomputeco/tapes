package vertex_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestVertex(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Vertex Provider Suite")
}
