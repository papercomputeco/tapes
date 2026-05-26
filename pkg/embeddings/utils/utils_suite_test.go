package embeddingutils_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEmbeddingUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedding Utils Suite")
}
