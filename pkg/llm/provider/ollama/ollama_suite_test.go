package ollama_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOllama(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ollama Provider Suite")
}
