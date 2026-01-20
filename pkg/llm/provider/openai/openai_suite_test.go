package openai_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOpenAI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpenAI Provider Suite")
}
