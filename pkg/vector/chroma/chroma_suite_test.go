package chroma_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestChroma(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Chroma Vector Suite")
}
