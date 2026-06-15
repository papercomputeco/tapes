package spanembed_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSpanEmbed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SpanEmbed Suite")
}
