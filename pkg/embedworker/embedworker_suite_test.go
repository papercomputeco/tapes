package embedworker_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEmbedWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EmbedWorker Suite")
}
