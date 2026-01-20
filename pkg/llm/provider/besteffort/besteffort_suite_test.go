package besteffort_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBestEffort(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BestEffort Provider Suite")
}
