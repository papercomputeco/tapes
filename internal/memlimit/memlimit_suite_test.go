package memlimit

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMemlimit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Soft Memory Limit Suite")
}
