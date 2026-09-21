package internallisten_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInternalListen(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Listener Suite")
}
