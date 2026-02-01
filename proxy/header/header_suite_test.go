package header

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHeader(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Header Suite")
}
