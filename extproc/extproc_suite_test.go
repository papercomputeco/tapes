package extproc

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestExtproc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Extproc Suite")
}
