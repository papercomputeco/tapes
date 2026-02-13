package startcmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStart(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Start Command Suite")
}
