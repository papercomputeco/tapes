package recap_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRecap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Recap Suite")
}
