package seed

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSeed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Seed Suite")
}
