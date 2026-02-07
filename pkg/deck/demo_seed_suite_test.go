package deck

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDemoSeed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Demo Seed Suite")
}
