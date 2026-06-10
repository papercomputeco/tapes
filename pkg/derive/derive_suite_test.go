package derive_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDerive(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Derive Suite")
}
