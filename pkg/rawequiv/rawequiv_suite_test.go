package rawequiv_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRawEquiv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RawEquiv Suite")
}
