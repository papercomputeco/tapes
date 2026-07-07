package reflection_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestReflection(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Reflection Suite")
}
