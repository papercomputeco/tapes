package nop_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestNopPublisher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Nop Publisher Suite")
}
