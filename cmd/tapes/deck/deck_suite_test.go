package deckcmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDeckCommander(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Deck Commander Suite")
}
