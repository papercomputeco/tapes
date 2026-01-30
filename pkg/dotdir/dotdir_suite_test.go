package dotdir_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDotdir(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dotdir Suite")
}
