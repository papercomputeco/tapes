package initcmder_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Init Command Suite")
}
