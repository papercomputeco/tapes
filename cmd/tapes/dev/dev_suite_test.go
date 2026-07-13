package devcmder_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDev(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dev Command Suite")
}
