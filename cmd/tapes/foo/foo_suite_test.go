package foocmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFooCommander(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Foo Commander Suite")
}
