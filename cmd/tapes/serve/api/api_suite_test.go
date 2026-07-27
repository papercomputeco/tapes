package apicmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPICommander(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Standalone API Command Suite")
}
