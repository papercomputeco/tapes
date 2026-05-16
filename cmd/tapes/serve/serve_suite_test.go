package servecmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestServe(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Serve Suite")
}
