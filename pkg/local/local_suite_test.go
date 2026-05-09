package local_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLocal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pkg/local Suite")
}
