package eventstream_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEventstream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Eventstream Suite")
}
