package proxycmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProxyKafkaE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Serve Proxy Suite")
}
