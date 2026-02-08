package bedrock_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBedrock(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bedrock Provider Suite")
}
