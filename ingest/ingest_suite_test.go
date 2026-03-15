package ingest_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIngest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingest Suite")
}
