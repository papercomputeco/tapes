package pgvector_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgvector(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pgvector Vector Suite")
}
