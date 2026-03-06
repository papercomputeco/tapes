package turso_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTurso(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Turso Storer Suite")
}
