package inmemory_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInmemory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inmemory Storer Suite")
}
