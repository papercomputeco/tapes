package qdrant_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestQdrant(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Qdrant Vector Suite")
}
