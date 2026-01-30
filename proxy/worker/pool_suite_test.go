package worker

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWorkerPool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker Pool Suite")
}
