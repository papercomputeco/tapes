package synccmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSyncCommander(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sync Commander Suite")
}
