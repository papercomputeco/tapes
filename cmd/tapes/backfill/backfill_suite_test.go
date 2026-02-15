package backfillcmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBackfillCommander(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backfill Commander Suite")
}
