package backfill_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBackfill(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backfill Suite")
}
