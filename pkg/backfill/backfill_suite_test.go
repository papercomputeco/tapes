package backfill_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/testdb"
)

func TestBackfill(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backfill Suite")
}

// testPostgresDSN is the Dagger-provided Postgres the DB-backed specs in
// this package run against. Empty when the suite is not running under
// `make test`; the specs that need it skip rather than fail, so the
// package's pure specs still run on their own.
var testPostgresDSN string

var _ = BeforeSuite(func() {
	ctx := context.Background()
	suite, err := testdb.AcquireSuite(ctx)
	if errors.Is(err, testdb.ErrNotConfigured) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
	testPostgresDSN = suite.DSN()

	DeferCleanup(func() {
		Expect(suite.Close(ctx)).To(Succeed())
	})
})
