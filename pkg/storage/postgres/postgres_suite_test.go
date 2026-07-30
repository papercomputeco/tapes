package postgres_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/testdb"
)

var testPostgresDSN string

var _ = BeforeSuite(func() {
	ctx := context.Background()
	suite, err := testdb.AcquireSuite(ctx)
	Expect(err).NotTo(HaveOccurred())
	testPostgresDSN = suite.DSN()

	DeferCleanup(func() {
		Expect(suite.Close(ctx)).To(Succeed())
	})
})

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PostgreSQL Storer Suite")
}
