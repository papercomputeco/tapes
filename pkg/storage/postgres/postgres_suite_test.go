package postgres_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/testdb"
)

var suiteLock *testdb.SuiteLock

var _ = BeforeSuite(func() {
	var err error
	suiteLock, err = testdb.AcquireSuiteLock(context.Background())
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	Expect(suiteLock.Close(context.Background())).To(Succeed())
})

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PostgreSQL Storer Suite")
}
