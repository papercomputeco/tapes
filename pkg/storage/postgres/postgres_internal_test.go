package postgres_test

import (
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("toMigrateDSN", func() {
	It("preserves sslmode from key-value DSNs", func() {
		dsn := postgres.ToMigrateDSNForTest("host=localhost dbname=tapes sslmode=disable")

		u, err := url.Parse(dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(u.Scheme).To(Equal("pgx5"))
		Expect(u.Query().Get("sslmode")).To(Equal("disable"))
	})
})
