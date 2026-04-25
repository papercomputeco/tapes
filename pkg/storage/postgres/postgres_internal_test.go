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

var _ = Describe("integer conversions", func() {
	It("handles both 32-bit and 64-bit integer values", func() {
		Expect(postgres.InterfaceInt32ForTest(int32(42))).To(Equal(int32(42)))
		Expect(postgres.InterfaceInt32ForTest(int64(42))).To(Equal(int32(42)))
		Expect(postgres.InterfaceInt64ForTest(int64(42))).To(Equal(int64(42)))
		Expect(postgres.InterfaceInt64ForTest(int32(42))).To(Equal(int64(42)))
	})
})
