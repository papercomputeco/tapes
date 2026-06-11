package config_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/config"
)

var _ = Describe("RedactDSN", func() {
	It("masks the password in a URL DSN", func() {
		redacted := config.RedactDSN("postgresql://tapes:s3cr3t@yeazel-db-rw.tenant-cnq93:5432/tapes")
		Expect(redacted).NotTo(ContainSubstring("s3cr3t"))
		Expect(redacted).To(ContainSubstring("tapes:xxxxx@yeazel-db-rw.tenant-cnq93:5432/tapes"))
	})

	It("masks the password in a keyword/value DSN", func() {
		redacted := config.RedactDSN("host=localhost port=5432 user=tapes password=s3cr3t dbname=tapes")
		Expect(redacted).NotTo(ContainSubstring("s3cr3t"))
		Expect(redacted).To(ContainSubstring("password=xxxxx"))
	})

	It("returns URL DSNs without credentials unchanged", func() {
		dsn := "postgresql://yeazel-db-rw.tenant-cnq93:5432/tapes"
		Expect(config.RedactDSN(dsn)).To(Equal(dsn))
	})

	It("returns URL DSNs with a user but no password unchanged", func() {
		dsn := "postgresql://tapes@yeazel-db-rw.tenant-cnq93:5432/tapes"
		Expect(config.RedactDSN(dsn)).To(Equal(dsn))
	})

	It("masks a password passed as a query parameter", func() {
		redacted := config.RedactDSN("postgresql://tapes@yeazel-db-rw.tenant-cnq93:5432/tapes?password=s3cr3t&sslmode=require")
		Expect(redacted).NotTo(ContainSubstring("s3cr3t"))
		Expect(redacted).To(ContainSubstring("password=xxxxx"))
		Expect(redacted).To(ContainSubstring("sslmode=require"))
	})

	It("masks both userinfo and query parameter passwords", func() {
		redacted := config.RedactDSN("postgresql://tapes:s3cr3t@host:5432/tapes?password=als0secret")
		Expect(redacted).NotTo(ContainSubstring("s3cr3t"))
		Expect(redacted).NotTo(ContainSubstring("als0secret"))
	})

	It("masks URL-shaped strings that do not parse as URLs", func() {
		redacted := config.RedactDSN("postgresql://tapes:pass with space@host:5432/tapes")
		Expect(redacted).NotTo(ContainSubstring("pass with space"))
	})

	It("masks query parameter passwords in URL-shaped strings that do not parse", func() {
		redacted := config.RedactDSN("postgresql://tapes:pass with space@host:5432/tapes?password=s3cr3t&sslmode=require")
		Expect(redacted).NotTo(ContainSubstring("s3cr3t"))
		Expect(redacted).To(ContainSubstring("sslmode=require"))
	})

	It("returns non-DSN strings unchanged", func() {
		Expect(config.RedactDSN("not a dsn")).To(Equal("not a dsn"))
	})
})
