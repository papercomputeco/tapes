package local_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/local"
)

var _ = Describe("local", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "local-test-*")
		Expect(err).NotTo(HaveOccurred())

		tmpDir, err = filepath.EvalSymlinks(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("EnsureLocalPostgresDir", func() {
		It("creates a postgres subdirectory beneath the override dir", func() {
			overrideDir := filepath.Join(tmpDir, ".tapes")

			result, err := local.EnsureLocalPostgresDir(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(overrideDir, "postgres")))
			Expect(result).To(BeADirectory())
		})

		It("falls back to ~/.tapes/postgres when no override or local dir exists", func() {
			homeDir := filepath.Join(tmpDir, "home")
			Expect(os.MkdirAll(filepath.Join(homeDir, ".tapes"), 0o755)).To(Succeed())

			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(tmpDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Chdir(origDir)).To(Succeed()) })

			origHome := os.Getenv("HOME")
			Expect(os.Setenv("HOME", homeDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Setenv("HOME", origHome)).To(Succeed()) })

			result, err := local.EnsureLocalPostgresDir("")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(homeDir, ".tapes", "postgres")))
			Expect(result).To(BeADirectory())
		})
	})

	Describe("PostgresDSN", func() {
		It("substitutes the configured port", func() {
			Expect(local.PostgresDSN(5555)).To(ContainSubstring(":5555/"))
		})

		It("defaults to the standard Postgres port when zero", func() {
			Expect(local.PostgresDSN(0)).To(ContainSubstring(":5432/"))
		})
	})

	Describe("IsLocalDefaultHost", func() {
		It("returns true for empty DSN", func() {
			Expect(local.IsLocalDefaultHost("")).To(BeTrue())
		})

		It("returns true for localhost on the default port", func() {
			Expect(local.IsLocalDefaultHost("postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable")).To(BeTrue())
		})

		It("returns true for 127.0.0.1 on the default port", func() {
			Expect(local.IsLocalDefaultHost("postgres://u:p@127.0.0.1:5432/db")).To(BeTrue())
		})

		It("returns true when the port is omitted (defaults to 5432)", func() {
			Expect(local.IsLocalDefaultHost("postgres://u:p@localhost/db")).To(BeTrue())
		})

		It("returns false for a non-local host", func() {
			Expect(local.IsLocalDefaultHost("postgres://u:p@staging.example.com:5432/db")).To(BeFalse())
		})

		It("returns false for a non-default port on localhost", func() {
			Expect(local.IsLocalDefaultHost("postgres://u:p@localhost:6543/db")).To(BeFalse())
		})

		It("returns false for an unparseable DSN", func() {
			Expect(local.IsLocalDefaultHost("not a url")).To(BeFalse())
		})
	})
})
