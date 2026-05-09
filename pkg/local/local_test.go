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
})
