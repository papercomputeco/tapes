package localcmder

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

	Describe("resolveLocalTapesDir", func() {
		It("uses the override config dir when provided", func() {
			overrideDir := filepath.Join(tmpDir, "override")

			result, err := resolveLocalTapesDir(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(overrideDir))
			Expect(result).To(BeADirectory())
		})

		It("falls back to ~/.tapes and creates it when no target exists", func() {
			homeDir := filepath.Join(tmpDir, "home")
			Expect(os.MkdirAll(homeDir, 0o755)).To(Succeed())

			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(tmpDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Chdir(origDir)).To(Succeed()) })

			origHome := os.Getenv("HOME")
			Expect(os.Setenv("HOME", homeDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Setenv("HOME", origHome)).To(Succeed()) })

			result, err := resolveLocalTapesDir("")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(homeDir, ".tapes")))
			Expect(result).To(BeADirectory())
		})
	})

	Describe("ensureLocalPostgresDir", func() {
		It("creates a postgres subdirectory beneath the resolved tapes dir", func() {
			overrideDir := filepath.Join(tmpDir, ".tapes")

			result, err := ensureLocalPostgresDir(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(overrideDir, postgresDirName)))
			Expect(result).To(BeADirectory())
		})
	})
})
