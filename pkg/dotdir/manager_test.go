package dotdir_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

var _ = Describe("dotdir", func() {
	var tmpDir string
	var m *dotdir.Manager

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "dotdir-test-*")
		Expect(err).NotTo(HaveOccurred())

		// Resolve symlinks so paths match filepath.Abs results
		// (e.g. on macOS /var -> /private/var).
		tmpDir, err = filepath.EvalSymlinks(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		m = dotdir.NewManager()
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("NewManager", func() {
		It("creates a new manager", func() {
			Expect(m).ToNot(BeNil())
		})
	})

	Describe("Target", func() {
		It("creates the directory if it doesn't exist", func() {
			dir := filepath.Join(tmpDir, "newdir")
			result, err := m.Target(dir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(dir))

			info, err := os.Stat(dir)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.IsDir()).To(BeTrue())
		})

		It("returns existing directory without error", func() {
			result, err := m.Target(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(tmpDir))
		})

		It("returns the override dir even when a local .tapes dir exists", func() {
			// Create a local .tapes dir in the tmpDir
			localTapes := filepath.Join(tmpDir, ".tapes")
			Expect(os.Mkdir(localTapes, 0o755)).To(Succeed())

			// Change to tmpDir so the local dir is discoverable
			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(tmpDir)).To(Succeed())
			DeferCleanup(func() { os.Chdir(origDir) })

			overrideDir := filepath.Join(tmpDir, "override")
			result, err := m.Target(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(overrideDir))
		})

		It("returns the local .tapes dir when it exists and no override is provided", func() {
			// Create a local .tapes dir in the tmpDir
			localTapes := filepath.Join(tmpDir, ".tapes")
			Expect(os.Mkdir(localTapes, 0o755)).To(Succeed())

			// Change to tmpDir so the local dir is discoverable
			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(tmpDir)).To(Succeed())
			DeferCleanup(func() { os.Chdir(origDir) })

			result, err := m.Target("")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(localTapes))
		})

		It("returns empty string when no local .tapes dir exists, no home .tapes dir exists, and no override is provided", func() {
			// Ensure we're in a directory without a .tapes subdir
			emptyDir := filepath.Join(tmpDir, "empty")
			Expect(os.Mkdir(emptyDir, 0o755)).To(Succeed())

			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(emptyDir)).To(Succeed())
			DeferCleanup(func() { os.Chdir(origDir) })

			// Override HOME so that ~/.tapes from the real home dir is not found.
			origHome := os.Getenv("HOME")
			Expect(os.Setenv("HOME", emptyDir)).To(Succeed())
			DeferCleanup(func() { os.Setenv("HOME", origHome) })

			result, err := m.Target("")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})
	})
})
