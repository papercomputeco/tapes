package sqlitepath

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ResolveSQLitePath", func() {
	var (
		origHome    string
		origXDG     string
		origTapesDB string
		origTapesSQ string
		origCwd     string
	)

	BeforeEach(func() {
		origHome = os.Getenv("HOME")
		origXDG = os.Getenv("XDG_DATA_HOME")
		origTapesDB = os.Getenv("TAPES_DB")
		origTapesSQ = os.Getenv("TAPES_SQLITE")
		var err error
		origCwd, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.Setenv("HOME", origHome)).To(Succeed())
		Expect(os.Setenv("XDG_DATA_HOME", origXDG)).To(Succeed())
		Expect(os.Setenv("TAPES_DB", origTapesDB)).To(Succeed())
		Expect(os.Setenv("TAPES_SQLITE", origTapesSQ)).To(Succeed())
		Expect(os.Chdir(origCwd)).To(Succeed())
	})

	It("prefers TAPES_SQLITE when set", func() {
		Expect(os.Setenv("TAPES_SQLITE", "/tmp/custom.db")).To(Succeed())
		Expect(os.Setenv("TAPES_DB", "")).To(Succeed())

		path, err := ResolveSQLitePath("")
		Expect(err).NotTo(HaveOccurred())
		Expect(path).To(Equal("/tmp/custom.db"))
	})

	It("resolves ~/.tapes/tapes.db when present", func() {
		homeDir, err := os.MkdirTemp("", "tapes-home-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = os.RemoveAll(homeDir)
		})

		tmpDir, err := os.MkdirTemp("", "tapes-cwd-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = os.RemoveAll(tmpDir)
		})

		Expect(os.Setenv("HOME", homeDir)).To(Succeed())
		Expect(os.Setenv("XDG_DATA_HOME", "")).To(Succeed())
		Expect(os.Setenv("TAPES_DB", "")).To(Succeed())
		Expect(os.Setenv("TAPES_SQLITE", "")).To(Succeed())
		Expect(os.Chdir(tmpDir)).To(Succeed())

		dbPath := filepath.Join(homeDir, ".tapes", "tapes.db")
		Expect(os.MkdirAll(filepath.Dir(dbPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(dbPath, []byte("test"), 0o644)).To(Succeed())

		path, err := ResolveSQLitePath("")
		Expect(err).NotTo(HaveOccurred())
		Expect(path).To(Equal(dbPath))
	})
})
