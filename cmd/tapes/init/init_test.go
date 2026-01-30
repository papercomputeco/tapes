package initcmder_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
)

var _ = Describe("NewInitCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := initcmder.NewInitCmd()
		Expect(cmd.Use).To(Equal("init"))
	})

	It("accepts zero arguments", func() {
		cmd := initcmder.NewInitCmd()
		err := cmd.Args(cmd, []string{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects any arguments", func() {
		cmd := initcmder.NewInitCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Init command execution", func() {
	var (
		tmpDir  string
		origDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-init-test-*")
		Expect(err).NotTo(HaveOccurred())

		origDir, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())

		err = os.Chdir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := os.Chdir(origDir)
		Expect(err).NotTo(HaveOccurred())
		os.RemoveAll(tmpDir)
	})

	It("creates a .tapes directory in the current directory", func() {
		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		info, err := os.Stat(filepath.Join(tmpDir, ".tapes"))
		Expect(err).NotTo(HaveOccurred())
		Expect(info.IsDir()).To(BeTrue())
	})

	It("succeeds when .tapes directory already exists", func() {
		err := os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)
		Expect(err).NotTo(HaveOccurred())

		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		info, err := os.Stat(filepath.Join(tmpDir, ".tapes"))
		Expect(err).NotTo(HaveOccurred())
		Expect(info.IsDir()).To(BeTrue())
	})

	It("does not overwrite existing contents when already initialized", func() {
		tapesDir := filepath.Join(tmpDir, ".tapes")
		err := os.MkdirAll(tapesDir, 0o755)
		Expect(err).NotTo(HaveOccurred())

		// Write a file into the existing .tapes dir
		testFile := filepath.Join(tapesDir, "checkout.json")
		err = os.WriteFile(testFile, []byte(`{"hash":"abc"}`), 0o644)
		Expect(err).NotTo(HaveOccurred())

		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		// Verify the existing file is still there
		data, err := os.ReadFile(testFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"hash":"abc"}`))
	})
})
