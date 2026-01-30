package statuscmder_test

import (
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
	"github.com/papercomputeco/tapes/pkg/dotdir"
)

var _ = Describe("NewStatusCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := statuscmder.NewStatusCmd()
		Expect(cmd.Use).To(Equal("status"))
	})

	It("accepts zero arguments", func() {
		cmd := statuscmder.NewStatusCmd()
		err := cmd.Args(cmd, []string{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects any arguments", func() {
		cmd := statuscmder.NewStatusCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Status command execution", func() {
	var (
		tmpDir  string
		origDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-status-test-*")
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

	It("runs without error when no checkout state exists", func() {
		// Create a local .tapes dir so the manager picks it up
		err := os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)
		Expect(err).NotTo(HaveOccurred())

		cmd := statuscmder.NewStatusCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs without error when checkout state exists", func() {
		tapesDir := filepath.Join(tmpDir, ".tapes")
		err := os.MkdirAll(tapesDir, 0o755)
		Expect(err).NotTo(HaveOccurred())

		state := &dotdir.CheckoutState{
			Hash: "abc123def456",
			Messages: []dotdir.CheckoutMessage{
				{Role: "user", Content: "Hello!"},
				{Role: "assistant", Content: "Hi there!"},
			},
		}

		data, err := json.MarshalIndent(state, "", "  ")
		Expect(err).NotTo(HaveOccurred())
		err = os.WriteFile(filepath.Join(tapesDir, "checkout.json"), data, 0o644)
		Expect(err).NotTo(HaveOccurred())

		cmd := statuscmder.NewStatusCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})
})
