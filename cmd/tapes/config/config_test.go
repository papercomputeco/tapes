package configcmder_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	configcmder "github.com/papercomputeco/tapes/cmd/tapes/config"
)

var _ = Describe("NewConfigCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := configcmder.NewConfigCmd()
		Expect(cmd.Use).To(Equal("config"))
	})

	It("has set, get, and list subcommands", func() {
		cmd := configcmder.NewConfigCmd()
		cmds := cmd.Commands()
		subcommands := make([]string, 0, len(cmds))
		for _, sub := range cmds {
			subcommands = append(subcommands, sub.Name())
		}
		Expect(subcommands).To(ContainElements("set", "get", "list"))
	})
})

var _ = Describe("Config command execution", func() {
	var (
		tmpDir  string
		origDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-config-test-*")
		Expect(err).NotTo(HaveOccurred())

		origDir, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())

		// Create a local .tapes dir so the manager picks it up
		err = os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)
		Expect(err).NotTo(HaveOccurred())

		err = os.Chdir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := os.Chdir(origDir)
		Expect(err).NotTo(HaveOccurred())
		os.RemoveAll(tmpDir)
	})

	Describe("set subcommand", func() {
		It("sets a config value successfully", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"set", "proxy.provider", "anthropic"})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			// Verify the config file was created
			_, err = os.Stat(filepath.Join(tmpDir, ".tapes", "config.toml"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects unknown keys", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"set", "invalid_key", "value"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})

		It("requires exactly two arguments", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"set", "proxy.provider"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})

		It("rejects zero arguments", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"set"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})

		It("rejects invalid uint values", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"set", "embedding.dimensions", "not-a-number"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("get subcommand", func() {
		It("gets a previously set value", func() {
			// First set a value
			setCmd := configcmder.NewConfigCmd()
			setCmd.SetArgs([]string{"set", "proxy.provider", "anthropic"})
			err := setCmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			// Then get it
			getCmd := configcmder.NewConfigCmd()
			getCmd.SetArgs([]string{"get", "proxy.provider"})
			err = getCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})

		It("runs without error for unset key", func() {
			getCmd := configcmder.NewConfigCmd()
			getCmd.SetArgs([]string{"get", "proxy.provider"})
			err := getCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects unknown keys", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"get", "invalid_key"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})

		It("requires exactly one argument", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"get"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("list subcommand", func() {
		It("runs without error when no config exists", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"list"})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})

		It("runs without error when config has values", func() {
			// Set some values first
			setCmd := configcmder.NewConfigCmd()
			setCmd.SetArgs([]string{"set", "proxy.provider", "anthropic"})
			err := setCmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"list"})
			err = cmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects any arguments", func() {
			cmd := configcmder.NewConfigCmd()
			cmd.SetArgs([]string{"list", "extra"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
		})
	})
})
