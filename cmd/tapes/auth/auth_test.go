package authcmder_test

import (
	"bytes"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"

	authcmder "github.com/papercomputeco/tapes/cmd/tapes/auth"
	"github.com/papercomputeco/tapes/pkg/credentials"
)

var _ = Describe("Auth Command", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "auth-test-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("NewAuthCmd", func() {
		It("creates a command with expected properties", func() {
			cmd := authcmder.NewAuthCmd()
			Expect(cmd.Use).To(Equal("auth [provider]"))
			Expect(cmd.Short).NotTo(BeEmpty())
		})

		It("has --list flag", func() {
			cmd := authcmder.NewAuthCmd()
			flag := cmd.Flags().Lookup("list")
			Expect(flag).NotTo(BeNil())
		})

		It("has --remove flag", func() {
			cmd := authcmder.NewAuthCmd()
			flag := cmd.Flags().Lookup("remove")
			Expect(flag).NotTo(BeNil())
		})
	})

	Describe("--list flag", func() {
		It("shows no credentials when none stored", func() {
			cmd := authcmder.NewAuthCmd()
			out := &bytes.Buffer{}
			cmd.SetOut(out)
			cmd.SetArgs([]string{"--list", "--config-dir", tmpDir})

			cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")

			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})

		It("lists stored credentials", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			err = mgr.SetKey("openai", "sk-test")
			Expect(err).NotTo(HaveOccurred())

			cmd := authcmder.NewAuthCmd()
			out := &bytes.Buffer{}
			cmd.SetOut(out)
			cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
			cmd.SetArgs([]string{"--list", "--config-dir", tmpDir})

			err = cmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("--remove flag", func() {
		It("removes stored credentials", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			err = mgr.SetKey("openai", "sk-test")
			Expect(err).NotTo(HaveOccurred())

			cmd := authcmder.NewAuthCmd()
			out := &bytes.Buffer{}
			cmd.SetOut(out)
			cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
			cmd.SetArgs([]string{"--remove", "openai", "--config-dir", tmpDir})

			err = cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("openai")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(BeEmpty())
		})
	})

	Describe("provider argument validation", func() {
		It("returns error when no provider given", func() {
			cmd := authcmder.NewAuthCmd()
			cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
			cmd.SetArgs([]string{})

			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("provider argument required"))
		})

		It("returns error for unsupported provider", func() {
			cmd := authcmder.NewAuthCmd()
			cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
			cmd.SetIn(bytes.NewBufferString("sk-test\n"))
			cmd.SetArgs([]string{"ollama", "--config-dir", tmpDir})

			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported provider"))
		})
	})

	Describe("shell completion", func() {
		It("provides provider name completions", func() {
			cmd := authcmder.NewAuthCmd()
			completions, directive := cmd.ValidArgsFunction(cmd, []string{}, "")
			Expect(completions).To(ConsistOf("openai", "anthropic"))
			Expect(directive).To(Equal(cobra.ShellCompDirectiveNoFileComp))
		})

		It("provides no completions after first arg", func() {
			cmd := authcmder.NewAuthCmd()
			completions, directive := cmd.ValidArgsFunction(cmd, []string{"openai"}, "")
			Expect(completions).To(BeNil())
			Expect(directive).To(Equal(cobra.ShellCompDirectiveNoFileComp))
		})
	})
})
