package servecmder

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
)

var _ = Describe("combined cassette configuration", func() {
	newStack := func(directory string, arguments ...string) (*Stack, *cobra.Command) {
		cmd := &cobra.Command{}
		cmd.SetContext(context.Background())
		cmd.Flags().String("config-dir", directory, "")
		stack := &Stack{}
		stack.AddFlags(cmd, ServeFlags)
		Expect(cmd.ParseFlags(arguments)).To(Succeed())
		return stack, cmd
	}

	It("loads URL lists from config", func() {
		directory := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(directory, "config.toml"), []byte(`cassettes = ["http://one/openapi", "http://two/spec"]`), 0o600)).To(Succeed())
		stack, cmd := newStack(directory)
		Expect(stack.Resolve(cmd, ServeFlags)).To(Succeed())
		Expect(stack.CassetteSources).To(Equal([]string{"http://one/openapi", "http://two/spec"}))
	})

	It("accepts comma-separated and repeated flags and replaces environment and config", func() {
		directory := GinkgoT().TempDir()
		GinkgoT().Setenv("TAPES_CASSETTES", "http://environment/openapi")
		Expect(os.WriteFile(filepath.Join(directory, "config.toml"), []byte(`cassettes = ["http://configured/openapi"]`), 0o600)).To(Succeed())
		stack, cmd := newStack(directory, "--cassettes=http://one/openapi,http://two/openapi", "--cassettes=http://three/openapi")
		Expect(stack.Resolve(cmd, ServeFlags)).To(Succeed())
		Expect(stack.CassetteSources).To(Equal([]string{"http://one/openapi", "http://two/openapi", "http://three/openapi"}))
	})

	It("loads CSV sources from the environment and replaces config", func() {
		directory := GinkgoT().TempDir()
		GinkgoT().Setenv("TAPES_CASSETTES", "http://one/openapi,http://two/openapi")
		Expect(os.WriteFile(filepath.Join(directory, "config.toml"), []byte(`cassettes = ["http://configured/openapi"]`), 0o600)).To(Succeed())
		stack, cmd := newStack(directory)
		Expect(stack.Resolve(cmd, ServeFlags)).To(Succeed())
		Expect(stack.CassetteSources).To(Equal([]string{"http://one/openapi", "http://two/openapi"}))
	})
})
