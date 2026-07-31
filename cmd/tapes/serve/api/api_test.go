package apicmder

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("standalone API cassette configuration", func() {
	It("exposes a comma-separated and repeatable StringSlice flag", func() {
		command := NewAPICmd()
		flag := command.Flags().Lookup("cassettes")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Value.Type()).To(Equal("stringSlice"))
		Expect(command.Flags().Set("cassettes", "http://one/openapi,http://two/spec")).To(Succeed())
		Expect(command.Flags().Set("cassettes", "http://three/openapi")).To(Succeed())
		values, err := command.Flags().GetStringSlice("cassettes")
		Expect(err).NotTo(HaveOccurred())
		Expect(values).To(Equal([]string{"http://one/openapi", "http://two/spec", "http://three/openapi"}))
	})

	It("resolves config sources through PreRunE and lets the flag replace them", func() {
		directory := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(directory, "config.toml"), []byte(`cassettes = ["http://config/openapi"]`), 0o600)).To(Succeed())

		commander := &apiCommander{flags: apiFlags}
		command := newAPICmd(commander)
		command.Flags().String("config-dir", directory, "")
		Expect(command.PreRunE(command, nil)).To(Succeed())
		Expect(commander.cassetteSources).To(Equal([]string{"http://config/openapi"}))

		commander = &apiCommander{flags: apiFlags}
		command = newAPICmd(commander)
		command.Flags().String("config-dir", directory, "")
		Expect(command.Flags().Set("cassettes", "http://flag/one,http://flag/two")).To(Succeed())
		Expect(command.PreRunE(command, nil)).To(Succeed())
		Expect(commander.cassetteSources).To(Equal([]string{"http://flag/one", "http://flag/two"}))
	})

	DescribeTable("rejects invalid effective sources before server startup",
		func(source, problem string) {
			commander := &apiCommander{flags: apiFlags}
			command := newAPICmd(commander)
			command.Flags().String("config-dir", GinkgoT().TempDir(), "")
			Expect(command.Flags().Set("cassettes", source)).To(Succeed())

			Expect(command.PreRunE(command, nil)).To(MatchError(And(
				ContainSubstring("cassettes[0]"),
				ContainSubstring(problem),
			)))
		},
		Entry("without a scheme", "cassette.internal/openapi", "must use the http or https scheme"),
		Entry("with a non-HTTP scheme", "ftp://cassette.internal/openapi", "must use the http or https scheme"),
		Entry("without an authority", "http:///openapi", "must include a host"),
		Entry("with only a port", "http://:8080/openapi", "must include a host"),
		Entry("when malformed", "http://cassette.internal/%zz", "must be a valid URL"),
	)
})
