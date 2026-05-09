package localcmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewLocalCmd", func() {
	It("registers up/down/status subcommands", func() {
		cmd := NewLocalCmd()
		names := make([]string, 0, len(cmd.Commands()))
		for _, sub := range cmd.Commands() {
			names = append(names, sub.Name())
		}
		Expect(names).To(ConsistOf("up", "down", "status"))
	})

	It("defaults Postgres and Ollama ports", func() {
		cmd := NewLocalCmd()
		Expect(cmd.Flags().Lookup("postgres-port").DefValue).To(Equal("5432"))
		Expect(cmd.Flags().Lookup("ollama-port").DefValue).To(Equal("11434"))
	})
})
