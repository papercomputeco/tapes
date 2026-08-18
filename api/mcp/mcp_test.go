package mcp_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/mcp"
)

var _ = Describe("MCP Server", func() {
	Describe("NewServer", func() {
		It("creates a usable server from an empty config", func() {
			s, err := mcp.NewServer(mcp.Config{})
			Expect(err).NotTo(HaveOccurred())
			Expect(s.Handler()).NotTo(BeNil())
		})
	})
})
