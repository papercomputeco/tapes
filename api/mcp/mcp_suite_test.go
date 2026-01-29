package mcp_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMcp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MCP Suite")
}
