package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMCPTool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MCP Tool Cassette Suite")
}

var _ = Describe("mcp-tool cassette", func() {
	It("advertises and serves the ping tool", func() {
		document := openAPIDocument(defaultName)
		paths := document["paths"].(map[string]any)
		operation := paths["/api/mcp-tool/ping"].(map[string]any)["post"].(map[string]any)
		extension := operation["x-tapes-mcp"].(map[string]any)
		Expect(extension["name"]).To(Equal("ping"))

		request := httptest.NewRequest(http.MethodPost, "/api/mcp-tool/ping", bytes.NewBufferString(`{"ping":"ping"}`))
		response := httptest.NewRecorder()
		routes(defaultName).ServeHTTP(response, request)

		Expect(response.Code).To(Equal(http.StatusOK))
		var body map[string]string
		Expect(json.Unmarshal(response.Body.Bytes(), &body)).To(Succeed())
		Expect(body).To(Equal(map[string]string{"pong": "pong"}))
	})

	It("rejects arguments outside the advertised schema", func() {
		request := httptest.NewRequest(http.MethodPost, "/api/mcp-tool/ping", bytes.NewBufferString(`{"ping":"nope"}`))
		response := httptest.NewRecorder()
		routes(defaultName).ServeHTTP(response, request)
		Expect(response.Code).To(Equal(http.StatusBadRequest))
	})
})
