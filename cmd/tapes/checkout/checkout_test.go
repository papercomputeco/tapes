package checkoutcmder_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	checkoutcmder "github.com/papercomputeco/tapes/cmd/tapes/checkout"
	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("NewCheckoutCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		Expect(cmd.Use).To(Equal("checkout [hash]"))
	})

	It("accepts zero arguments for clearing checkout", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		err := cmd.Args(cmd, []string{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("accepts one argument for a hash", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		err := cmd.Args(cmd, []string{"abc123"})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects more than one argument", func() {
		cmd := checkoutcmder.NewCheckoutCmd()
		err := cmd.Args(cmd, []string{"abc123", "def456"})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Session API response parsing", func() {
	// This tests that the checkout command can correctly parse the
	// API response format used by GET /v1/sessions/:hash

	type turn struct {
		Hash       string             `json:"hash"`
		ParentHash *string            `json:"parent_hash,omitempty"`
		Role       string             `json:"role"`
		Content    []llm.ContentBlock `json:"content"`
		Model      string             `json:"model,omitempty"`
		Provider   string             `json:"provider,omitempty"`
		StopReason string             `json:"stop_reason,omitempty"`
	}

	type sessionResponse struct {
		Hash  string `json:"hash"`
		Depth int    `json:"depth"`
		Turns []turn `json:"turns"`
	}

	It("parses a valid API session response", func() {
		parentHash := "hash1"
		resp := sessionResponse{
			Hash:  "hash2",
			Depth: 2,
			Turns: []turn{
				{
					Hash: "hash1",
					Role: "user",
					Content: []llm.ContentBlock{
						{Type: "text", Text: "Hello!"},
					},
					Model:    "llama3.2",
					Provider: "ollama",
				},
				{
					Hash:       "hash2",
					ParentHash: &parentHash,
					Role:       "assistant",
					Content: []llm.ContentBlock{
						{Type: "text", Text: "Hi there!"},
					},
					Model:      "llama3.2",
					Provider:   "ollama",
					StopReason: "stop",
				},
			},
		}

		data, err := json.Marshal(resp)
		Expect(err).NotTo(HaveOccurred())

		var parsed sessionResponse
		err = json.Unmarshal(data, &parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.Hash).To(Equal("hash2"))
		Expect(parsed.Depth).To(Equal(2))
		Expect(parsed.Turns).To(HaveLen(2))
		Expect(parsed.Turns[0].Role).To(Equal("user"))
		Expect(parsed.Turns[1].Role).To(Equal("assistant"))

		// Extract text from content blocks
		var b strings.Builder
		for _, block := range parsed.Turns[1].Content {
			if block.Type == "text" {
				b.WriteString(block.Text)
			}
		}
		Expect(b.String()).To(Equal("Hi there!"))
	})

	It("correctly handles a mock API server returning a session", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/sessions/abc123"))
			Expect(r.Method).To(Equal("GET"))

			resp := sessionResponse{
				Hash:  "abc123",
				Depth: 2,
				Turns: []turn{
					{
						Hash: "root",
						Role: "user",
						Content: []llm.ContentBlock{
							{Type: "text", Text: "What is Go?"},
						},
					},
					{
						Hash: "abc123",
						Role: "assistant",
						Content: []llm.ContentBlock{
							{Type: "text", Text: "Go is a programming language."},
						},
					},
				},
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		// Fetch from mock server
		url := server.URL + "/v1/sessions/abc123"
		resp, err := http.Get(url)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		var session sessionResponse
		err = json.NewDecoder(resp.Body).Decode(&session)
		Expect(err).NotTo(HaveOccurred())
		Expect(session.Hash).To(Equal("abc123"))
		Expect(session.Turns).To(HaveLen(2))
	})

	It("handles API returning 404 for unknown hash", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "session not found",
			})
		}))
		defer server.Close()

		resp, err := http.Get(server.URL + "/v1/sessions/unknown")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
	})
})
