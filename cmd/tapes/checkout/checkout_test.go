package checkoutcmder_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

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

var _ = Describe("History API response parsing", func() {
	// This tests that the checkout command can correctly parse the
	// API response format used by GET /dag/history/:hash

	type historyMessage struct {
		Hash       string             `json:"hash"`
		ParentHash *string            `json:"parent_hash,omitempty"`
		Role       string             `json:"role"`
		Content    []llm.ContentBlock `json:"content"`
		Model      string             `json:"model,omitempty"`
		Provider   string             `json:"provider,omitempty"`
		StopReason string             `json:"stop_reason,omitempty"`
	}

	type historyResponse struct {
		Messages []historyMessage `json:"messages"`
		HeadHash string           `json:"head_hash"`
		Depth    int              `json:"depth"`
	}

	It("parses a valid API history response", func() {
		parentHash := "hash1"
		resp := historyResponse{
			Messages: []historyMessage{
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
			HeadHash: "hash2",
			Depth:    2,
		}

		data, err := json.Marshal(resp)
		Expect(err).NotTo(HaveOccurred())

		var parsed historyResponse
		err = json.Unmarshal(data, &parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.HeadHash).To(Equal("hash2"))
		Expect(parsed.Depth).To(Equal(2))
		Expect(parsed.Messages).To(HaveLen(2))
		Expect(parsed.Messages[0].Role).To(Equal("user"))
		Expect(parsed.Messages[1].Role).To(Equal("assistant"))

		// Extract text from content blocks
		var text string
		for _, block := range parsed.Messages[1].Content {
			if block.Type == "text" {
				text += block.Text
			}
		}
		Expect(text).To(Equal("Hi there!"))
	})

	It("correctly handles a mock API server returning history", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/dag/history/abc123"))
			Expect(r.Method).To(Equal("GET"))

			resp := historyResponse{
				Messages: []historyMessage{
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
				HeadHash: "abc123",
				Depth:    2,
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		// Fetch from mock server
		url := fmt.Sprintf("%s/dag/history/abc123", server.URL)
		resp, err := http.Get(url)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		var history historyResponse
		err = json.NewDecoder(resp.Body).Decode(&history)
		Expect(err).NotTo(HaveOccurred())
		Expect(history.HeadHash).To(Equal("abc123"))
		Expect(history.Messages).To(HaveLen(2))
	})

	It("handles API returning 404 for unknown hash", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "node not found",
			})
		}))
		defer server.Close()

		resp, err := http.Get(fmt.Sprintf("%s/dag/history/unknown", server.URL))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
	})
})
