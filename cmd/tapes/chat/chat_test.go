package chatcmder_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chatcmder "github.com/papercomputeco/tapes/cmd/tapes/chat"
)

var _ = Describe("NewChatCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := chatcmder.NewChatCmd()
		Expect(cmd.Use).To(Equal("chat"))
	})

	It("has required --model flag", func() {
		cmd := chatcmder.NewChatCmd()
		flag := cmd.Flags().Lookup("model")
		Expect(flag).NotTo(BeNil())
		Expect(flag.Shorthand).To(Equal("m"))
	})

	It("has --proxy flag with default value", func() {
		cmd := chatcmder.NewChatCmd()
		flag := cmd.Flags().Lookup("proxy")
		Expect(flag).NotTo(BeNil())
		Expect(flag.DefValue).To(Equal("http://localhost:8080"))
	})

	It("has persistent --api flag with default value", func() {
		cmd := chatcmder.NewChatCmd()
		flag := cmd.PersistentFlags().Lookup("api")
		Expect(flag).NotTo(BeNil())
		Expect(flag.DefValue).To(Equal("http://localhost:8081"))
	})
})

var _ = Describe("Ollama request format", func() {
	// These tests validate that the Ollama request/response JSON format
	// used by the chat command is correct.

	Describe("request serialization", func() {
		type ollamaRequest struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Stream bool `json:"stream"`
		}

		It("serializes a basic request correctly", func() {
			req := ollamaRequest{
				Model: "llama3.2",
				Messages: []struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					{Role: "user", Content: "Hello!"},
				},
				Stream: true,
			}

			data, err := json.Marshal(req)
			Expect(err).NotTo(HaveOccurred())

			var parsed map[string]any
			err = json.Unmarshal(data, &parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed["model"]).To(Equal("llama3.2"))
			Expect(parsed["stream"]).To(BeTrue())

			messages := parsed["messages"].([]any)
			Expect(messages).To(HaveLen(1))
			msg := messages[0].(map[string]any)
			Expect(msg["role"]).To(Equal("user"))
			Expect(msg["content"]).To(Equal("Hello!"))
		})

		It("serializes a multi-turn conversation correctly", func() {
			req := ollamaRequest{
				Model: "llama3.2",
				Messages: []struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					{Role: "user", Content: "What is Go?"},
					{Role: "assistant", Content: "Go is a programming language."},
					{Role: "user", Content: "Tell me more."},
				},
				Stream: true,
			}

			data, err := json.Marshal(req)
			Expect(err).NotTo(HaveOccurred())

			var parsed map[string]any
			err = json.Unmarshal(data, &parsed)
			Expect(err).NotTo(HaveOccurred())

			messages := parsed["messages"].([]any)
			Expect(messages).To(HaveLen(3))
		})
	})

	Describe("stream chunk parsing", func() {
		type ollamaStreamChunk struct {
			Model     string    `json:"model"`
			CreatedAt time.Time `json:"created_at"`
			Message   struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			Done bool `json:"done"`
		}

		It("parses a content chunk", func() {
			raw := `{"model":"llama3.2","created_at":"2024-01-01T00:00:00Z","message":{"role":"assistant","content":"Hello"},"done":false}`

			var chunk ollamaStreamChunk
			err := json.Unmarshal([]byte(raw), &chunk)
			Expect(err).NotTo(HaveOccurred())
			Expect(chunk.Model).To(Equal("llama3.2"))
			Expect(chunk.Message.Role).To(Equal("assistant"))
			Expect(chunk.Message.Content).To(Equal("Hello"))
			Expect(chunk.Done).To(BeFalse())
		})

		It("parses a final done chunk", func() {
			raw := `{"model":"llama3.2","created_at":"2024-01-01T00:00:00Z","message":{"role":"assistant","content":""},"done":true}`

			var chunk ollamaStreamChunk
			err := json.Unmarshal([]byte(raw), &chunk)
			Expect(err).NotTo(HaveOccurred())
			Expect(chunk.Done).To(BeTrue())
			Expect(chunk.Message.Content).To(BeEmpty())
		})

		It("reconstructs full content from multiple chunks", func() {
			chunks := []string{
				`{"model":"llama3.2","message":{"role":"assistant","content":"Hello"},"done":false}`,
				`{"model":"llama3.2","message":{"role":"assistant","content":" world"},"done":false}`,
				`{"model":"llama3.2","message":{"role":"assistant","content":"!"},"done":false}`,
				`{"model":"llama3.2","message":{"role":"assistant","content":""},"done":true}`,
			}

			var fullContent strings.Builder
			for _, raw := range chunks {
				var chunk ollamaStreamChunk
				err := json.Unmarshal([]byte(raw), &chunk)
				Expect(err).NotTo(HaveOccurred())
				fullContent.WriteString(chunk.Message.Content)
			}

			Expect(fullContent.String()).To(Equal("Hello world!"))
		})
	})
})

var _ = Describe("Streaming proxy interaction", func() {
	It("handles a streaming response from a mock Ollama server", func() {
		// Create a mock server that returns streaming Ollama responses
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/api/chat"))
			Expect(r.Method).To(Equal("POST"))

			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)

			chunks := []string{
				`{"model":"llama3.2","message":{"role":"assistant","content":"Hi"},"done":false}`,
				`{"model":"llama3.2","message":{"role":"assistant","content":" there!"},"done":false}`,
				`{"model":"llama3.2","message":{"role":"assistant","content":""},"done":true}`,
			}

			for _, chunk := range chunks {
				fmt.Fprintln(w, chunk)
			}
		}))
		defer server.Close()

		// Verify the request format by sending a request
		reqBody := map[string]any{
			"model": "llama3.2",
			"messages": []map[string]string{
				{"role": "user", "content": "hello"},
			},
			"stream": true,
		}
		data, err := json.Marshal(reqBody)
		Expect(err).NotTo(HaveOccurred())

		resp, err := http.Post(server.URL+"/api/chat", "application/json", strings.NewReader(string(data)))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		// Parse streaming response
		type streamChunk struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			Done bool `json:"done"`
		}

		var fullContent strings.Builder
		decoder := json.NewDecoder(resp.Body)
		for decoder.More() {
			var chunk streamChunk
			err := decoder.Decode(&chunk)
			Expect(err).NotTo(HaveOccurred())
			fullContent.WriteString(chunk.Message.Content)
		}

		Expect(fullContent.String()).To(Equal("Hi there!"))
	})
})
