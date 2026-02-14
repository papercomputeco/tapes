package deck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewLLMCaller", func() {
	It("returns an ollama caller when no key is available", func() {
		cfg := LLMCallerConfig{
			Provider: "openai",
			Model:    "gpt-4o-mini",
			APIKey:   "", // no key
		}
		caller, err := NewLLMCaller(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})

	It("returns an error for unsupported provider", func() {
		cfg := LLMCallerConfig{
			Provider: "unsupported",
			APIKey:   "key",
		}
		_, err := NewLLMCaller(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported provider"))
	})

	It("creates an openai caller with explicit key", func() {
		cfg := LLMCallerConfig{
			Provider: "openai",
			Model:    "gpt-4o-mini",
			APIKey:   "test-key",
		}
		caller, err := NewLLMCaller(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})

	It("creates an anthropic caller with explicit key", func() {
		cfg := LLMCallerConfig{
			Provider: "anthropic",
			Model:    "claude-haiku-4-5-20251001",
			APIKey:   "test-key",
		}
		caller, err := NewLLMCaller(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})

	It("creates an ollama caller explicitly", func() {
		cfg := LLMCallerConfig{
			Provider: "ollama",
			Model:    "llama3.2",
		}
		caller, err := NewLLMCaller(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})
})

var _ = Describe("OpenAI caller", func() {
	It("calls the OpenAI API and returns response content", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			Expect(r.Header.Get("Authorization")).To(Equal("Bearer test-key"))
			Expect(r.Header.Get("Content-Type")).To(Equal("application/json"))

			var req openAIRequest
			err := json.NewDecoder(r.Body).Decode(&req)
			Expect(err).NotTo(HaveOccurred())
			Expect(req.Model).To(Equal("gpt-4o-mini"))
			Expect(req.ResponseFormat).NotTo(BeNil())
			Expect(req.ResponseFormat.Type).To(Equal("json_object"))

			resp := openAIResponse{
				Choices: []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				}{
					{Message: struct {
						Content string `json:"content"`
					}{Content: `{"goal_category":"fix_bug"}`}},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		caller := newOpenAICaller("test-key", "gpt-4o-mini", server.URL)
		result, err := caller(context.Background(), "test prompt")
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(ContainSubstring("fix_bug"))
	})

	It("returns error on non-200 status", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":{"message":"invalid key"}}`))
		}))
		defer server.Close()

		caller := newOpenAICaller("bad-key", "gpt-4o-mini", server.URL)
		_, err := caller(context.Background(), "test prompt")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("status 401"))
	})
})

var _ = Describe("Anthropic caller", func() {
	It("calls the Anthropic API and returns response content", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/messages"))
			Expect(r.Header.Get("x-api-key")).To(Equal("test-key"))
			Expect(r.Header.Get("anthropic-version")).To(Equal("2023-06-01"))

			resp := anthropicResponse{
				Content: []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				}{
					{Type: "text", Text: `{"goal_category":"implement_feature"}`},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		caller := newAnthropicCaller("test-key", "claude-haiku-4-5-20251001", server.URL)
		result, err := caller(context.Background(), "test prompt")
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(ContainSubstring("implement_feature"))
	})
})

var _ = Describe("Ollama caller", func() {
	It("calls the Ollama API and returns response content", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/api/chat"))

			var req ollamaChatRequest
			err := json.NewDecoder(r.Body).Decode(&req)
			Expect(err).NotTo(HaveOccurred())
			Expect(req.Stream).To(BeFalse())
			Expect(req.Format).To(Equal("json"))

			resp := ollamaChatResponse{Done: true}
			resp.Message.Content = `{"goal_category":"refactor_code"}`
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer server.Close()

		caller := newOllamaCaller("llama3.2", server.URL)
		result, err := caller(context.Background(), "test prompt")
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(ContainSubstring("refactor_code"))
	})
})
