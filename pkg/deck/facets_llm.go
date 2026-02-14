package deck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/credentials"
)

const providerOllama = "ollama"

// LLMCallerConfig holds configuration for creating an LLM caller.
type LLMCallerConfig struct {
	Provider string               // "openai", "anthropic", or "ollama"
	Model    string               // e.g. "gpt-4o-mini", "claude-haiku-4-5-20251001"
	APIKey   string               // explicit API key (highest priority)
	BaseURL  string               // override base URL
	CredMgr  *credentials.Manager // credentials from tapes auth
}

// HasLLMCredentials checks whether an API key can be resolved from the config
// without creating a caller. Used for auto-enabling insights.
func HasLLMCredentials(cfg LLMCallerConfig) bool {
	if cfg.APIKey != "" {
		return true
	}
	provider := strings.ToLower(cfg.Provider)
	if provider == providerOllama {
		return true
	}
	if cfg.CredMgr != nil {
		if key := resolveAPIKeyFromCreds(cfg.CredMgr, provider); key != "" {
			return true
		}
	}
	if key := resolveAPIKeyFromEnv(provider); key != "" {
		return true
	}
	return false
}

// NewLLMCaller creates a LLMCallFunc based on the provided configuration.
// Resolution order for API key:
//  1. Explicit APIKey in config
//  2. credentials.Manager (from tapes auth)
//  3. Environment variables (OPENAI_API_KEY / ANTHROPIC_API_KEY)
//  4. Fall back to Ollama at localhost:11434
func NewLLMCaller(cfg LLMCallerConfig) (LLMCallFunc, error) {
	provider := strings.ToLower(cfg.Provider)
	model := cfg.Model

	// Resolve API key: explicit > tapes auth > env vars
	apiKey := cfg.APIKey
	if apiKey == "" && cfg.CredMgr != nil {
		apiKey = resolveAPIKeyFromCreds(cfg.CredMgr, provider)
	}
	if apiKey == "" {
		apiKey = resolveAPIKeyFromEnv(provider)
	}

	// If no key found and provider is not explicitly ollama, fall back to ollama
	if apiKey == "" && provider != providerOllama {
		log.Printf("facets: no API key found for %s, falling back to ollama", provider)
		provider = providerOllama
	}

	switch provider {
	case providerOpenAI, "":
		if model == "" {
			model = "gpt-4o-mini"
		}
		baseURL := cfg.BaseURL
		if baseURL == "" {
			baseURL = "https://api.openai.com"
		}
		return newOpenAICaller(apiKey, model, baseURL), nil

	case providerAnthropic:
		if model == "" {
			model = "claude-haiku-4-5-20251001"
		}
		baseURL := cfg.BaseURL
		if baseURL == "" {
			baseURL = "https://api.anthropic.com"
		}
		return newAnthropicCaller(apiKey, model, baseURL), nil

	case providerOllama:
		if model == "" {
			model = "llama3.2"
		}
		baseURL := cfg.BaseURL
		if baseURL == "" {
			baseURL = "http://localhost:11434"
		}
		return newOllamaCaller(model, baseURL), nil

	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}

func resolveAPIKeyFromCreds(mgr *credentials.Manager, provider string) string {
	if mgr == nil {
		return ""
	}
	key, err := mgr.GetKey(provider)
	if err != nil || key != "" {
		return key
	}
	// If provider-specific key not found, try others
	if provider == providerOpenAI || provider == "" {
		if key, err = mgr.GetKey(providerAnthropic); err == nil && key != "" {
			return key
		}
	}
	if provider == providerAnthropic {
		if key, err = mgr.GetKey(providerOpenAI); err == nil && key != "" {
			return key
		}
	}
	return ""
}

func resolveAPIKeyFromEnv(provider string) string {
	switch provider {
	case providerAnthropic:
		return os.Getenv("ANTHROPIC_API_KEY")
	case providerOpenAI, "":
		return os.Getenv("OPENAI_API_KEY")
	default:
		// Try both
		if key := os.Getenv("OPENAI_API_KEY"); key != "" {
			return key
		}
		return os.Getenv("ANTHROPIC_API_KEY")
	}
}

// --- OpenAI caller ---

type openAIRequest struct {
	Model          string            `json:"model"`
	Messages       []openAIMessage   `json:"messages"`
	ResponseFormat *openAIRespFormat `json:"response_format,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRespFormat struct {
	Type string `json:"type"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func newOpenAICaller(apiKey, model, baseURL string) LLMCallFunc {
	return func(ctx context.Context, prompt string) (string, error) {
		reqBody := openAIRequest{
			Model: model,
			Messages: []openAIMessage{
				{Role: "user", Content: prompt},
			},
			ResponseFormat: &openAIRespFormat{Type: "json_object"},
		}

		data, err := json.Marshal(reqBody)
		if err != nil {
			return "", fmt.Errorf("marshal request: %w", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("openai request: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("openai API error (status %d): %s", resp.StatusCode, string(body))
		}

		var result openAIResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("unmarshal response: %w", err)
		}

		if result.Error != nil {
			return "", fmt.Errorf("openai error: %s", result.Error.Message)
		}

		if len(result.Choices) == 0 {
			return "", errors.New("openai returned no choices")
		}

		return result.Choices[0].Message.Content, nil
	}
}

// --- Anthropic caller ---

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func newAnthropicCaller(apiKey, model, baseURL string) LLMCallFunc {
	return func(ctx context.Context, prompt string) (string, error) {
		reqBody := anthropicRequest{
			Model:     model,
			MaxTokens: 1024,
			Messages: []anthropicMessage{
				{Role: "user", Content: prompt + "\n\nReturn ONLY valid JSON, no markdown or extra text."},
			},
		}

		data, err := json.Marshal(reqBody)
		if err != nil {
			return "", fmt.Errorf("marshal request: %w", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("anthropic request: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("anthropic API error (status %d): %s", resp.StatusCode, string(body))
		}

		var result anthropicResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("unmarshal response: %w", err)
		}

		if result.Error != nil {
			return "", fmt.Errorf("anthropic error: %s", result.Error.Message)
		}

		if len(result.Content) == 0 {
			return "", errors.New("anthropic returned no content")
		}

		return result.Content[0].Text, nil
	}
}

// --- Ollama caller ---

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Format   string              `json:"format"`
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatResponse struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
	Done bool `json:"done"`
}

func newOllamaCaller(model, baseURL string) LLMCallFunc {
	return func(ctx context.Context, prompt string) (string, error) {
		reqBody := ollamaChatRequest{
			Model: model,
			Messages: []ollamaChatMessage{
				{Role: "user", Content: prompt},
			},
			Stream: false,
			Format: "json",
		}

		data, err := json.Marshal(reqBody)
		if err != nil {
			return "", fmt.Errorf("marshal request: %w", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/chat", bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("ollama request: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("ollama API error (status %d): %s", resp.StatusCode, string(body))
		}

		var result ollamaChatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("unmarshal response: %w", err)
		}

		return result.Message.Content, nil
	}
}
