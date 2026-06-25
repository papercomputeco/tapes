package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/credentials"
)

// The LLM caller used for skill extraction. Self-contained so the
// skill pipeline has no dependency on the deck TUI's packages.

const (
	providerOpenAI    = "openai"
	providerAnthropic = "anthropic"
	providerOllama    = "ollama"

	llmCallTimeout = 30 * time.Second

	// llmCallRetries is the number of additional attempts on a transient
	// provider failure (HTTP 429/502/503/504 or a transport error). One
	// retry is enough to ride out a brief rate-limit or upstream blip
	// without risking the handler's overall time budget.
	llmCallRetries  = 1
	llmRetryBackoff = 500 * time.Millisecond
)

// ErrNoAPIKey is returned by NewLLMCaller when no key resolves for the
// provider. Skill generation reuses the platform's search/embedding
// credential, so this means the tenant has search/embedding disabled —
// the handler maps it to a clear 4xx rather than an opaque 500.
var ErrNoAPIKey = errors.New("no API key for skill-generation provider")

// LLMCallFunc is the signature for an LLM inference call.
type LLMCallFunc func(ctx context.Context, prompt string) (string, error)

// LLMCallerConfig holds configuration for creating an LLM caller.
type LLMCallerConfig struct {
	Provider string               // "openai", "anthropic", or "ollama"
	Model    string               // e.g. "gpt-4o-mini", "claude-haiku-4-5-20251001"
	APIKey   string               // explicit API key (highest priority)
	BaseURL  string               // override base URL
	CredMgr  *credentials.Manager // credentials from tapes auth
}

// NewLLMCaller creates an LLMCallFunc based on the provided configuration.
// Resolution order for API key:
//  1. Explicit APIKey in config
//  2. credentials.Manager (from tapes auth)
//  3. Environment variables (OPENAI_API_KEY / ANTHROPIC_API_KEY)
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

	// Require an API key for non-ollama providers
	if apiKey == "" && provider != providerOllama {
		envVar := envVarForProvider(provider)
		return nil, fmt.Errorf("%w: provider %q — set %s, use --api-key, or run 'tapes auth'", ErrNoAPIKey, provider, envVar)
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
	if err == nil && key != "" {
		return key
	}
	// Provider-specific key errored or was absent — fall through to the
	// cross-provider fallbacks rather than returning empty early.
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

func envVarForProvider(provider string) string {
	switch provider {
	case providerAnthropic:
		return "ANTHROPIC_API_KEY"
	case providerOpenAI, "":
		return "OPENAI_API_KEY"
	default:
		return "OPENAI_API_KEY or ANTHROPIC_API_KEY"
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

		body, err := postJSON(ctx, baseURL+"/v1/chat/completions", reqBody, map[string]string{
			"Authorization": "Bearer " + apiKey,
		})
		if err != nil {
			return "", fmt.Errorf("openai request: %w", err)
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

		body, err := postJSON(ctx, baseURL+"/v1/messages", reqBody, map[string]string{
			"x-api-key":         apiKey,
			"anthropic-version": "2023-06-01",
		})
		if err != nil {
			return "", fmt.Errorf("anthropic request: %w", err)
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

		body, err := postJSON(ctx, baseURL+"/api/chat", reqBody, nil)
		if err != nil {
			return "", fmt.Errorf("ollama request: %w", err)
		}

		var result ollamaChatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("unmarshal response: %w", err)
		}

		return result.Message.Content, nil
	}
}

// postJSON issues a JSON POST and returns the response body, retrying a
// transient provider failure (429/502/503/504 or a transport blip) up to
// llmCallRetries times. One timeout spans every attempt, so retries never
// extend the handler's time budget past llmCallTimeout; a deadline or
// cancellation stops retrying immediately.
func postJSON(ctx context.Context, url string, reqBody any, headers map[string]string) ([]byte, error) {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, llmCallTimeout)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt <= llmCallRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(llmRetryBackoff):
			}
		}

		body, status, err := doPostJSON(ctx, url, data, headers)
		if err != nil {
			lastErr = err
			// A blown deadline or cancellation has no budget left to
			// retry; a live-context transport blip does.
			if ctx.Err() != nil {
				return nil, err
			}
			continue
		}
		if status == http.StatusOK {
			return body, nil
		}
		lastErr = fmt.Errorf("API error (status %d): %s", status, string(body))
		if !isRetryableStatus(status) {
			return nil, lastErr
		}
	}
	return nil, lastErr
}

// doPostJSON performs a single POST attempt, returning the body and HTTP
// status separately so the caller can classify retryable statuses.
func doPostJSON(ctx context.Context, url string, data []byte, headers map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", err)
	}
	return body, resp.StatusCode, nil
}

// isRetryableStatus reports whether an HTTP status is a transient
// provider condition worth one more attempt.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}
