package deck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Format   string              `json:"format,omitempty"`
}

type ollamaChatResponse struct {
	Message ollamaChatMessage `json:"message"`
	Done    bool              `json:"done"`
	Error   string            `json:"error"`
}

// NewOllamaFacetLLM returns an Ollama-backed LLM call for facet extraction.
func NewOllamaFacetLLM(baseURL, model string) LLMCallFunc {
	return func(ctx context.Context, prompt string) (string, error) {
		request := ollamaChatRequest{
			Model: model,
			Messages: []ollamaChatMessage{
				{Role: "user", Content: prompt},
			},
			Stream: false,
			Format: "json",
		}

		payload, err := json.Marshal(request)
		if err != nil {
			return "", fmt.Errorf("marshal ollama request: %w", err)
		}

		target := strings.TrimRight(baseURL, "/") + "/api/chat"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
		if err != nil {
			return "", fmt.Errorf("create ollama request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 2 * time.Minute}
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("send ollama request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return "", fmt.Errorf("ollama status %d: %s", resp.StatusCode, string(body))
		}

		var response ollamaChatResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return "", fmt.Errorf("decode ollama response: %w", err)
		}
		if response.Error != "" {
			return "", fmt.Errorf("ollama error: %s", response.Error)
		}

		return response.Message.Content, nil
	}
}
