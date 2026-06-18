// Package openai implements pkg/embeddings's Embedder client for OpenAI's
// embeddings API.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/vector"
)

const (
	// DefaultEmbeddingModel is OpenAI's default Tapes embedding model.
	DefaultEmbeddingModel = "text-embedding-3-large"

	// DefaultBaseURL is the OpenAI API base URL.
	DefaultBaseURL = "https://api.openai.com/v1"

	maxErrorBodyBytes = 4 << 10
)

// Embedder wraps OpenAI's embeddings API.
type Embedder struct {
	baseURL    string
	model      string
	apiKey     string
	dimensions uint
	httpClient *http.Client
}

// EmbedderConfig holds configuration for the OpenAI embedder.
type EmbedderConfig struct {
	// BaseURL is the OpenAI-compatible API URL. It may include or omit /v1.
	// Defaults to DefaultBaseURL if empty.
	BaseURL string

	// Model is the embedding model to use.
	// Defaults to DefaultEmbeddingModel if empty.
	Model string

	// APIKey is the OpenAI API key. Defaults to OPENAI_API_KEY if empty.
	APIKey string

	// Dimensions optionally requests shortened embeddings for text-embedding-3
	// models. Leave 0 to use the model default.
	Dimensions uint
}

type embedRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	EncodingFormat string `json:"encoding_format,omitempty"`
	Dimensions     uint   `json:"dimensions,omitempty"`
}

type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// NewEmbedder creates a new embedder using OpenAI's embeddings API.
func NewEmbedder(cfg EmbedderConfig) (*Embedder, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	baseURL, err := normalizeBaseURL(baseURL)
	if err != nil {
		return nil, err
	}

	model := cfg.Model
	if model == "" {
		model = DefaultEmbeddingModel
	}

	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey == "" {
		return nil, errors.New("OPENAI_API_KEY is required for openai embeddings")
	}

	return &Embedder{
		baseURL:    baseURL,
		model:      model,
		apiKey:     apiKey,
		dimensions: cfg.Dimensions,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

// Embed converts text into a vector embedding.
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embedRequest{
		Model:          e.model,
		Input:          text,
		EncodingFormat: "float",
		Dimensions:     e.dimensions,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("%w: marshaling request: %w", vector.ErrEmbedding, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/embeddings", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("%w: creating request: %w", vector.ErrEmbedding, err)
	}
	req.Header.Set("Authorization", "Bearer "+e.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		// No response was received (connection refused, timeout, DNS); a
		// transport failure is transient, so surface it as retryable.
		return nil, &embeddings.APIError{Message: err.Error(), Transient: true}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return nil, newAPIError(resp.StatusCode, body)
	}

	var embedResp embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("%w: decoding response: %w", vector.ErrEmbedding, err)
	}

	if len(embedResp.Data) == 0 || len(embedResp.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("%w: no embeddings returned", vector.ErrEmbedding)
	}

	return embedResp.Data[0].Embedding, nil
}

// Close releases resources held by the embedder.
func (e *Embedder) Close() error {
	return nil
}

// newAPIError builds a structured *embeddings.APIError from a non-200
// response. It parses OpenAI's error envelope ({"error":{message,type,code}})
// for the machine code and message, falling back to the raw body when the
// envelope is absent, and extracts the reported token count for oversize
// rejections so the caller can size its chunks.
func newAPIError(status int, body []byte) *embeddings.APIError {
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	message := strings.TrimSpace(string(body))
	code := ""
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Message != "" {
		message = envelope.Error.Message
		code = envelope.Error.Code
	}
	return &embeddings.APIError{
		Status:          status,
		Code:            code,
		Message:         message,
		RequestedTokens: embeddings.ParseRequestedTokens(message),
	}
}

func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimRight(raw, "/")
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid OpenAI embedding base URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid OpenAI embedding base URL: %q", raw)
	}
	if !pathContainsSegment(u.Path, "v1") {
		u.Path = strings.TrimRight(u.Path, "/") + "/v1"
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func pathContainsSegment(path string, segment string) bool {
	return slices.Contains(strings.Split(path, "/"), segment)
}

var _ embeddings.Embedder = (*Embedder)(nil)
