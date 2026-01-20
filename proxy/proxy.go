// Package proxy provides an LLM inference proxy that stores conversations in a Merkle DAG.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Proxy is a client, LLM inference proxy that instruments storing sessions as Merkle DAGs.
// The proxy is designed to be stateless as it builds nodes from incoming messages
// and stores them in a content-addressable merkle.Storer.
type Proxy struct {
	config     Config
	storer     merkle.Storer
	logger     *zap.Logger
	httpClient *http.Client
	server     *fiber.App
	detector   *provider.Detector
}

// New creates a new Proxy.
func New(config Config, logger *zap.Logger) (*Proxy, error) {
	var storer merkle.Storer
	var err error

	if config.DBPath != "" {
		storer, err = merkle.NewSQLiteStorer(config.DBPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		logger.Info("using SQLite storage", zap.String("path", config.DBPath))
	} else {
		storer = merkle.NewMemoryStorer()
		logger.Info("using in-memory storage")
	}

	app := fiber.New(fiber.Config{
		// Disable startup message for cleaner logs
		DisableStartupMessage: true,
		// Enable streaming
		StreamRequestBody: true,
	})

	p := &Proxy{
		config:   config,
		storer:   storer,
		logger:   logger,
		server:   app,
		detector: provider.NewDetector(),
		httpClient: &http.Client{
			// LLM requests can be slow, especially with thinking blocks
			Timeout: 5 * time.Minute,
		},
	}

	// Register transparent proxy route - forwards any path to upstream
	app.All("/*", p.handleProxy)

	// Health check (takes precedence due to explicit registration)
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(map[string]string{"status": "ok"})
	})

	// DAG inspection endpoints
	app.Get("/dag/stats", p.handleDAGStats)
	app.Get("/dag/node/:hash", p.handleGetNode)
	app.Get("/dag/history", p.handleListHistories)
	app.Get("/dag/history/:hash", p.handleGetHistory)

	return p, nil
}

// Run starts the proxy server on the given listening address
func (p *Proxy) Run() error {
	p.logger.Info("starting proxy server",
		zap.String("listen", p.config.ListenAddr),
		zap.String("upstream", p.config.UpstreamURL),
	)

	return p.server.Listen(p.config.ListenAddr)
}

// Close shuts down the proxy and releases resources.
func (p *Proxy) Close() error {
	return p.storer.Close()
}

// handleProxy is a transparent proxy handler that forwards requests to upstream
// and stores conversation turns in the Merkle DAG.
func (p *Proxy) handleProxy(c *fiber.Ctx) error {
	startTime := time.Now()

	// Get the request path and method
	path := c.Path()
	method := c.Method()

	// Only process POST requests that look like chat/completion endpoints
	body := c.Body()
	isChatRequest := method == "POST" && len(body) > 0

	// Detect provider and parse request if this looks like a chat request
	var detectedProvider provider.Provider
	var parsedReq *llm.ChatRequest
	if isChatRequest {
		detectedProvider = p.detector.Detect(body)
		var err error
		parsedReq, err = detectedProvider.ParseRequest(body)
		if err != nil {
			p.logger.Warn("failed to parse request",
				zap.Error(err),
				zap.String("provider", detectedProvider.Name()),
			)
		} else {
			p.logger.Debug("detected provider",
				zap.String("provider", detectedProvider.Name()),
				zap.String("model", parsedReq.Model),
				zap.Int("message_count", len(parsedReq.Messages)),
			)
		}
	}

	// Determine if streaming (check parsed request or raw JSON)
	streaming := false
	if parsedReq != nil && parsedReq.Stream != nil {
		streaming = *parsedReq.Stream
	} else if isChatRequest {
		// Fallback: check raw JSON for stream field
		var streamCheck struct {
			Stream *bool `json:"stream"`
		}
		if err := json.Unmarshal(body, &streamCheck); err == nil && streamCheck.Stream != nil {
			streaming = *streamCheck.Stream
		}
	}

	if streaming && isChatRequest {
		return p.handleStreamingProxy(c, path, body, detectedProvider, parsedReq, startTime)
	}

	return p.handleNonStreamingProxy(c, path, method, body, detectedProvider, parsedReq, startTime)
}

// handleNonStreamingProxy handles non-streaming requests.
func (p *Proxy) handleNonStreamingProxy(c *fiber.Ctx, path, method string, body []byte, detectedProvider provider.Provider, parsedReq *llm.ChatRequest, startTime time.Time) error {
	// Build upstream URL
	upstreamURL := p.config.UpstreamURL + path

	// Create upstream request
	var reqBody io.Reader
	if len(body) > 0 {
		reqBody = bytes.NewReader(body)
	}

	httpReq, err := http.NewRequestWithContext(c.Context(), method, upstreamURL, reqBody)
	if err != nil {
		p.logger.Error("failed to create upstream request", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "internal error"})
	}

	// Copy relevant headers
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		// Skip hop-by-hop headers
		if k != "Connection" && k != "Host" {
			httpReq.Header.Set(k, string(value))
		}
	})

	p.logger.Debug("forwarding request to upstream",
		zap.String("method", method),
		zap.String("url", upstreamURL),
	)

	// Make the request
	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		p.logger.Error("upstream request failed", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{Error: "upstream request failed"})
	}
	defer httpResp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		p.logger.Error("failed to read upstream response", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{Error: "failed to read upstream response"})
	}

	// Copy response headers
	for k, v := range httpResp.Header {
		if k != "Connection" && k != "Transfer-Encoding" {
			c.Set(k, strings.Join(v, ", "))
		}
	}

	// If this was a chat request and we have a provider, try to parse and store
	if detectedProvider != nil && parsedReq != nil && httpResp.StatusCode == http.StatusOK {
		parsedResp, err := detectedProvider.ParseResponse(respBody)
		if err != nil {
			p.logger.Warn("failed to parse response",
				zap.Error(err),
				zap.String("provider", detectedProvider.Name()),
			)
		} else {
			p.logger.Debug("received response from upstream",
				zap.String("model", parsedResp.Model),
				zap.String("provider", detectedProvider.Name()),
				zap.Duration("duration", time.Since(startTime)),
			)

			// Store in DAG
			headHash, err := p.storeConversationTurn(c.Context(), detectedProvider.Name(), parsedReq, parsedResp)
			if err != nil {
				p.logger.Error("failed to store conversation", zap.Error(err))
			} else {
				p.logger.Info("conversation stored",
					zap.String("head_hash", truncate(headHash, 16)),
					zap.String("provider", detectedProvider.Name()),
				)
			}
		}
	}

	// Return response to client
	return c.Status(httpResp.StatusCode).Send(respBody)
}

// handleStreamingProxy handles streaming requests.
func (p *Proxy) handleStreamingProxy(c *fiber.Ctx, path string, body []byte, detectedProvider provider.Provider, parsedReq *llm.ChatRequest, startTime time.Time) error {
	// Build upstream URL
	upstreamURL := p.config.UpstreamURL + path

	httpReq, err := http.NewRequestWithContext(c.Context(), "POST", upstreamURL, bytes.NewReader(body))
	if err != nil {
		p.logger.Error("failed to create upstream request", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "internal error"})
	}

	// Copy relevant headers
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		if k != "Connection" && k != "Host" {
			httpReq.Header.Set(k, string(value))
		}
	})

	p.logger.Debug("forwarding streaming request to upstream",
		zap.String("url", upstreamURL),
	)

	// Make the request
	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		p.logger.Error("upstream request failed", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{Error: "upstream request failed"})
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(httpResp.Body)
		p.logger.Error("upstream returned error",
			zap.Int("status", httpResp.StatusCode),
			zap.String("body", string(respBody)),
		)
		return c.Status(httpResp.StatusCode).Send(respBody)
	}

	// Copy response headers
	for k, v := range httpResp.Header {
		if k != "Connection" && k != "Transfer-Encoding" {
			c.Set(k, strings.Join(v, ", "))
		}
	}

	// Use Fiber's streaming response
	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		// Accumulate chunks for storage
		var allChunks [][]byte
		var fullContent strings.Builder

		scanner := bufio.NewScanner(httpResp.Body)
		// Increase buffer size for large chunks
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)

		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			// Store chunk for later parsing
			chunkCopy := make([]byte, len(line))
			copy(chunkCopy, line)
			allChunks = append(allChunks, chunkCopy)

			// Try to extract content incrementally for logging
			// This is a best-effort extraction from the raw chunk
			var chunkData map[string]any
			if err := json.Unmarshal(line, &chunkData); err == nil {
				// Try common content paths
				if msg, ok := chunkData["message"].(map[string]any); ok {
					if content, ok := msg["content"].(string); ok {
						fullContent.WriteString(content)
					}
				} else if choices, ok := chunkData["choices"].([]any); ok && len(choices) > 0 {
					if choice, ok := choices[0].(map[string]any); ok {
						if delta, ok := choice["delta"].(map[string]any); ok {
							if content, ok := delta["content"].(string); ok {
								fullContent.WriteString(content)
							}
						}
					}
				}
			}

			// Write chunk to client
			w.Write(line)
			w.Write([]byte("\n"))
			w.Flush()
		}

		if err := scanner.Err(); err != nil {
			p.logger.Error("error reading stream", zap.Error(err))
		}

		// After streaming completes, try to reconstruct and store the full response
		if detectedProvider != nil && parsedReq != nil && len(allChunks) > 0 {
			p.logger.Debug("streaming complete",
				zap.String("content_preview", truncate(fullContent.String(), 200)),
				zap.Int("chunk_count", len(allChunks)),
				zap.Duration("duration", time.Since(startTime)),
			)

			// Try to parse the final chunk or reconstruct response
			finalResp := p.reconstructStreamedResponse(detectedProvider, allChunks, fullContent.String())
			if finalResp != nil {
				headHash, err := p.storeConversationTurn(context.Background(), detectedProvider.Name(), parsedReq, finalResp)
				if err != nil {
					p.logger.Error("failed to store conversation", zap.Error(err))
				} else {
					p.logger.Info("conversation stored",
						zap.String("head_hash", truncate(headHash, 16)),
						zap.String("provider", detectedProvider.Name()),
					)
				}
			}
		}
	}))

	return nil
}

// reconstructStreamedResponse attempts to build a ChatResponse from accumulated stream chunks.
func (p *Proxy) reconstructStreamedResponse(prov provider.Provider, chunks [][]byte, fullContent string) *llm.ChatResponse {
	// Try parsing the last chunk as it often contains final metadata
	if len(chunks) > 0 {
		lastChunk := chunks[len(chunks)-1]
		resp, err := prov.ParseResponse(lastChunk)
		if err == nil && resp != nil {
			// If the last chunk has minimal content, supplement with accumulated content
			if resp.Message.GetText() == "" && fullContent != "" {
				resp.Message = llm.NewTextMessage("assistant", fullContent)
			}
			return resp
		}
	}

	// Fallback: construct a minimal response from accumulated content
	if fullContent != "" {
		return &llm.ChatResponse{
			Message:   llm.NewTextMessage("assistant", fullContent),
			Done:      true,
			CreatedAt: time.Now(),
		}
	}

	return nil
}

// storeConversationTurn stores a request-response pair in the Merkle DAG.
func (p *Proxy) storeConversationTurn(ctx context.Context, providerName string, req *llm.ChatRequest, resp *llm.ChatResponse) (string, error) {
	var parent *merkle.Node

	// Store each message from the request as nodes
	for _, msg := range req.Messages {
		content := map[string]any{
			"type":     "message",
			"role":     msg.Role,
			"content":  msg.Content, // Now stores []ContentBlock
			"model":    req.Model,
			"provider": providerName,
		}

		node := merkle.NewNode(content, parent)
		if err := p.storer.Put(ctx, node); err != nil {
			return "", fmt.Errorf("storing message node: %w", err)
		}

		p.logger.Debug("stored message in DAG",
			zap.String("hash", truncate(node.Hash, 16)),
			zap.String("role", msg.Role),
			zap.String("content_preview", truncate(msg.GetText(), 50)),
		)

		parent = node
	}

	// Store the response message
	responseContent := map[string]any{
		"type":        "message",
		"role":        resp.Message.Role,
		"content":     resp.Message.Content, // Now stores []ContentBlock
		"model":       resp.Model,
		"provider":    providerName,
		"stop_reason": resp.StopReason,
	}

	// Add usage metrics if present
	if resp.Usage != nil {
		responseContent["usage"] = map[string]any{
			"prompt_tokens":      resp.Usage.PromptTokens,
			"completion_tokens":  resp.Usage.CompletionTokens,
			"total_tokens":       resp.Usage.TotalTokens,
			"total_duration_ns":  resp.Usage.TotalDurationNs,
			"prompt_duration_ns": resp.Usage.PromptDurationNs,
		}
	}

	responseNode := merkle.NewNode(responseContent, parent)
	if err := p.storer.Put(ctx, responseNode); err != nil {
		return "", fmt.Errorf("storing response node: %w", err)
	}

	p.logger.Debug("stored response in DAG",
		zap.String("hash", truncate(responseNode.Hash, 16)),
		zap.String("content_preview", truncate(resp.Message.GetText(), 50)),
	)

	return responseNode.Hash, nil
}

// handleDAGStats returns statistics about the DAG.
func (p *Proxy) handleDAGStats(c *fiber.Ctx) error {
	ctx := c.Context()

	nodes, err := p.storer.List(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list nodes"})
	}

	roots, err := p.storer.Roots(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get roots"})
	}

	leaves, err := p.storer.Leaves(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get leaves"})
	}

	stats := map[string]any{
		"total_nodes": len(nodes),
		"root_count":  len(roots),
		"leaf_count":  len(leaves),
	}

	return c.JSON(stats)
}

// handleGetNode returns a single node by its hash.
func (p *Proxy) handleGetNode(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if hash == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "hash parameter required"})
	}

	node, err := p.storer.Get(c.Context(), hash)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "node not found"})
	}

	return c.JSON(node)
}

// HistoryResponse contains the conversation history for a given node.
type HistoryResponse struct {
	// Messages in chronological order (oldest first, up to and including the requested node)
	Messages []HistoryMessage `json:"messages"`
	// HeadHash is the hash of the node that was requested
	HeadHash string `json:"head_hash"`
	// Depth is the number of messages in the history
	Depth int `json:"depth"`
}

// HistoryMessage represents a message in the conversation history.
type HistoryMessage struct {
	Hash       string         `json:"hash"`
	ParentHash *string        `json:"parent_hash,omitempty"`
	Role       string         `json:"role"`
	Content    any            `json:"content"` // Can be string or []ContentBlock
	Model      string         `json:"model,omitempty"`
	Provider   string         `json:"provider,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// handleListHistories returns all conversation histories (one per leaf node).
func (p *Proxy) handleListHistories(c *fiber.Ctx) error {
	ctx := c.Context()

	leaves, err := p.storer.Leaves(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get leaves"})
	}

	histories := make([]HistoryResponse, 0, len(leaves))
	for _, leaf := range leaves {
		history, err := p.buildHistory(ctx, leaf.Hash)
		if err != nil {
			p.logger.Warn("failed to build history for leaf", zap.String("hash", leaf.Hash), zap.Error(err))
			continue
		}
		histories = append(histories, *history)
	}

	return c.JSON(map[string]any{
		"count":     len(histories),
		"histories": histories,
	})
}

// handleGetHistory returns the full conversation history leading up to a given node.
func (p *Proxy) handleGetHistory(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if hash == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "hash parameter required"})
	}

	history, err := p.buildHistory(c.Context(), hash)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "node not found"})
	}

	return c.JSON(history)
}

// buildHistory constructs a HistoryResponse for the given node hash.
func (p *Proxy) buildHistory(ctx context.Context, hash string) (*HistoryResponse, error) {
	ancestry, err := p.storer.Ancestry(ctx, hash)
	if err != nil {
		return nil, err
	}

	messages := make([]HistoryMessage, len(ancestry))
	for i, node := range ancestry {
		idx := len(ancestry) - 1 - i

		msg := HistoryMessage{
			Hash:       node.Hash,
			ParentHash: node.ParentHash,
		}

		if content, ok := node.Content.(map[string]any); ok {
			if role, ok := content["role"].(string); ok {
				msg.Role = role
			}
			// Content can now be []ContentBlock or string
			msg.Content = content["content"]
			if model, ok := content["model"].(string); ok {
				msg.Model = model
			}
			if provider, ok := content["provider"].(string); ok {
				msg.Provider = provider
			}
			// Copy additional metadata
			metadata := make(map[string]any)
			for k, v := range content {
				if k != "role" && k != "content" && k != "model" && k != "type" && k != "provider" {
					metadata[k] = v
				}
			}
			if len(metadata) > 0 {
				msg.Metadata = metadata
			}
		}

		messages[idx] = msg
	}

	return &HistoryResponse{
		Messages: messages,
		HeadHash: hash,
		Depth:    len(messages),
	}, nil
}

func truncate(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
