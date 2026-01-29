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
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// Proxy is a client, LLM inference proxy that instruments storing sessions as Merkle DAGs.
// The proxy is designed to be stateless as it builds nodes from incoming messages
// and stores them in a content-addressable merkle.Storer.
type Proxy struct {
	config     Config
	driver     storage.Driver
	logger     *zap.Logger
	httpClient *http.Client
	server     *fiber.App
	provider   provider.Provider
}

// New creates a new Proxy.
// The storer is injected to allow sharing with other components (e.g., the API server).
// Returns an error if the configured provider type is not recognized.
func New(config Config, driver storage.Driver, logger *zap.Logger) (*Proxy, error) {
	prov, err := provider.New(config.ProviderType)
	if err != nil {
		return nil, fmt.Errorf("could not create new provider: %w", err)
	}

	app := fiber.New(fiber.Config{
		// Disable startup message for cleaner logs
		DisableStartupMessage: true,
		// Enable streaming
		StreamRequestBody: true,
	})

	p := &Proxy{
		config:   config,
		driver:   driver,
		logger:   logger,
		server:   app,
		provider: prov,
		httpClient: &http.Client{
			// LLM requests can be slow, especially with thinking blocks
			Timeout: 5 * time.Minute,
		},
	}

	// Register transparent proxy route - forwards any path to upstream
	app.All("/*", p.handleProxy)

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
	return p.driver.Close()
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

	// Parse request using configured provider
	var parsedReq *llm.ChatRequest
	if isChatRequest {
		var err error
		parsedReq, err = p.provider.ParseRequest(body)
		if err != nil {
			p.logger.Warn("failed to parse request",
				zap.Error(err),
				zap.String("provider", p.provider.Name()),
			)
		} else {
			p.logger.Debug("parsed request",
				zap.String("provider", p.provider.Name()),
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
		return p.handleStreamingProxy(c, path, body, parsedReq, startTime)
	}

	return p.handleNonStreamingProxy(c, path, method, body, parsedReq, startTime)
}

// handleNonStreamingProxy handles non-streaming requests.
func (p *Proxy) handleNonStreamingProxy(c *fiber.Ctx, path, method string, body []byte, parsedReq *llm.ChatRequest, startTime time.Time) error {
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

	// If this was a chat request, try to parse and store
	if parsedReq != nil && httpResp.StatusCode == http.StatusOK {
		parsedResp, err := p.provider.ParseResponse(respBody)
		if err != nil {
			p.logger.Warn("failed to parse response",
				zap.Error(err),
				zap.String("provider", p.provider.Name()),
			)
		} else {
			p.logger.Debug("received response from upstream",
				zap.String("model", parsedResp.Model),
				zap.String("provider", p.provider.Name()),
				zap.Duration("duration", time.Since(startTime)),
			)

			// Store in DAG
			headHash, err := p.storeConversationTurn(c.Context(), p.provider.Name(), parsedReq, parsedResp)
			if err != nil {
				p.logger.Error("failed to store conversation", zap.Error(err))
			} else {
				p.logger.Info("conversation stored",
					zap.String("head_hash", truncate(headHash, 16)),
					zap.String("provider", p.provider.Name()),
				)
			}
		}
	}

	// Return response to client
	return c.Status(httpResp.StatusCode).Send(respBody)
}

// handleStreamingProxy handles streaming requests.
func (p *Proxy) handleStreamingProxy(c *fiber.Ctx, path string, body []byte, parsedReq *llm.ChatRequest, startTime time.Time) error {
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
		if parsedReq != nil && len(allChunks) > 0 {
			p.logger.Debug("streaming complete",
				zap.String("content_preview", truncate(fullContent.String(), 200)),
				zap.Int("chunk_count", len(allChunks)),
				zap.Duration("duration", time.Since(startTime)),
			)

			// Try to parse the final chunk or reconstruct response
			finalResp := p.reconstructStreamedResponse(allChunks, fullContent.String())
			if finalResp != nil {
				headHash, err := p.storeConversationTurn(context.Background(), p.provider.Name(), parsedReq, finalResp)
				if err != nil {
					p.logger.Error("failed to store conversation", zap.Error(err))
				} else {
					p.logger.Info("conversation stored",
						zap.String("head_hash", truncate(headHash, 16)),
						zap.String("provider", p.provider.Name()),
					)
				}
			}
		}
	}))

	return nil
}

// reconstructStreamedResponse attempts to build a ChatResponse from accumulated stream chunks.
func (p *Proxy) reconstructStreamedResponse(chunks [][]byte, fullContent string) *llm.ChatResponse {
	// Try parsing the last chunk as it often contains final metadata
	if len(chunks) > 0 {
		lastChunk := chunks[len(chunks)-1]
		resp, err := p.provider.ParseResponse(lastChunk)
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
// If a vector driver is configured, it also stores embeddings for semantic search.
func (p *Proxy) storeConversationTurn(ctx context.Context, providerName string, req *llm.ChatRequest, resp *llm.ChatResponse) (string, error) {
	var parent *merkle.Node
	var nodesToEmbed []*merkle.Node

	// Store each message from the request as nodes.
	for _, msg := range req.Messages {
		bucket := merkle.Bucket{
			Type:     "message",
			Role:     msg.Role,
			Content:  msg.Content,
			Model:    req.Model,
			Provider: providerName,
		}

		node := merkle.NewNode(bucket, parent)

		if err := p.driver.Put(ctx, node); err != nil {
			return "", fmt.Errorf("storing message node: %w", err)
		}

		p.logger.Debug("stored message in DAG",
			zap.String("hash", truncate(node.Hash, 16)),
			zap.String("role", msg.Role),
			zap.String("content_preview", truncate(msg.GetText(), 50)),
		)

		nodesToEmbed = append(nodesToEmbed, node)
		parent = node
	}

	// Store the response message with metadata.
	// Note: StopReason and Usage are passed via NodeOptions and stored on the Node,
	// but they do NOT affect the content-addressable hash.
	responseBucket := merkle.Bucket{
		Type:     "message",
		Role:     resp.Message.Role,
		Content:  resp.Message.Content,
		Model:    resp.Model,
		Provider: providerName,
	}

	responseNode := merkle.NewNode(responseBucket, parent, merkle.NodeMeta{
		StopReason: resp.StopReason,
		Usage:      resp.Usage,
	})
	if err := p.driver.Put(ctx, responseNode); err != nil {
		return "", fmt.Errorf("storing response node: %w", err)
	}

	p.logger.Debug("stored response in DAG",
		zap.String("hash", truncate(responseNode.Hash, 16)),
		zap.String("content_preview", truncate(resp.Message.GetText(), 50)),
	)

	nodesToEmbed = append(nodesToEmbed, responseNode)

	// Store embeddings if vector driver is configured
	println(p.config.VectorDriver)
	println(p.config.Embedder)
	if p.config.VectorDriver != nil && p.config.Embedder != nil {
		p.logger.Debug("storing in vector store with embedder")
		p.storeEmbeddings(ctx, nodesToEmbed)
	}

	return responseNode.Hash, nil
}

// storeEmbeddings generates and stores embeddings for the given nodes.
// Errors are logged but not returned to avoid failing the main storage operation.
//
// @jpmcb - this function needs refactoring: currently, it brute forces doing
// all the embeddings for all nodes, regardless if their text embedding already
// exists in the db. At this point, we haven't inserted new nodes not in the merkle dag.
//
// We should also do this async off the proxy, not blocking for the LLM client.
func (p *Proxy) storeEmbeddings(ctx context.Context, nodes []*merkle.Node) {
	for _, node := range nodes {
		text := node.Bucket.ExtractText()
		if text == "" {
			p.logger.Debug("skipping embedding for node with no text content",
				zap.String("hash", truncate(node.Hash, 16)),
			)
			continue
		}

		embedding, err := p.config.Embedder.Embed(ctx, text)
		if err != nil {
			p.logger.Warn("failed to generate embedding",
				zap.String("hash", truncate(node.Hash, 16)),
				zap.Error(err),
			)
			continue
		}

		doc := vector.Document{
			ID:        node.Hash,
			Hash:      node.Hash,
			Embedding: embedding,
		}

		if err := p.config.VectorDriver.Add(ctx, []vector.Document{doc}); err != nil {
			p.logger.Warn("failed to store embedding",
				zap.String("hash", truncate(node.Hash, 16)),
				zap.Error(err),
			)
			continue
		}

		p.logger.Debug("stored embedding",
			zap.String("hash", truncate(node.Hash, 16)),
			zap.Int("embedding_dim", len(embedding)),
		)
	}
}

func truncate(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
