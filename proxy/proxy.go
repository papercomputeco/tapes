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
	"github.com/gofiber/fiber/v2/middleware/compress"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/sse"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/proxy/header"
	"github.com/papercomputeco/tapes/proxy/worker"
)

// Proxy is a client, LLM inference proxy that instruments storing sessions as Merkle DAGs.
// The proxy is transparent: it forwards requests to the upstream LLM provider and
// enqueues conversation turns for async storage via its worker pool.
type Proxy struct {
	config        Config
	driver        storage.Driver
	workerPool    *worker.Pool
	logger        *zap.Logger
	httpClient    *http.Client
	server        *fiber.App
	provider      provider.Provider
	headerHandler *header.Handler
}

// New creates a new Proxy.
// The storer is injected to handle async persistence of conversation turns.
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

	// Add compression middleware to handle responses
	app.Use(compress.New())

	wp, err := worker.NewPool(&worker.Config{
		Driver:       driver,
		VectorDriver: config.VectorDriver,
		Embedder:     config.Embedder,
		Project:      config.Project,
		Logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create worker pool: %w", err)
	}

	p := &Proxy{
		config:        config,
		driver:        driver,
		workerPool:    wp,
		logger:        logger,
		server:        app,
		provider:      prov,
		headerHandler: header.NewHandler(),
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

// Close gracefully shuts down the proxy and waits for the worker pool to drain
func (p *Proxy) Close() error {
	p.workerPool.Close()
	return p.server.Shutdown()
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

	// Determine if streaming: check the parsed request's explicit Stream field,
	// fall back to raw JSON, and finally consult the provider's default.
	// Some providers (e.g. Ollama) stream by default when "stream" is omitted.
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
		} else {
			streaming = p.provider.DefaultStreaming()
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

	p.headerHandler.SetUpstreamRequestHeaders(c, httpReq)

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

	p.headerHandler.SetClientResponseHeaders(c, httpResp)

	// If this was a chat request, enqueue for async storage
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

			// Non-blocking enqueue for async storage
			p.workerPool.Enqueue(worker.Job{
				Provider: p.provider.Name(),
				Req:      parsedReq,
				Resp:     parsedResp,
			})
		}
	}

	// Return response to client immediately
	return c.Status(httpResp.StatusCode).Send(respBody)
}

// handleStreamingProxy handles streaming requests.
func (p *Proxy) handleStreamingProxy(c *fiber.Ctx, path string, body []byte, parsedReq *llm.ChatRequest, startTime time.Time) error {
	// Build upstream URL
	upstreamURL := p.config.UpstreamURL + path

	// Use context.Background() instead of c.Context() because fasthttp recycles
	// its RequestCtx after the handler returns, but the streaming callback runs
	// asynchronously in a separate goroutine and needs the upstream connection
	// to remain open.
	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		p.logger.Error("failed to create upstream request", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "internal error"})
	}

	p.headerHandler.SetUpstreamRequestHeaders(c, httpReq)

	p.logger.Debug("forwarding streaming request to upstream",
		zap.String("url", upstreamURL),
	)

	// Make the request
	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		p.logger.Error("upstream request failed", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{Error: "upstream request failed"})
	}
	if httpResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		p.logger.Error("upstream returned error",
			zap.Int("status", httpResp.StatusCode),
			zap.String("body", string(respBody)),
		)
		return c.Status(httpResp.StatusCode).Send(respBody)
	}

	p.headerHandler.SetClientResponseHeaders(c, httpResp)

	// Use io.Pipe + SetBodyStream instead of SetBodyStreamWriter.
	// SetBodyStreamWriter uses an internal PipeConns with a buffered channel
	// (capacity 4) and two bufio.Writers, which means Flush() in the callback
	// only pushes data into the pipe — NOT to the TCP socket. This causes all
	// chunks to buffer in memory before being sent to the client.
	//
	// With io.Pipe, pw.Write blocks until the reader consumes the data, and
	// the reader is fasthttp's writeBodyChunked which flushes to TCP after
	// every chunk. This gives direct backpressure and true per-chunk streaming
	// for LLM based.
	pr, pw := io.Pipe()
	go p.handleHTTPRespToPipeWriter(httpResp, pw, parsedReq, startTime)

	// Set the pipe reader as the body stream with unknown size (-1),
	// which triggers chunked transfer encoding in fasthttp.
	c.Context().Response.SetBodyStream(pr, -1)

	return nil
}

func (p *Proxy) handleHTTPRespToPipeWriter(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, startTime time.Time) {
	// Close the upstream response body once streaming is complete.
	defer httpResp.Body.Close()
	defer pw.Close()

	switch ct := httpResp.Header.Get("Content-Type"); {
	case strings.HasPrefix(ct, "text/event-stream"):
		p.handleSSEStream(httpResp, pw, parsedReq, startTime)
	default:
		p.handleNDJSONStream(httpResp, pw, parsedReq, startTime)
	}
}

// handleSSEStream reads an SSE-formatted upstream response (used by OpenAI
// and Anthropic), forwarding raw bytes verbatim to the pipe writer while
// parsing events for telemetry accumulation.
func (p *Proxy) handleSSEStream(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, startTime time.Time) {
	var allChunks [][]byte
	var fullContent strings.Builder

	tr := sse.NewTeeReader(httpResp.Body, pw)

	for {
		ev, err := tr.Next()
		if err != nil {
			p.logger.Error("error reading SSE stream", zap.Error(err))
			return
		}
		if ev == nil {
			break
		}

		// Skip non-data sentinels like OpenAI's "[DONE]"
		if ev.Data == "[DONE]" {
			continue
		}

		// Store the data payload for later reconstruction
		chunkCopy := []byte(ev.Data)
		allChunks = append(allChunks, chunkCopy)

		// Best-effort content extraction from the JSON payload
		p.extractContentFromJSON([]byte(ev.Data), &fullContent)
	}

	p.enqueueStreamedResponse(allChunks, fullContent.String(), parsedReq, startTime)
}

// handleNDJSONStream reads a newline-delimited JSON upstream response (used by
// Ollama), forwarding raw bytes to the pipe writer while accumulating chunks
// for telemetry.
func (p *Proxy) handleNDJSONStream(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, startTime time.Time) {
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

		// Best-effort content extraction from the raw chunk
		p.extractContentFromJSON(line, &fullContent)

		// Write chunk to client — pw.Write blocks until fasthttp reads
		// from the pipe reader and flushes to the TCP socket.
		// This ensures transparent streaming of chunks.
		if _, err := pw.Write(line); err != nil {
			p.logger.Error("error writing chunk to pipe", zap.Error(err))
			return
		}
		if _, err := pw.Write([]byte("\n")); err != nil {
			p.logger.Error("error writing newline to pipe", zap.Error(err))
			return
		}
	}

	if err := scanner.Err(); err != nil {
		p.logger.Error("error reading NDJSON stream", zap.Error(err))
	}

	p.enqueueStreamedResponse(allChunks, fullContent.String(), parsedReq, startTime)
}

// extractContentFromJSON performs best-effort content extraction from a JSON
// streaming chunk, dispatching on the configured provider.
func (p *Proxy) extractContentFromJSON(data []byte, content *strings.Builder) {
	var chunkData map[string]any
	if err := json.Unmarshal(data, &chunkData); err != nil {
		return
	}

	switch p.provider.Name() {
	case "ollama":
		// Ollama NDJSON: message.content
		if msg, ok := chunkData["message"].(map[string]any); ok {
			if c, ok := msg["content"].(string); ok {
				content.WriteString(c)
			}
		}
	case "openai":
		// OpenAI SSE: choices[0].delta.content
		if choices, ok := chunkData["choices"].([]any); ok && len(choices) > 0 {
			if choice, ok := choices[0].(map[string]any); ok {
				if delta, ok := choice["delta"].(map[string]any); ok {
					if c, ok := delta["content"].(string); ok {
						content.WriteString(c)
					}
				}
			}
		}
	case "anthropic":
		// Anthropic SSE: content_block_delta events carry delta.text
		if delta, ok := chunkData["delta"].(map[string]any); ok {
			if text, ok := delta["text"].(string); ok {
				content.WriteString(text)
			}
		}
	}
}

// enqueueStreamedResponse handles post-stream telemetry: logging and
// enqueuing the reconstructed response for async storage.
func (p *Proxy) enqueueStreamedResponse(allChunks [][]byte, fullContent string, parsedReq *llm.ChatRequest, startTime time.Time) {
	if parsedReq != nil && len(allChunks) > 0 {
		p.logger.Debug("streaming complete",
			zap.String("content_preview", fullContent),
			zap.Int("chunk_count", len(allChunks)),
			zap.Duration("duration", time.Since(startTime)),
		)

		finalResp := p.reconstructStreamedResponse(allChunks, fullContent)
		if finalResp != nil {
			p.workerPool.Enqueue(worker.Job{
				Provider: p.provider.Name(),
				Req:      parsedReq,
				Resp:     finalResp,
			})
		}
	}
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
