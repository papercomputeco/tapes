// Package proxy provides an LLM inference proxy that stores conversations in a Merkle DAG.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

const (
	agentPathPrefix   = "/agents/"
	providerOpenAI    = "openai"
	providerAnthropic = "anthropic"
	providerOllama    = "ollama"
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
	providers     map[string]provider.Provider
	defaultProv   provider.Provider
	headerHandler *header.Handler
}

// New creates a new Proxy.
// The storer is injected to handle async persistence of conversation turns.
// Returns an error if the configured provider type is not recognized.
func New(config Config, driver storage.Driver, logger *zap.Logger) (*Proxy, error) {
	if config.ProviderType == "" {
		return nil, errors.New("provider type is required")
	}

	providers := make(map[string]provider.Provider)
	defaultProv, err := provider.New(config.ProviderType)
	if err != nil {
		return nil, fmt.Errorf("could not create new provider: %w", err)
	}
	providers[config.ProviderType] = defaultProv

	for _, route := range config.AgentRoutes {
		if route.ProviderType == "" {
			continue
		}
		if _, exists := providers[route.ProviderType]; exists {
			continue
		}
		prov, err := provider.New(route.ProviderType)
		if err != nil {
			return nil, fmt.Errorf("could not create provider %s: %w", route.ProviderType, err)
		}
		providers[route.ProviderType] = prov
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
		providers:     providers,
		defaultProv:   defaultProv,
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

// RunWithListener starts the proxy server using the provided listener.
func (p *Proxy) RunWithListener(listener net.Listener) error {
	p.logger.Info("starting proxy server",
		zap.String("listen", listener.Addr().String()),
		zap.String("upstream", p.config.UpstreamURL),
	)

	return p.server.Listener(listener)
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
	agentName, providerName, path := p.resolveAgent(c.Path(), c.Get(header.AgentNameHeader))
	prov, upstreamURL := p.resolveProvider(agentName, providerName, path)
	method := c.Method()

	// Only process POST requests that look like chat/completion endpoints
	body := c.Body()
	isChatRequest := method == "POST" && len(body) > 0

	// Parse request using configured provider
	var parsedReq *llm.ChatRequest
	if isChatRequest {
		var err error
		parsedReq, err = prov.ParseRequest(body)
		if err != nil {
			p.logger.Warn("failed to parse request",
				zap.Error(err),
				zap.String("provider", prov.Name()),
				zap.String("agent", agentName),
			)
		} else {
			p.logger.Debug("parsed request",
				zap.String("provider", prov.Name()),
				zap.String("agent", agentName),
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
			streaming = prov.DefaultStreaming()
		}
	}

	if streaming && isChatRequest {
		return p.handleStreamingProxy(c, path, upstreamURL, prov, agentName, body, parsedReq, startTime)
	}

	return p.handleNonStreamingProxy(c, path, method, upstreamURL, prov, agentName, body, parsedReq, startTime)
}

// handleNonStreamingProxy handles non-streaming requests.
func (p *Proxy) handleNonStreamingProxy(c *fiber.Ctx, path, method, upstreamURL string, prov provider.Provider, agentName string, body []byte, parsedReq *llm.ChatRequest, startTime time.Time) error {
	// Build upstream URL
	upstreamURL += path

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
		parsedResp, err := prov.ParseResponse(respBody)
		if err != nil {
			p.logger.Warn("failed to parse response",
				zap.Error(err),
				zap.String("provider", prov.Name()),
				zap.String("agent", agentName),
			)
		} else {
			p.logger.Debug("received response from upstream",
				zap.String("model", parsedResp.Model),
				zap.String("provider", prov.Name()),
				zap.String("agent", agentName),
				zap.Duration("duration", time.Since(startTime)),
			)

			// Non-blocking enqueue for async storage
			p.workerPool.Enqueue(worker.Job{
				Provider:  prov.Name(),
				AgentName: agentName,
				Req:       parsedReq,
				Resp:      parsedResp,
			})
		}
	}

	// Return response to client immediately
	return c.Status(httpResp.StatusCode).Send(respBody)
}

// handleStreamingProxy handles streaming requests.
func (p *Proxy) handleStreamingProxy(c *fiber.Ctx, path, upstreamURL string, prov provider.Provider, agentName string, body []byte, parsedReq *llm.ChatRequest, startTime time.Time) error {
	// Build upstream URL
	upstreamURL += path

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
	go p.handleHTTPRespToPipeWriter(httpResp, pw, parsedReq, prov, agentName, startTime)

	// Set the pipe reader as the body stream with unknown size (-1),
	// which triggers chunked transfer encoding in fasthttp.
	c.Context().Response.SetBodyStream(pr, -1)

	return nil
}

func (p *Proxy) handleHTTPRespToPipeWriter(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, prov provider.Provider, agentName string, startTime time.Time) {
	// Close the upstream response body once streaming is complete.
	defer httpResp.Body.Close()
	defer pw.Close()

	switch ct := httpResp.Header.Get("Content-Type"); {
	case strings.HasPrefix(ct, "text/event-stream"):
		p.handleSSEStream(httpResp, pw, parsedReq, prov, agentName, startTime)
	default:
		p.handleNDJSONStream(httpResp, pw, parsedReq, prov, agentName, startTime)
	}
}

// handleSSEStream reads an SSE-formatted upstream response (used by OpenAI
// and Anthropic), forwarding raw bytes verbatim to the pipe writer while
// parsing events for telemetry accumulation.
func (p *Proxy) handleSSEStream(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, prov provider.Provider, agentName string, startTime time.Time) {
	var allChunks [][]byte
	var fullContent strings.Builder
	var streamUsage llm.Usage

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
		p.extractContentFromJSON([]byte(ev.Data), prov.Name(), &fullContent)

		// Accumulate usage from SSE events (Anthropic splits usage across events)
		p.extractUsageFromSSE([]byte(ev.Data), prov.Name(), &streamUsage)
	}

	p.enqueueStreamedResponse(allChunks, fullContent.String(), &streamUsage, parsedReq, prov, agentName, startTime)
}

// handleNDJSONStream reads a newline-delimited JSON upstream response (used by
// Ollama), forwarding raw bytes to the pipe writer while accumulating chunks
// for telemetry.
func (p *Proxy) handleNDJSONStream(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, prov provider.Provider, agentName string, startTime time.Time) {
	var allChunks [][]byte
	var fullContent strings.Builder
	var streamUsage llm.Usage

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
		p.extractContentFromJSON(line, prov.Name(), &fullContent)

		// Accumulate usage from NDJSON events
		p.extractUsageFromSSE(line, prov.Name(), &streamUsage)

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

	p.enqueueStreamedResponse(allChunks, fullContent.String(), &streamUsage, parsedReq, prov, agentName, startTime)
}

// extractContentFromJSON performs best-effort content extraction from a JSON
// streaming chunk, dispatching on the configured provider.
func (p *Proxy) extractContentFromJSON(data []byte, providerName string, content *strings.Builder) {
	var chunkData map[string]any
	if err := json.Unmarshal(data, &chunkData); err != nil {
		return
	}

	switch providerName {
	case providerOllama:
		// Ollama NDJSON: message.content
		if msg, ok := chunkData["message"].(map[string]any); ok {
			if c, ok := msg["content"].(string); ok {
				content.WriteString(c)
			}
		}
	case providerOpenAI:
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
	case providerAnthropic:
		// Anthropic SSE: content_block_delta events carry delta.text
		if delta, ok := chunkData["delta"].(map[string]any); ok {
			if text, ok := delta["text"].(string); ok {
				content.WriteString(text)
			}
		}
	}
}

// extractUsageFromSSE extracts token usage from SSE event data.
// Anthropic splits usage across message_start (input tokens) and message_delta (output tokens).
// OpenAI includes usage in the final chunk. Ollama includes it in the final NDJSON line.
func (p *Proxy) extractUsageFromSSE(data []byte, providerName string, usage *llm.Usage) {
	var chunkData map[string]any
	if err := json.Unmarshal(data, &chunkData); err != nil {
		return
	}

	switch providerName {
	case providerAnthropic:
		chunkType, _ := chunkData["type"].(string)
		switch chunkType {
		case "message_start":
			// message_start contains: message.usage.{input_tokens, cache_creation_input_tokens, cache_read_input_tokens}
			if msg, ok := chunkData["message"].(map[string]any); ok {
				if u, ok := msg["usage"].(map[string]any); ok {
					inputTokens := jsonInt(u, "input_tokens")
					cacheCreation := jsonInt(u, "cache_creation_input_tokens")
					cacheRead := jsonInt(u, "cache_read_input_tokens")
					usage.PromptTokens = inputTokens + cacheCreation + cacheRead
					usage.CacheCreationInputTokens = cacheCreation
					usage.CacheReadInputTokens = cacheRead
				}
			}
		case "message_delta":
			// message_delta contains: usage.output_tokens
			if u, ok := chunkData["usage"].(map[string]any); ok {
				usage.CompletionTokens = jsonInt(u, "output_tokens")
			}
		}
	case providerOpenAI:
		// OpenAI includes usage in the final chunk
		if u, ok := chunkData["usage"].(map[string]any); ok {
			usage.PromptTokens = jsonInt(u, "prompt_tokens")
			usage.CompletionTokens = jsonInt(u, "completion_tokens")
		}
	case providerOllama:
		// Ollama includes usage in the final NDJSON line (done=true)
		if done, ok := chunkData["done"].(bool); ok && done {
			usage.PromptTokens = jsonInt(chunkData, "prompt_eval_count")
			usage.CompletionTokens = jsonInt(chunkData, "eval_count")
		}
	}
}

// jsonInt extracts an integer from a JSON map, handling float64 JSON number representation.
func jsonInt(m map[string]any, key string) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	return 0
}

// enqueueStreamedResponse handles post-stream telemetry: logging and
// enqueuing the reconstructed response for async storage.
func (p *Proxy) enqueueStreamedResponse(allChunks [][]byte, fullContent string, streamUsage *llm.Usage, parsedReq *llm.ChatRequest, prov provider.Provider, agentName string, startTime time.Time) {
	if parsedReq != nil && len(allChunks) > 0 {
		p.logger.Debug("streaming complete",
			zap.String("content_preview", fullContent),
			zap.Int("chunk_count", len(allChunks)),
			zap.String("agent", agentName),
			zap.Duration("duration", time.Since(startTime)),
		)

		finalResp := p.reconstructStreamedResponse(allChunks, fullContent, streamUsage, prov)
		if finalResp != nil {
			p.workerPool.Enqueue(worker.Job{
				Provider:  prov.Name(),
				AgentName: agentName,
				Req:       parsedReq,
				Resp:      finalResp,
			})
		}
	}
}

// reconstructStreamedResponse attempts to build a ChatResponse from accumulated stream chunks.
func (p *Proxy) reconstructStreamedResponse(chunks [][]byte, fullContent string, streamUsage *llm.Usage, prov provider.Provider) *llm.ChatResponse {
	// Try parsing the last chunk as it often contains final metadata
	if len(chunks) > 0 {
		lastChunk := chunks[len(chunks)-1]
		resp, err := prov.ParseResponse(lastChunk)
		if err == nil && resp != nil {
			// If the last chunk has minimal content, supplement with accumulated content
			if resp.Message.GetText() == "" && fullContent != "" {
				resp.Message = llm.NewTextMessage("assistant", fullContent)
			}
			// Prefer accumulated stream usage over last-chunk usage (which is often empty)
			if streamUsage != nil && (streamUsage.PromptTokens > 0 || streamUsage.CompletionTokens > 0) {
				streamUsage.TotalTokens = streamUsage.PromptTokens + streamUsage.CompletionTokens
				resp.Usage = streamUsage
			}
			return resp
		}
	}

	// Fallback: construct a minimal response from accumulated content
	if fullContent != "" {
		resp := &llm.ChatResponse{
			Message:   llm.NewTextMessage("assistant", fullContent),
			Done:      true,
			CreatedAt: time.Now(),
		}
		if streamUsage != nil && (streamUsage.PromptTokens > 0 || streamUsage.CompletionTokens > 0) {
			streamUsage.TotalTokens = streamUsage.PromptTokens + streamUsage.CompletionTokens
			resp.Usage = streamUsage
		}
		return resp
	}

	return nil
}

func (p *Proxy) resolveAgent(path, headerValue string) (string, string, string) {
	agent := strings.TrimSpace(headerValue)
	if agent != "" {
		return agent, "", path
	}

	if !strings.HasPrefix(path, agentPathPrefix) {
		return "", "", path
	}

	remainder := strings.TrimPrefix(path, agentPathPrefix)
	if remainder == "" {
		return "", "", path
	}

	parts := strings.SplitN(remainder, "/", 2)
	agent = strings.TrimSpace(parts[0])
	if agent == "" {
		return "", "", path
	}

	if len(parts) == 1 {
		return agent, "", "/"
	}

	providerName, trimmedPath := resolveProviderOverride("/" + parts[1])
	return agent, providerName, trimmedPath
}

func (p *Proxy) resolveProvider(agentName, providerName, path string) (provider.Provider, string) {
	if providerName != "" {
		return p.providerByName(providerName, agentName, path)
	}

	if agentName != "" {
		if route, ok := p.config.AgentRoutes[agentName]; ok {
			if prov, ok := p.providers[route.ProviderType]; ok {
				upstream := route.UpstreamURL
				if upstream == "" {
					upstream = p.config.UpstreamURL
				}
				return prov, p.resolveOpenAIAuthUpstream(agentName, route.ProviderType, path, upstream)
			}
		}
	}

	return p.defaultProv, p.config.UpstreamURL
}

func (p *Proxy) providerByName(providerName, agentName, path string) (provider.Provider, string) {
	if prov, ok := p.providers[providerName]; ok {
		switch providerName {
		case providerOpenAI:
			upstream := p.providerUpstream(providerName, "https://api.openai.com/v1")
			return prov, p.resolveOpenAIAuthUpstream(agentName, providerName, path, upstream)
		case providerAnthropic:
			return prov, p.providerUpstream(providerName, "https://api.anthropic.com")
		case providerOllama:
			return prov, p.providerUpstream(providerName, p.config.UpstreamURL)
		}

		return prov, p.config.UpstreamURL
	}

	return p.defaultProv, p.config.UpstreamURL
}

func (p *Proxy) resolveOpenAIAuthUpstream(agentName, providerName, path, upstream string) string {
	if providerName == providerOpenAI && agentName == "codex" && isOpenAIAuthPath(path) {
		return "https://auth.openai.com"
	}
	return upstream
}

func (p *Proxy) providerUpstream(providerName, fallback string) string {
	if p.config.ProviderUpstreams == nil {
		return fallback
	}
	if upstream := strings.TrimSpace(p.config.ProviderUpstreams[providerName]); upstream != "" {
		return upstream
	}
	return fallback
}

func resolveProviderOverride(path string) (string, string) {
	if !strings.HasPrefix(path, "/providers/") {
		return "", path
	}

	remainder := strings.TrimPrefix(path, "/providers/")
	if remainder == "" {
		return "", path
	}

	parts := strings.SplitN(remainder, "/", 2)
	providerName := strings.TrimSpace(parts[0])
	if providerName == "" {
		return "", path
	}

	if len(parts) == 1 {
		return providerName, "/"
	}

	return providerName, "/" + parts[1]
}

func isOpenAIAuthPath(path string) bool {
	lower := strings.ToLower(path)
	if strings.HasPrefix(lower, "/oauth") || strings.HasPrefix(lower, "/v1/oauth") {
		return true
	}
	if strings.HasPrefix(lower, "/auth") || strings.HasPrefix(lower, "/v1/auth") {
		return true
	}
	if strings.HasPrefix(lower, "/api/oauth") || strings.HasPrefix(lower, "/api/auth") {
		return true
	}
	if strings.HasPrefix(lower, "/oauth2") || strings.HasPrefix(lower, "/v1/oauth2") {
		return true
	}
	return false
}
