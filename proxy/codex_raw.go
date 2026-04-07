package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
)

func isCodexResponsesPath(path string) bool {
	return path == "/responses" || path == "/v1/responses"
}

func isCodexRawResponsesRequest(agentName, method, path string, body []byte) bool {
	return agentName == "codex" && method == http.MethodPost && isCodexResponsesPath(path) && len(body) > 0
}

func rewriteUpstreamPath(agentName, path string) string {
	if agentName != "codex" {
		return path
	}

	switch {
	case path == "/responses", path == "/v1/responses":
		return "/backend-api/codex/responses"
	case strings.HasPrefix(path, "/responses/"):
		return "/backend-api/codex/responses" + strings.TrimPrefix(path, "/responses")
	case strings.HasPrefix(path, "/v1/responses/"):
		return "/backend-api/codex/responses" + strings.TrimPrefix(path, "/v1/responses")
	case strings.HasPrefix(path, "/api/codex/"):
		return "/backend-api/codex/" + strings.TrimPrefix(path, "/api/codex/")
	case strings.HasPrefix(path, "/connectors/"):
		return "/backend-api/codex/connectors/" + strings.TrimPrefix(path, "/connectors/")
	default:
		return path
	}
}

func (p *Proxy) handleCodexRawResponses(c *fiber.Ctx, path, upstreamURL string, prov provider.Provider, agentName string, body []byte, startTime time.Time) error {
	upstreamURL += rewriteUpstreamPath(agentName, path)

	var parsedReq *llm.ChatRequest
	if req, err := prov.ParseRequest(body); err != nil {
		p.logger.Warn("failed to parse raw codex request",
			"error", err,
			"provider", prov.Name(),
			"agent", agentName,
		)
	} else {
		parsedReq = req
		p.logger.Debug("parsed raw codex request",
			"provider", prov.Name(),
			"agent", agentName,
			"model", parsedReq.Model,
			"message_count", len(parsedReq.Messages),
		)
	}

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		p.logger.Error("failed to create raw codex upstream request", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "internal error"})
	}

	copyCodexUpstreamHeaders(c, httpReq)

	p.logger.Debug("forwarding raw codex responses request",
		"url", upstreamURL,
		"agent", agentName,
	)

	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		p.logger.Error("upstream request failed", "error", err)
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{Error: "upstream request failed"})
	}
	if httpResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		p.logger.Error("upstream returned error",
			"status", httpResp.StatusCode,
			"body", string(respBody),
		)
		copyCodexClientResponseHeaders(c, httpResp)
		return c.Status(httpResp.StatusCode).Send(respBody)
	}

	copyCodexClientResponseHeaders(c, httpResp)
	pr, pw := io.Pipe()
	go p.handleCodexHTTPRespToPipeWriter(httpResp, pw, parsedReq, prov, agentName, startTime)
	c.Context().Response.SetBodyStream(pr, -1)
	return nil
}

func copyCodexUpstreamHeaders(c *fiber.Ctx, req *http.Request) {
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := strings.ToLower(string(key))
		switch k {
		case "host", "content-length", "connection", "upgrade", "accept-encoding":
			return
		default:
			req.Header.Set(string(key), string(value))
		}
	})
	req.Host = req.URL.Host
}

func copyCodexClientResponseHeaders(c *fiber.Ctx, resp *http.Response) {
	for k, v := range resp.Header {
		switch strings.ToLower(k) {
		case "connection", "transfer-encoding", "content-encoding", "content-length":
			continue
		default:
			c.Set(k, strings.Join(v, ", "))
		}
	}
}

func (p *Proxy) handleCodexHTTPRespToPipeWriter(httpResp *http.Response, pw *io.PipeWriter, parsedReq *llm.ChatRequest, prov provider.Provider, agentName string, startTime time.Time) {
	defer httpResp.Body.Close()
	defer pw.Close()
	p.handleSSEStream(httpResp, pw, parsedReq, prov, agentName, startTime)
}
