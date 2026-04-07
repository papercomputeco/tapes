package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	fastws "github.com/fasthttp/websocket"
	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"
)

const (
	listenAddr    = "127.0.0.1:8765"
	upstreamHost  = "chatgpt.com"
	upstreamHTTPS = "https://chatgpt.com"
	upstreamWSS   = "wss://chatgpt.com"
)

var (
	sensitiveHeaders = map[string]struct{}{
		"authorization":       {},
		"proxy-authorization": {},
		"cookie":              {},
		"set-cookie":          {},
	}
	skipUpstreamHeaders = map[string]struct{}{
		"host":            {},
		"content-length":  {},
		"connection":      {},
		"upgrade":         {},
		"accept-encoding": {},
	}
	skipClientHeaders = map[string]struct{}{
		"connection":        {},
		"transfer-encoding": {},
		"content-encoding":  {},
		"content-length":    {},
	}
)

type logFields map[string]any

type turnRecorder struct {
	mu             sync.Mutex
	current        *turnState
	lastModel      string
	lastEventCount int
}

type turnState struct {
	RequestEvent map[string]any
	ResponseID   string
	Model        string
	OutputParts  []string
	EventCount   int
	StopReason   string
	Usage        map[string]int
	RawRequest   json.RawMessage
	RawResponse  json.RawMessage
}

type messageConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	Close() error
}

func main() {
	app := fiber.New(fiber.Config{
		StreamRequestBody: true,
	})

	app.Use("/ws", func(c fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("request_uri", string(c.Request().URI().RequestURI()))
			c.Locals("request_path", c.Path())
			c.Locals("request_headers", c.GetReqHeaders())
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.All("/ws", websocket.New(handleWebsocket))
	app.All("/*", handleHTTP)

	logEvent("server_started", logFields{
		"http_listen": "http://" + listenAddr,
		"ws_listen":   "ws://" + listenAddr + "/ws",
	})

	log.Fatal(app.Listen(listenAddr))
}

func handleHTTP(c fiber.Ctx) error {
	body := bytes.Clone(c.Body())
	upstreamPath := rewritePath(string(c.Request().URI().RequestURI()))
	headers := interestingHeaders(c.GetReqHeaders())

	logEvent("http_request", logFields{
		"method":        c.Method(),
		"path":          c.Path(),
		"upstream_path": upstreamPath,
		"body_bytes":    len(body),
		"headers":       headers,
		"client":        c.IP(),
		"fallback_hint": isFallbackPath(c.Path()),
	})

	req, err := http.NewRequest(c.Method(), upstreamHTTPS+upstreamPath, bytes.NewReader(body))
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
	}
	copyUpstreamHeaders(c.GetReqHeaders(), req.Header)
	req.Host = upstreamHost

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logEvent("http_upstream_error", logFields{"error": err.Error()})
		return c.Status(fiber.StatusBadGateway).SendString(err.Error())
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.Status(fiber.StatusBadGateway).SendString(err.Error())
	}

	copyClientHeaders(c, resp.Header)
	logEvent("http_response", logFields{
		"method":         c.Method(),
		"path":           c.Path(),
		"upstream_path":  upstreamPath,
		"status":         resp.StatusCode,
		"content_type":   resp.Header.Get("Content-Type"),
		"response_bytes": len(respBody),
		"client":         c.IP(),
	})

	if isFallbackPath(c.Path()) {
		recorder := &turnRecorder{}
		recorder.observeClientBody(body)
		parseSummary := parseSSE(bytes.NewReader(respBody), func(payload []byte) {
			recorder.observeUpstreamEvent(payload)
		})
		logEvent("http_sse_probe", logFields{
			"path":             c.Path(),
			"upstream_path":    upstreamPath,
			"events_seen":      parseSummary.EventsSeen,
			"data_frames_seen": parseSummary.DataFramesSeen,
			"done_seen":        parseSummary.DoneSeen,
			"json_frames_seen": parseSummary.JSONFramesSeen,
			"parse_errors":     parseSummary.ParseErrors,
			"client":           c.IP(),
		})
		logEvent("http_turn_probe", logFields{
			"path":            c.Path(),
			"upstream_path":   upstreamPath,
			"request_model":   recorder.currentModel(),
			"recorded_events": recorder.eventCount(),
			"client":          c.IP(),
		})
	}

	c.Status(resp.StatusCode)
	return c.Send(respBody)
}

func handleWebsocket(clientConn *websocket.Conn) {
	recorder := &turnRecorder{}
	requestURI, _ := clientConn.Locals("request_uri").(string)
	requestPath, _ := clientConn.Locals("request_path").(string)
	requestHeaders, _ := clientConn.Locals("request_headers").(map[string][]string)
	upstreamURL := upstreamWSS + rewritePath(requestURI)
	headers := http.Header{}
	for k, values := range requestHeaders {
		if _, skip := skipUpstreamHeaders[strings.ToLower(k)]; skip {
			continue
		}
		for _, v := range values {
			headers.Add(k, v)
		}
	}
	headers.Set("Host", upstreamHost)

	logEvent("ws_upgrade", logFields{
		"path":          requestPath,
		"upstream_path": rewritePath(requestURI),
		"headers":       interestingHeaders(requestHeaders),
	})

	upstreamConn, resp, err := fastws.DefaultDialer.Dial(upstreamURL, headers)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		logEvent("ws_upstream_error", logFields{
			"error":  err.Error(),
			"status": status,
		})
		_ = clientConn.WriteJSON(map[string]any{"error": err.Error(), "status": status})
		_ = clientConn.Close()
		return
	}
	defer upstreamConn.Close()
	defer clientConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go relayWS(&wg, clientConn.Conn, upstreamConn, "client_to_upstream", recorder)
	go relayWS(&wg, upstreamConn, clientConn.Conn, "upstream_to_client", recorder)
	wg.Wait()
}

func relayWS(wg *sync.WaitGroup, src, dst messageConn, direction string, recorder *turnRecorder) {
	defer wg.Done()
	for {
		mt, msg, err := src.ReadMessage()
		if err != nil {
			logEvent("ws_closed", logFields{"direction": direction, "error": err.Error()})
			_ = dst.Close()
			return
		}
		if mt == websocket.TextMessage {
			recorder.observe(direction, msg)
		}
		if err := dst.WriteMessage(mt, msg); err != nil {
			logEvent("ws_write_error", logFields{"direction": direction, "error": err.Error()})
			_ = src.Close()
			return
		}
	}
}

type sseParseSummary struct {
	EventsSeen     int
	DataFramesSeen int
	DoneSeen       bool
	JSONFramesSeen int
	ParseErrors    int
}

func parseSSE(r io.Reader, handle func([]byte)) sseParseSummary {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var dataLines []string
	var summary sseParseSummary
	flush := func() {
		if len(dataLines) == 0 {
			return
		}
		summary.EventsSeen++
		payload := strings.Join(dataLines, "\n")
		dataLines = nil
		if payload == "[DONE]" {
			summary.DoneSeen = true
			return
		}
		summary.DataFramesSeen++
		var check map[string]any
		if err := json.Unmarshal([]byte(payload), &check); err == nil {
			summary.JSONFramesSeen++
		} else {
			summary.ParseErrors++
		}
		handle([]byte(payload))
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	flush()
	return summary
}

func (r *turnRecorder) observeClientBody(body []byte) {
	var event map[string]any
	if err := json.Unmarshal(body, &event); err != nil {
		return
	}
	r.mu.Lock()
	if r.current == nil && looksLikeRequest(event) {
		r.current = &turnState{}
	}
	if r.current != nil {
		r.current.RawRequest = append(json.RawMessage(nil), body...)
	}
	r.mu.Unlock()
	r.observeMap("client_to_upstream", event)
}

func (r *turnRecorder) observe(direction string, payload []byte) {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return
	}
	r.observeMap(direction, event)
}

func (r *turnRecorder) observeUpstreamEvent(payload []byte) {
	r.observe("upstream_to_client", payload)
}

func (r *turnRecorder) observeMap(direction string, event map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	kind := eventType(event)
	if kind == "response.created" {
		r.current = &turnState{
			RequestEvent: nil,
			ResponseID:   nestedString(event, "response", "id"),
			Model:        nestedString(event, "response", "model"),
			Usage:        map[string]int{},
		}
	}

	if r.current == nil && direction == "client_to_upstream" && looksLikeRequest(event) {
		r.current = &turnState{
			RequestEvent: event,
			Model:        stringValue(event["model"]),
			Usage:        map[string]int{},
		}
	}
	if r.current == nil {
		return
	}

	r.current.EventCount++
	if direction == "client_to_upstream" && r.current.RequestEvent == nil && looksLikeRequest(event) {
		r.current.RequestEvent = event
		if r.current.Model == "" {
			r.current.Model = stringValue(event["model"])
		}
	}

	if direction == "upstream_to_client" {
		r.current.RawResponse = appendJSON(r.current.RawResponse, event)
		if model := nestedString(event, "response", "model"); model != "" {
			r.current.Model = model
		}
		if responseID := nestedString(event, "response", "id"); responseID != "" {
			r.current.ResponseID = responseID
		}
		if status := nestedString(event, "response", "status"); status != "" {
			r.current.StopReason = status
		}
		mergeUsage(r.current.Usage, event)
		switch kind {
		case "response.output_text.delta", "response.output_text.added", "output_text.delta":
			if delta := stringValue(event["delta"]); delta != "" {
				r.current.OutputParts = append(r.current.OutputParts, delta)
			}
		default:
			r.current.OutputParts = append(r.current.OutputParts, extractTextParts(event)...)
		}
	}

	switch kind {
	case "response.done", "response.completed", "response.failed":
		r.flush(event)
		r.current = nil
	}
}

func (r *turnRecorder) flush(finalEvent map[string]any) {
	if r.current == nil {
		return
	}
	model := r.current.Model
	if model == "" {
		model = nestedString(finalEvent, "response", "model")
	}
	responseID := r.current.ResponseID
	if responseID == "" {
		responseID = nestedString(finalEvent, "response", "id")
	}
	inputText := strings.Join(extractRequestText(r.current.RequestEvent), "\n")
	outputText := strings.Join(r.current.OutputParts, "")
	if outputText == "" {
		outputText = strings.Join(extractTextParts(finalEvent), "\n")
	}
	fields := logFields{
		"request_model":  model,
		"response_id":    responseID,
		"response_chars": len(outputText),
		"request_chars":  len(inputText),
		"event_count":    r.current.EventCount,
		"stop_reason":    coalesceString(r.current.StopReason, nestedString(finalEvent, "response", "status"), eventType(finalEvent)),
	}
	if len(r.current.Usage) > 0 {
		fields["usage"] = r.current.Usage
	}
	logEvent("turn_reconstructed", fields)
	r.lastModel = model
	r.lastEventCount = r.current.EventCount
}

func (r *turnRecorder) currentModel() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		return r.current.Model
	}
	return r.lastModel
}

func (r *turnRecorder) eventCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		return r.current.EventCount
	}
	return r.lastEventCount
}

func rewritePath(path string) string {
	u, err := url.Parse(path)
	if err != nil {
		return "/backend-api/codex/responses"
	}
	cleanPath := u.Path
	switch {
	case strings.HasPrefix(cleanPath, "/backend-api/"):
	case cleanPath == "/responses", cleanPath == "/v1/responses":
		cleanPath = "/backend-api/codex/responses"
	case strings.HasPrefix(cleanPath, "/responses/"):
		cleanPath = "/backend-api/codex/responses" + strings.TrimPrefix(cleanPath, "/responses")
	case strings.HasPrefix(cleanPath, "/v1/responses/"):
		cleanPath = "/backend-api/codex/responses" + strings.TrimPrefix(cleanPath, "/v1/responses")
	case strings.HasPrefix(cleanPath, "/api/codex/"):
		cleanPath = "/backend-api/codex/" + strings.TrimPrefix(cleanPath, "/api/codex/")
	case strings.HasPrefix(cleanPath, "/connectors/"):
		cleanPath = "/backend-api/codex/connectors/" + strings.TrimPrefix(cleanPath, "/connectors/")
	default:
		cleanPath = "/backend-api/codex" + cleanPath
	}
	u.Path = cleanPath
	return u.RequestURI()
}

func copyUpstreamHeaders(src map[string][]string, dst http.Header) {
	for k, values := range src {
		if _, skip := skipUpstreamHeaders[strings.ToLower(k)]; skip {
			continue
		}
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func copyClientHeaders(c fiber.Ctx, src http.Header) {
	for k, values := range src {
		if _, skip := skipClientHeaders[strings.ToLower(k)]; skip {
			continue
		}
		c.Set(k, strings.Join(values, ", "))
	}
}

func interestingHeaders(headers map[string][]string) map[string]string {
	out := map[string]string{}
	for k, values := range headers {
		lower := strings.ToLower(k)
		switch lower {
		case "authorization", "content-type", "accept", "user-agent", "chatgpt-account-id", "upgrade", "connection", "sec-websocket-protocol":
			if len(values) > 0 {
				out[k] = redactHeaderValue(k, values[0])
			}
		}
	}
	return out
}

func interestingHTTPHeaders(headers http.Header) map[string]string {
	out := map[string]string{}
	for k, values := range headers {
		if len(values) == 0 {
			continue
		}
		switch strings.ToLower(k) {
		case "authorization", "content-type", "accept", "user-agent", "chatgpt-account-id", "upgrade", "connection", "sec-websocket-protocol":
			out[k] = redactHeaderValue(k, values[0])
		}
	}
	return out
}

func redactHeaderValue(name, value string) string {
	if _, ok := sensitiveHeaders[strings.ToLower(name)]; !ok {
		return value
	}
	if strings.EqualFold(name, "authorization") {
		parts := strings.SplitN(value, " ", 2)
		if len(parts) == 2 {
			return parts[0] + " [redacted]"
		}
	}
	return "[redacted]"
}

func eventType(event map[string]any) string {
	for _, key := range []string{"type", "event", "method"} {
		if value, ok := event[key].(string); ok {
			return value
		}
	}
	return ""
}

func looksLikeRequest(event map[string]any) bool {
	_, hasInput := event["input"]
	_, hasMessages := event["messages"]
	_, hasModel := event["model"]
	return hasModel || hasInput || hasMessages
}

func extractTextParts(value any) []string {
	var parts []string
	switch v := value.(type) {
	case string:
		parts = append(parts, v)
	case []any:
		for _, item := range v {
			parts = append(parts, extractTextParts(item)...)
		}
	case map[string]any:
		if text, ok := v["text"].(string); ok {
			parts = append(parts, text)
		}
		for _, key := range []string{"content", "input", "message", "delta"} {
			if child, ok := v[key]; ok {
				parts = append(parts, extractTextParts(child)...)
			}
		}
	}
	return parts
}

func extractRequestText(value any) []string {
	var parts []string
	switch v := value.(type) {
	case map[string]any:
		for _, key := range []string{"input_text", "text"} {
			if text, ok := v[key].(string); ok && text != "" {
				parts = append(parts, text)
			}
		}
		for _, key := range []string{"input", "messages", "content", "message"} {
			if child, ok := v[key]; ok {
				parts = append(parts, extractRequestText(child)...)
			}
		}
	case []any:
		for _, item := range v {
			parts = append(parts, extractRequestText(item)...)
		}
	case string:
		if v != "" {
			parts = append(parts, v)
		}
	}
	return parts
}

func mergeUsage(dst map[string]int, event map[string]any) {
	for _, candidate := range []map[string]any{
		nestedMap(event, "response", "usage"),
		nestedMap(event, "usage"),
	} {
		for _, key := range []string{"prompt_tokens", "completion_tokens", "total_tokens", "input_tokens", "output_tokens"} {
			if candidate == nil {
				continue
			}
			if value, ok := candidate[key].(float64); ok {
				dst[key] = int(value)
			}
		}
	}
}

func nestedMap(m map[string]any, path ...string) map[string]any {
	current := any(m)
	for _, key := range path {
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = asMap[key]
		if !ok {
			return nil
		}
	}
	asMap, _ := current.(map[string]any)
	return asMap
}

func appendJSON(existing json.RawMessage, event map[string]any) json.RawMessage {
	payload, err := json.Marshal(event)
	if err != nil {
		return existing
	}
	if len(existing) == 0 {
		return payload
	}
	combined := make([]byte, 0, len(existing)+1+len(payload))
	combined = append(combined, existing...)
	combined = append(combined, '\n')
	combined = append(combined, payload...)
	return combined
}

func coalesceString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func nestedString(m map[string]any, path ...string) string {
	current := any(m)
	for _, key := range path {
		asMap, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = asMap[key]
		if !ok {
			return ""
		}
	}
	return stringValue(current)
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func isFallbackPath(path string) bool {
	return strings.Contains(path, "responses")
}

func logEvent(event string, fields logFields) {
	record := map[string]any{
		"ts":    time.Now().Format("2006-01-02T15:04:05-0700"),
		"event": event,
	}
	for k, v := range fields {
		record[k] = v
	}
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(record)
}
