package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// httpClientTimeout bounds every trace-API request the generator makes.
const httpClientTimeout = 30 * time.Second

// Querier is the read surface skill generation needs from the tapes
// trace API: turn summaries for a session, and span payloads for one
// turn. The HTTP client below implements it; tests substitute a fake.
type Querier interface {
	// TraceSummaries returns the user-visible turns of a session
	// (GET /v1/traces?session_id=), newest derive's projection.
	TraceSummaries(ctx context.Context, sessionID string) ([]TraceSummary, error)

	// Trace returns one turn's spans with full payloads
	// (GET /v1/traces/{trace_id}).
	Trace(ctx context.Context, traceID string) (*Trace, error)
}

// TraceSummary is one user-visible turn header — the turn-grain
// prompt/response pair the deriver folded for the session.
type TraceSummary struct {
	TraceID         string
	UserPrompt      string
	ResponsePreview string
	// Synthetic is non-empty for turns the harness manufactured
	// (compaction, resume replay); they carry no user intent and are
	// excluded from skill transcripts.
	Synthetic string
	StartedAt time.Time
	// Token counts folded by the deriver for the turn. Total* spans the
	// whole turn (spine + harness shadow calls); Main* counts only the
	// conversation-spine calls. Surfaced by the checkout export.
	TotalInputTokens  int64
	TotalOutputTokens int64
	MainInputTokens   int64
	MainOutputTokens  int64
}

// Trace is one turn's span detail.
type Trace struct {
	TraceID string
	Spans   []Span
}

// Span is the slice of the API's span shape the transcript builder
// consumes: identity, ordering, the call-kind taxonomy, and the
// decoded output content for llm spans.
type Span struct {
	SpanID       string
	ParentSpanID string
	Kind         string // "llm", "tool", "agent", "event"
	Name         string // tool name for tool spans
	Seq          int64
	// CallKind is the derive-time taxonomy ("main", "offshoot:…",
	// "injected:…") carried in span metadata; empty for tool spans.
	CallKind string
	// ThreadID is the harness sub-thread ("" = the main conversation).
	ThreadID string
	// Output is the decoded output content for llm spans (nil for
	// tool spans — the builder only needs their Name).
	Output []llm.ContentBlock
}

// APIClient implements Querier against a running tapes API server. The
// wire shapes mirror api.TraceListResponse / api.TraceDetail without
// importing the server package — same precedent as the other CLI-side
// clients.
type APIClient struct {
	apiTarget string
	client    *http.Client
}

// NewAPIClient constructs an APIClient pointed at apiTarget (e.g.
// "http://127.0.0.1:8081").
func NewAPIClient(apiTarget string) *APIClient {
	return &APIClient{
		apiTarget: normalizeAPITarget(apiTarget),
		client:    &http.Client{Timeout: httpClientTimeout},
	}
}

var _ Querier = (*APIClient)(nil)

func normalizeAPITarget(apiTarget string) string {
	target := strings.TrimSpace(apiTarget)
	if target == "" {
		return ""
	}
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	return strings.TrimRight(target, "/")
}

// wireTrace mirrors api.TraceItem.
type wireTrace struct {
	TraceID           string         `json:"trace_id"`
	UserPrompt        string         `json:"user_prompt"`
	ResponsePreview   string         `json:"response_preview"`
	StartedAt         time.Time      `json:"started_at"`
	TotalInputTokens  int64          `json:"total_input_tokens"`
	TotalOutputTokens int64          `json:"total_output_tokens"`
	MainInputTokens   int64          `json:"main_input_tokens"`
	MainOutputTokens  int64          `json:"main_output_tokens"`
	Metadata          map[string]any `json:"metadata"`
}

// wireSpan mirrors the subset of api.SpanItem the builder consumes.
type wireSpan struct {
	SpanID       string                     `json:"span_id"`
	ParentSpanID string                     `json:"parent_span_id"`
	Kind         string                     `json:"kind"`
	Name         string                     `json:"name"`
	Seq          int64                      `json:"seq"`
	Metadata     map[string]any             `json:"metadata"`
	Output       map[string]json.RawMessage `json:"output"`
}

// wireTraceList mirrors api.TraceListResponse.
type wireTraceList struct {
	Items []wireTrace `json:"items"`
}

// wireTraceDetail mirrors api.TraceDetail.
type wireTraceDetail struct {
	Trace wireTrace  `json:"trace"`
	Spans []wireSpan `json:"spans"`
}

// TraceSummaries implements Querier via GET /v1/traces?session_id=.
func (c *APIClient) TraceSummaries(ctx context.Context, sessionID string) ([]TraceSummary, error) {
	u, err := url.Parse(c.apiTarget + "/v1/traces")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	q := u.Query()
	q.Set("session_id", sessionID)
	u.RawQuery = q.Encode()

	var list wireTraceList
	if err := c.getJSON(ctx, u.String(), &list); err != nil {
		return nil, fmt.Errorf("list traces for session %s: %w", sessionID, err)
	}

	out := make([]TraceSummary, 0, len(list.Items))
	for _, item := range list.Items {
		out = append(out, TraceSummary{
			TraceID:           item.TraceID,
			UserPrompt:        item.UserPrompt,
			ResponsePreview:   item.ResponsePreview,
			Synthetic:         metadataString(item.Metadata, "synthetic"),
			StartedAt:         item.StartedAt,
			TotalInputTokens:  item.TotalInputTokens,
			TotalOutputTokens: item.TotalOutputTokens,
			MainInputTokens:   item.MainInputTokens,
			MainOutputTokens:  item.MainOutputTokens,
		})
	}
	return out, nil
}

// SessionInfo is the slice of a /v1/sessions item the CLI surfaces for
// listing sessions and resolving a short id prefix.
type SessionInfo struct {
	ID            string
	StartedAt     time.Time
	TurnCount     int
	TotalCostUSD  float64
	Model         string
	DerivedStatus string
	Preview       string
}

// wireSessionList mirrors the subset of api.SessionListResponse we read.
type wireSessionList struct {
	Items []struct {
		ID            string    `json:"id"`
		StartedAt     time.Time `json:"started_at"`
		TurnCount     int       `json:"turn_count"`
		TotalCostUsd  float64   `json:"total_cost_usd"`
		Model         string    `json:"model"`
		DerivedStatus string    `json:"derived_status"`
		Preview       string    `json:"preview"`
	} `json:"items"`
}

// Sessions lists captured sessions (newest first) via GET /v1/sessions.
func (c *APIClient) Sessions(ctx context.Context) ([]SessionInfo, error) {
	u, err := url.Parse(c.apiTarget + "/v1/sessions")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	q := u.Query()
	q.Set("limit", "200")
	u.RawQuery = q.Encode()

	var list wireSessionList
	if err := c.getJSON(ctx, u.String(), &list); err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}

	out := make([]SessionInfo, 0, len(list.Items))
	for _, item := range list.Items {
		out = append(out, SessionInfo{
			ID:            item.ID,
			StartedAt:     item.StartedAt,
			TurnCount:     item.TurnCount,
			TotalCostUSD:  item.TotalCostUsd,
			Model:         item.Model,
			DerivedStatus: item.DerivedStatus,
			Preview:       item.Preview,
		})
	}
	return out, nil
}

// CaptureStats is the slice of /v1/stats the CLI surfaces for a quick
// health readout.
type CaptureStats struct {
	SessionCount int     `json:"session_count"`
	TurnCount    int     `json:"turn_count"`
	TotalCost    float64 `json:"total_cost"`
}

// Stats fetches aggregate capture stats via GET /v1/stats.
func (c *APIClient) Stats(ctx context.Context) (*CaptureStats, error) {
	var stats CaptureStats
	if err := c.getJSON(ctx, c.apiTarget+"/v1/stats", &stats); err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}
	return &stats, nil
}

// Trace implements Querier via GET /v1/traces/{trace_id}.
func (c *APIClient) Trace(ctx context.Context, traceID string) (*Trace, error) {
	u := c.apiTarget + "/v1/traces/" + url.PathEscape(traceID)
	var detail wireTraceDetail
	if err := c.getJSON(ctx, u, &detail); err != nil {
		return nil, fmt.Errorf("get trace %s: %w", traceID, err)
	}

	trace := &Trace{TraceID: detail.Trace.TraceID, Spans: make([]Span, 0, len(detail.Spans))}
	for _, sp := range detail.Spans {
		span := Span{
			SpanID:       sp.SpanID,
			ParentSpanID: sp.ParentSpanID,
			Kind:         sp.Kind,
			Name:         sp.Name,
			Seq:          sp.Seq,
			CallKind:     metadataString(sp.Metadata, "call_kind"),
			ThreadID:     metadataString(sp.Metadata, "thread_id"),
		}
		if raw, ok := sp.Output["content"]; ok && len(raw) > 0 {
			// Tool spans carry a plain string here; llm/event spans a
			// content-block array. A failed decode just means no text.
			_ = json.Unmarshal(raw, &span.Output)
		}
		trace.Spans = append(trace.Spans, span)
	}
	return trace, nil
}

func (c *APIClient) getJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errors.New("not found")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("api returned status %d: %s", resp.StatusCode, string(body))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

func metadataString(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta[key].(string); ok {
		return v
	}
	return ""
}
