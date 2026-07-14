package deck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

const (
	httpQueryTimeout = 30 * time.Second
	// httpQueryOverviewLimit keeps the API-backed deck lightweight: fetch one
	// recent page of session rows, then render and drill into detail on
	// demand. This avoids the old "page through the whole corpus" behavior.
	httpQueryOverviewLimit = 25
)

// HTTPQuery is a Querier implementation that talks to a remote (or
// in-process) tapes API server over HTTP. It reads the product session/trace
// surface: /v1/sessions for the overview, /v1/traces?session_id= for a
// session's turn summaries, and /v1/traces/{trace_id} for a single turn's
// conversation.
type HTTPQuery struct {
	apiTarget string
	pricing   PricingTable
	client    *http.Client
	cache     sessionCache
}

// Compile-time checks that HTTPQuery satisfies the deck query interfaces.
var (
	_ Querier       = (*HTTPQuery)(nil)
	_ OverviewPager = (*HTTPQuery)(nil)
	_ TurnQuerier   = (*HTTPQuery)(nil)
)

// NewHTTPQuery constructs an HTTPQuery pointed at apiTarget (e.g.
// "http://127.0.0.1:8081"). The pricing table is used for per-call cost
// estimates in the turn drill-in; session and turn rollups arrive already
// costed from the API.
func NewHTTPQuery(apiTarget string, pricing PricingTable) *HTTPQuery {
	return &HTTPQuery{
		apiTarget: normalizeAPITarget(apiTarget),
		pricing:   pricing,
		client:    &http.Client{Timeout: httpQueryTimeout},
	}
}

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

// httpSessionItem mirrors api.SessionItem for JSON deserialization: capture
// identity at the top level, the deriver-owned projection nested under
// `rollup`. We do not import the api package to avoid pkg/deck depending on
// a server-side package.
type httpSessionItem struct {
	ID         string            `json:"id"`
	HarnessID  string            `json:"harness_id"`
	Cwd        string            `json:"cwd,omitempty"`
	StartedAt  time.Time         `json:"started_at"`
	LastSeenAt time.Time         `json:"last_seen_at"`
	EndedAt    *time.Time        `json:"ended_at,omitempty"`
	Rollup     httpSessionRollup `json:"rollup"`
}

// httpSessionRollup mirrors api.SessionRollup — the deriver-owned session
// projection (status, title, spend), split from capture identity on the
// wire so the client can't blur which layer owns a field.
type httpSessionRollup struct {
	Status    string           `json:"status"`
	Title     string           `json:"title,omitempty"`
	Preview   string           `json:"preview,omitempty"`
	TurnCount int              `json:"turn_count"`
	Model     string           `json:"model,omitempty"`
	Usage     httpSessionUsage `json:"usage"`
}

// httpSessionUsage mirrors api.SessionUsage — the session's total spend.
type httpSessionUsage struct {
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

// httpSessionListResponse mirrors api.SessionListResponse.
type httpSessionListResponse struct {
	Items      []httpSessionItem `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

// httpSessionDetailResponse mirrors api.SessionDetailResponse.
type httpSessionDetailResponse struct {
	Session httpSessionItem `json:"session"`
}

// httpTraceItem mirrors api.TraceItem (one turn header). Trace token
// totals live under `usage`; the conversation-spine slice under
// `main_usage`. session_id is not on the wire — it belongs to the session.
type httpTraceItem struct {
	TraceID         string         `json:"trace_id"`
	UserPrompt      string         `json:"user_prompt,omitempty"`
	ResponsePreview string         `json:"response_preview,omitempty"`
	Status          string         `json:"status"`
	Source          string         `json:"source"`
	StartedAt       time.Time      `json:"started_at"`
	EndedAt         *time.Time     `json:"ended_at,omitempty"`
	DurationNS      int64          `json:"duration_ns"`
	SpanCount       int            `json:"span_count"`
	Usage           httpTraceUsage `json:"usage"`
	MainUsage       httpMainUsage  `json:"main_usage"`
	Synthetic       string         `json:"synthetic,omitempty"`
}

// httpTraceUsage mirrors api.TraceUsage — a trace's total token/cost spend.
type httpTraceUsage struct {
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	CostUSD             float64 `json:"cost_usd"`
}

// httpMainUsage mirrors api.MainUsage — the conversation-spine slice.
type httpMainUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// httpTraceListResponse mirrors api.TraceListResponse.
type httpTraceListResponse struct {
	Items []httpTraceItem `json:"items"`
}

// httpSpanItem mirrors api.SpanItem. Input and Output are content-block
// arrays uniform for every kind (tool spans included — no unwrapping); the
// harness taxonomy (call_kind, model, thread_id, …) is typed rather than
// bagged in a metadata map; usage is an object (was `metrics`).
type httpSpanItem struct {
	SpanID     string          `json:"span_id"`
	Kind       string          `json:"kind"`
	Name       string          `json:"name"`
	StartedAt  time.Time       `json:"started_at"`
	DurationNS int64           `json:"duration_ns"`
	Seq        int64           `json:"seq"`
	CallKind   string          `json:"call_kind"`
	Model      string          `json:"model"`
	ThreadID   string          `json:"thread_id"`
	Input      json.RawMessage `json:"input"`
	Output     json.RawMessage `json:"output"`
	Usage      json.RawMessage `json:"usage"`
}

// httpTraceDetailResponse mirrors api.TraceDetail.
type httpTraceDetailResponse struct {
	Trace httpTraceItem  `json:"trace"`
	Spans []httpSpanItem `json:"spans"`
}

// Overview fetches a single recent page from /v1/sessions and applies the
// deck-side filtering and rollup logic to that bounded result set.
//
// Time bounds (since/from, to/until) are pushed down as query params so the
// server narrows the page before returning it. Model, status, and project
// filters are evaluated client-side against the page: the sessions list
// endpoint does not filter on them.
func (q *HTTPQuery) Overview(ctx context.Context, filters Filters) (*Overview, error) {
	page, err := q.OverviewPage(ctx, filters, "", httpQueryOverviewLimit)
	if err != nil {
		return nil, err
	}
	return page.Overview, nil
}

// OverviewPage fetches one bounded page from /v1/sessions and returns the
// API cursor needed to request the next page. The first page replaces the
// summary cache; subsequent pages merge into it so detail lookups keep
// working for every loaded row.
func (q *HTTPQuery) OverviewPage(ctx context.Context, filters Filters, cursor string, limit int) (*OverviewPage, error) {
	if limit <= 0 {
		limit = httpQueryOverviewLimit
	}

	page, err := q.fetchSessionPage(ctx, cursor, filters, limit)
	if err != nil {
		return nil, err
	}

	summaries := make([]SessionSummary, len(page.Items))
	for i, item := range page.Items {
		summaries[i] = summaryFromSessionItem(item)
	}
	if cursor == "" {
		q.cache.storeSummaries(summaries)
	} else {
		q.cache.appendSummaries(summaries)
	}

	overview := buildOverviewFromSummaries(summaries, filters)
	return &OverviewPage{
		Overview:   overview,
		NextCursor: page.NextCursor,
		HasMore:    page.NextCursor != "",
	}, nil
}

// SessionDetail fetches the turn summaries for one session via
// /v1/traces?session_id= and renders them as the session transcript. The
// SessionSummary comes from the overview cache when warm, falling back to
// GET /v1/sessions/{id} on a cold cache.
func (q *HTTPQuery) SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
	turns, err := q.fetchTraceSummaries(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	summary, err := q.summaryForID(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	turnSummaries := make([]TurnSummary, len(turns))
	for i, t := range turns {
		turnSummaries[i] = turnSummaryFromTraceItem(t)
	}

	messages := messagesFromTurns(turnSummaries, summary.Model)
	return &SessionDetail{
		Summary:         summary,
		Turns:           turnSummaries,
		Messages:        messages,
		GroupedMessages: buildGroupedMessages(messages),
	}, nil
}

// TurnConversation fetches one turn's spans via /v1/traces/{trace_id} and
// reconstructs the conversation: the main (conversation-spine) llm calls'
// input/output blocks in seq order. Offshoot and injected spans are counted
// rather than interleaved; tool spans feed the tool-frequency rollup.
func (q *HTTPQuery) TurnConversation(ctx context.Context, traceID string) (*TurnConversation, error) {
	detail, err := q.fetchTraceDetail(ctx, traceID)
	if err != nil {
		return nil, err
	}

	conv := &TurnConversation{
		Turn:          turnSummaryFromTraceItem(detail.Trace),
		ToolFrequency: map[string]int{},
	}

	var lastTime time.Time
	for _, sp := range detail.Spans {
		callKind := sp.CallKind
		switch sp.Kind {
		case "llm":
			switch {
			case callKind == "main" || callKind == "":
				conv.Messages = append(conv.Messages, q.messagesFromLLMSpan(detail.Trace.TraceID, sp, &lastTime)...)
			case strings.HasPrefix(callKind, "injected:"):
				conv.InjectedContexts++
			default:
				conv.OffshootCalls++
			}
		case "tool":
			if sp.Name != "" {
				conv.ToolFrequency[sp.Name]++
			}
		case "event":
			if strings.HasPrefix(callKind, "injected:") {
				conv.InjectedContexts++
			}
		}
	}

	conv.GroupedMessages = buildGroupedMessages(conv.Messages)
	return conv, nil
}

// messagesFromLLMSpan renders one conversation-spine llm call as up to two
// transcript rows: the delta user content that prompted the call, then the
// assistant output. Cost is estimated client-side from the span's usage and
// model via the pricing table.
func (q *HTTPQuery) messagesFromLLMSpan(traceID string, sp httpSpanItem, lastTime *time.Time) []SessionMessage {
	model := sessions.NormalizeModel(sp.Model)

	var messages []SessionMessage
	if inBlocks := decodeContentBlocks(sp.Input); len(inBlocks) > 0 {
		if text := strings.TrimSpace(sessions.ExtractText(inBlocks)); text != "" {
			delta := time.Duration(0)
			if !lastTime.IsZero() {
				delta = sp.StartedAt.Sub(*lastTime)
			}
			*lastTime = sp.StartedAt
			messages = append(messages, SessionMessage{
				TraceID:   traceID,
				Hash:      sp.SpanID + ":input",
				Role:      "user",
				Model:     model,
				Timestamp: sp.StartedAt,
				Delta:     delta,
				Text:      text,
			})
		}
	}

	tokens := tokensFromMetrics(sp.Usage)
	var inputCost, outputCost, totalCost float64
	if model != "" {
		if price, ok := sessions.PricingForModel(q.pricing, model); ok {
			inputCost, outputCost, totalCost = sessions.CostForTokensWithCache(price, tokens.Input, tokens.Output, tokens.CacheCreation, tokens.CacheRead)
		}
	}

	outBlocks := decodeContentBlocks(sp.Output)
	endTime := sp.StartedAt.Add(time.Duration(sp.DurationNS))
	delta := time.Duration(0)
	if !lastTime.IsZero() {
		delta = endTime.Sub(*lastTime)
	}
	*lastTime = endTime
	messages = append(messages, SessionMessage{
		TraceID:      traceID,
		Hash:         sp.SpanID + ":output",
		Role:         "assistant",
		Model:        model,
		Timestamp:    endTime,
		Delta:        delta,
		InputTokens:  tokens.Input,
		OutputTokens: tokens.Output,
		TotalTokens:  tokens.Total,
		InputCost:    inputCost,
		OutputCost:   outputCost,
		TotalCost:    totalCost,
		ToolCalls:    sessions.ExtractToolCalls(outBlocks),
		Text:         sessions.ExtractText(outBlocks),
	})
	return messages
}

// buildOverviewFromSummaries applies the client-side filters and computes
// the dashboard rollups over one loaded page of session rows.
func buildOverviewFromSummaries(summaries []SessionSummary, filters Filters) *Overview {
	overview := &Overview{
		Sessions:    make([]SessionSummary, 0, len(summaries)),
		CostByModel: map[string]ModelCost{},
	}
	for _, summary := range summaries {
		if !matchesFilters(summary, filters) {
			continue
		}

		overview.Sessions = append(overview.Sessions, summary)
		overview.TotalCost += summary.TotalCost
		overview.InputTokens += summary.InputTokens
		overview.OutputTokens += summary.OutputTokens
		overview.TotalTokens += summary.InputTokens + summary.OutputTokens
		overview.TotalDuration += summary.Duration
		overview.TotalTurns += summary.MessageCount

		switch summary.Status {
		case StatusCompleted:
			overview.Completed++
		case StatusFailed:
			overview.Failed++
		case StatusAbandoned:
			overview.Abandoned++
		}

		aggregate := overview.CostByModel[summary.Model]
		aggregate.Model = summary.Model
		aggregate.InputTokens += summary.InputTokens
		aggregate.OutputTokens += summary.OutputTokens
		aggregate.TotalCost += summary.TotalCost
		aggregate.SessionCount++
		overview.CostByModel[summary.Model] = aggregate
	}

	if total := len(overview.Sessions); total > 0 {
		overview.SuccessRate = float64(overview.Completed) / float64(total)
	}

	SortSessions(overview.Sessions, filters.Sort, filters.SortDir)
	return overview
}

func (q *HTTPQuery) fetchSessionPage(ctx context.Context, cursor string, filters Filters, limit int) (*httpSessionListResponse, error) {
	u, err := url.Parse(q.apiTarget + "/v1/sessions")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	qparams := u.Query()
	qparams.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		qparams.Set("cursor", cursor)
	}
	if cutoff := effectiveSinceCutoff(filters); !cutoff.IsZero() {
		qparams.Set("since", cutoff.UTC().Format(time.RFC3339))
	}
	if filters.To != nil {
		qparams.Set("until", filters.To.UTC().Format(time.RFC3339))
	}
	u.RawQuery = qparams.Encode()

	var page httpSessionListResponse
	if err := q.getJSON(ctx, u.String(), &page); err != nil {
		return nil, fmt.Errorf("fetching sessions: %w", err)
	}
	return &page, nil
}

func (q *HTTPQuery) fetchSessionItem(ctx context.Context, sessionID string) (*httpSessionItem, error) {
	var resp httpSessionDetailResponse
	if err := q.getJSON(ctx, q.apiTarget+"/v1/sessions/"+url.PathEscape(sessionID), &resp); err != nil {
		return nil, fmt.Errorf("fetching session %s: %w", sessionID, err)
	}
	return &resp.Session, nil
}

func (q *HTTPQuery) fetchTraceSummaries(ctx context.Context, sessionID string) ([]httpTraceItem, error) {
	u, err := url.Parse(q.apiTarget + "/v1/traces")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	qparams := u.Query()
	qparams.Set("session_id", sessionID)
	u.RawQuery = qparams.Encode()

	var resp httpTraceListResponse
	if err := q.getJSON(ctx, u.String(), &resp); err != nil {
		return nil, fmt.Errorf("fetching traces for session %s: %w", sessionID, err)
	}
	return resp.Items, nil
}

func (q *HTTPQuery) fetchTraceDetail(ctx context.Context, traceID string) (*httpTraceDetailResponse, error) {
	var resp httpTraceDetailResponse
	if err := q.getJSON(ctx, q.apiTarget+"/v1/traces/"+url.PathEscape(traceID), &resp); err != nil {
		return nil, fmt.Errorf("fetching trace %s: %w", traceID, err)
	}
	return &resp, nil
}

// getJSON performs one GET and decodes the 200 response into out.
func (q *HTTPQuery) getJSON(ctx context.Context, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return err
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

// summaryForID returns the cached summary for sessionID if available,
// falling back to GET /v1/sessions/{id} on a cold cache (e.g. when the deck
// was launched straight into a session with --session).
func (q *HTTPQuery) summaryForID(ctx context.Context, sessionID string) (SessionSummary, error) {
	if cached := q.cache.cachedSummary(sessionID); cached != nil {
		return *cached, nil
	}
	item, err := q.fetchSessionItem(ctx, sessionID)
	if err != nil {
		return SessionSummary{}, err
	}
	return summaryFromSessionItem(*item), nil
}

// summaryFromSessionItem adapts one /v1/sessions row onto the shared
// SessionSummary shape the TUI renders. The sessions table is the product
// grain: rollups (tokens, cost, turn count, status, model) are folded at
// derive time, so no client-side aggregation happens here.
func summaryFromSessionItem(item httpSessionItem) SessionSummary {
	end := item.LastSeenAt
	if item.EndedAt != nil {
		end = *item.EndedAt
	}
	duration := max(end.Sub(item.StartedAt), 0)
	return SessionSummary{
		ID:           item.ID,
		Label:        sessionLabel(item),
		Model:        item.Rollup.Model,
		Project:      projectFromCwd(item.Cwd),
		AgentName:    item.HarnessID,
		Status:       item.Rollup.Status,
		StartTime:    item.StartedAt,
		EndTime:      end,
		Duration:     duration,
		InputTokens:  item.Rollup.Usage.InputTokens,
		OutputTokens: item.Rollup.Usage.OutputTokens,
		TotalCost:    item.Rollup.Usage.CostUSD,
		MessageCount: item.Rollup.TurnCount,
		SessionCount: 1,
	}
}

// sessionLabel picks the row label: the session's derived title when set,
// else the first line of the first-prompt preview, else a short form of
// the id. Title and preview are both deriver-owned rollup fields.
func sessionLabel(item httpSessionItem) string {
	if title := strings.TrimSpace(item.Rollup.Title); title != "" {
		return title
	}
	if preview := firstLine(item.Rollup.Preview); preview != "" {
		return truncateID(preview, 48)
	}
	return truncateID(item.ID, 12)
}

func firstLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}

// projectFromCwd derives a short project name from the session's working
// directory. The sessions table has no project column; the basename of cwd
// is the closest product-grain equivalent of the old stem project field.
func projectFromCwd(cwd string) string {
	cwd = strings.TrimRight(strings.TrimSpace(cwd), "/")
	if cwd == "" || cwd == "/" {
		return ""
	}
	return path.Base(cwd)
}

// turnSummaryFromTraceItem adapts one /v1/traces row to the deck shape.
func turnSummaryFromTraceItem(item httpTraceItem) TurnSummary {
	return TurnSummary{
		TraceID:             item.TraceID,
		UserPrompt:          item.UserPrompt,
		ResponsePreview:     item.ResponsePreview,
		Status:              item.Status,
		StartedAt:           item.StartedAt,
		EndedAt:             item.EndedAt,
		Duration:            time.Duration(item.DurationNS),
		InputTokens:         item.Usage.InputTokens,
		OutputTokens:        item.Usage.OutputTokens,
		MainInputTokens:     item.MainUsage.InputTokens,
		MainOutputTokens:    item.MainUsage.OutputTokens,
		CacheReadTokens:     item.Usage.CacheReadTokens,
		CacheCreationTokens: item.Usage.CacheCreationTokens,
		TotalCost:           item.Usage.CostUSD,
		SpanCount:           item.SpanCount,
	}
}

// messagesFromTurns renders the session transcript at turn grain: each turn
// contributes a user row (the prompt) and an assistant row (the response
// preview, carrying the turn's folded tokens/cost and its duration as the
// row delta). Transcript consumers — the TUI conversation table and the
// skill generator — read this shape without knowing about spans.
func messagesFromTurns(turns []TurnSummary, model string) []SessionMessage {
	messages := make([]SessionMessage, 0, len(turns)*2)
	var lastTime time.Time
	for _, turn := range turns {
		userDelta := time.Duration(0)
		if !lastTime.IsZero() {
			userDelta = turn.StartedAt.Sub(lastTime)
		}
		messages = append(messages, SessionMessage{
			TraceID:   turn.TraceID,
			Hash:      turn.TraceID + ":prompt",
			Role:      "user",
			Timestamp: turn.StartedAt,
			Delta:     userDelta,
			Text:      turn.UserPrompt,
		})

		end := turn.StartedAt.Add(turn.Duration)
		if turn.EndedAt != nil {
			end = *turn.EndedAt
		}
		messages = append(messages, SessionMessage{
			TraceID:      turn.TraceID,
			Hash:         turn.TraceID + ":response",
			Role:         "assistant",
			Model:        model,
			Timestamp:    end,
			Delta:        turn.Duration,
			InputTokens:  turn.InputTokens,
			OutputTokens: turn.OutputTokens,
			TotalTokens:  turn.InputTokens + turn.OutputTokens,
			TotalCost:    turn.TotalCost,
			Text:         turn.ResponsePreview,
		})
		lastTime = end
	}
	return messages
}

// decodeContentBlocks unmarshals a stored content-block array (missing /
// null / malformed → nil).
func decodeContentBlocks(raw json.RawMessage) []llm.ContentBlock {
	if len(raw) == 0 {
		return nil
	}
	var blocks []llm.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil
	}
	return blocks
}

// tokensFromMetrics extracts token usage from a span's usage object
// (llm.Usage JSON; {} for usage-less spans).
func tokensFromMetrics(raw json.RawMessage) sessions.NodeTokens {
	var nt sessions.NodeTokens
	if len(raw) == 0 {
		return nt
	}
	var usage llm.Usage
	if err := json.Unmarshal(raw, &usage); err != nil {
		return nt
	}
	nt.Input = int64(usage.PromptTokens)
	nt.Output = int64(usage.CompletionTokens)
	nt.CacheCreation = int64(usage.CacheCreationInputTokens)
	nt.CacheRead = int64(usage.CacheReadInputTokens)
	nt.Total = nt.Input + nt.Output
	if usage.TotalTokens > 0 {
		nt.Total = int64(usage.TotalTokens)
	}
	return nt
}

// effectiveSinceCutoff returns the timestamp below which sessions should
// be excluded, derived from the deck's relative Filters.Since (a duration
// from now) and absolute Filters.From bounds. When both are set the later
// of the two wins, matching the client-side behaviour in matchesFilters so
// the server-side and client-side passes agree on the boundary.
//
// Returns the zero time when no cutoff applies; callers should treat that
// as "do not send a since query param".
func effectiveSinceCutoff(f Filters) time.Time {
	var cutoff time.Time
	if f.Since > 0 {
		cutoff = time.Now().Add(-f.Since)
	}
	if f.From != nil && (cutoff.IsZero() || f.From.After(cutoff)) {
		cutoff = *f.From
	}
	return cutoff
}

// truncateID returns a short prefix of value followed by an ellipsis when
// value exceeds limit. Used as a label fallback when no human prompt is
// available to derive a friendlier label from.
func truncateID(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}
