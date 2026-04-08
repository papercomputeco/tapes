package deck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

const (
	httpQueryTimeout    = 30 * time.Second
	httpQueryPageLimit  = 200
	httpQueryMaxSummary = 10000 // safety cap on total sessions paged through
)

// HTTPQuery is a Querier implementation that talks to a remote (or
// in-process) tapes API server over HTTP. It mirrors the Querier surface of
// the legacy SQLite-backed Query type so callers can swap implementations
// without changes.
type HTTPQuery struct {
	apiTarget string
	pricing   PricingTable
	client    *http.Client
	cache     sessionCache
}

// Compile-time check that HTTPQuery satisfies the Querier interface.
var _ Querier = (*HTTPQuery)(nil)

// NewHTTPQuery constructs an HTTPQuery pointed at apiTarget (e.g.
// "http://127.0.0.1:8081"). The pricing table is retained for client-side
// recalculation if needed; in practice the API server already returns
// fully-populated SessionSummary objects.
func NewHTTPQuery(apiTarget string, pricing PricingTable) *HTTPQuery {
	return &HTTPQuery{
		apiTarget: strings.TrimRight(apiTarget, "/"),
		pricing:   pricing,
		client:    &http.Client{Timeout: httpQueryTimeout},
	}
}

// httpSummaryResponse mirrors api.SessionSummaryListResponse for JSON
// deserialization. We do not import the api package to avoid pkg/deck
// depending on a server-side package.
type httpSummaryResponse struct {
	Items      []SessionSummary `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// httpSessionResponse mirrors api.SessionResponse.
type httpSessionResponse struct {
	Hash  string     `json:"hash"`
	Depth int        `json:"depth"`
	Turns []httpTurn `json:"turns"`
}

// httpTurn mirrors api.Turn.
type httpTurn struct {
	Hash       string             `json:"hash"`
	ParentHash *string            `json:"parent_hash,omitempty"`
	Role       string             `json:"role"`
	Content    []llm.ContentBlock `json:"content"`
	Model      string             `json:"model,omitempty"`
	Provider   string             `json:"provider,omitempty"`
	AgentName  string             `json:"agent_name,omitempty"`
	StopReason string             `json:"stop_reason,omitempty"`
	Usage      *llm.Usage         `json:"usage,omitempty"`
	CreatedAt  time.Time          `json:"created_at,omitzero"`
}

// Overview fetches all session summaries from the API (paging through with
// /v1/sessions/summary), then runs the existing deck-side grouping, filtering
// and rollup logic on top of the returned data.
func (q *HTTPQuery) Overview(ctx context.Context, filters Filters) (*Overview, error) {
	all, err := q.fetchAllSummaries(ctx)
	if err != nil {
		return nil, err
	}

	candidates := candidatesFromSummaries(all)
	q.cache.storeSessionCandidates(candidates)

	candidates = preFilterCandidatesByTime(candidates, filters)
	groups := groupSessionCandidates(candidates)

	overview := &Overview{
		Sessions:    make([]SessionSummary, 0, len(groups)),
		CostByModel: map[string]ModelCost{},
	}
	for _, group := range groups {
		summary := group.summary
		if !matchesFilters(summary, filters) {
			continue
		}

		overview.Sessions = append(overview.Sessions, summary)
		overview.TotalCost += summary.TotalCost
		overview.InputTokens += summary.InputTokens
		overview.OutputTokens += summary.OutputTokens
		overview.TotalTokens += summary.InputTokens + summary.OutputTokens
		overview.TotalDuration += summary.Duration
		overview.TotalToolCalls += summary.ToolCalls

		switch summary.Status {
		case StatusCompleted:
			overview.Completed++
		case StatusFailed:
			overview.Failed++
		case StatusAbandoned:
			overview.Abandoned++
		}

		for model, cost := range group.modelCosts {
			aggregate := overview.CostByModel[model]
			aggregate.Model = model
			aggregate.InputTokens += cost.InputTokens
			aggregate.OutputTokens += cost.OutputTokens
			aggregate.InputCost += cost.InputCost
			aggregate.OutputCost += cost.OutputCost
			aggregate.TotalCost += cost.TotalCost
			aggregate.SessionCount += cost.SessionCount
			overview.CostByModel[model] = aggregate
		}
	}

	if total := len(overview.Sessions); total > 0 {
		overview.SuccessRate = float64(overview.Completed) / float64(total)
	}

	SortSessions(overview.Sessions, filters.Sort, filters.SortDir)
	return overview, nil
}

// SessionDetail fetches the chain for a single session via /v1/sessions/:hash
// and renders the per-turn message data using the cached SessionSummary for
// the same ID. Group IDs (synthetic IDs from groupSessionCandidates) are
// resolved against the cached candidates.
func (q *HTTPQuery) SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
	if isGroupID(sessionID) {
		return q.groupSessionDetail(ctx, sessionID)
	}

	chain, err := q.fetchSessionChain(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	summary := q.summaryForID(sessionID, chain)
	messages, toolFreq := buildHTTPSessionMessages(chain, q.pricing)
	grouped := buildGroupedMessages(messages)
	return &SessionDetail{
		Summary:         summary,
		Messages:        messages,
		GroupedMessages: grouped,
		ToolFrequency:   toolFreq,
	}, nil
}

// groupSessionDetail merges the chains of every leaf belonging to a synthetic
// group ID. Falls back to a fresh Overview load if the cache is empty/stale.
func (q *HTTPQuery) groupSessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
	cached := q.cache.cachedSessionCandidates()
	if cached == nil {
		// Force a refresh so groupSessionCandidates has data to work with.
		if _, err := q.Overview(ctx, Filters{}); err != nil {
			return nil, err
		}
		cached = q.cache.cachedSessionCandidates()
		if cached == nil {
			return nil, fmt.Errorf("group %s not found: empty session set", sessionID)
		}
	}

	groups := groupSessionCandidates(cached)
	target := findGroupByID(groups, sessionID)
	if target == nil {
		return nil, fmt.Errorf("get session group: %s", sessionID)
	}

	// Fetch each member's chain and merge in chronological order.
	allTurns := make([]httpTurn, 0)
	for _, member := range target.members {
		chain, err := q.fetchSessionChain(ctx, member.summary.ID)
		if err != nil {
			return nil, fmt.Errorf("fetching member %s: %w", member.summary.ID, err)
		}
		allTurns = append(allTurns, chain...)
	}
	// Stable sort by created_at then hash so the same input always renders identically.
	sortTurnsByTime(allTurns)

	messages, toolFreq := buildHTTPSessionMessages(allTurns, q.pricing)
	grouped := buildGroupedMessages(messages)

	subSessions := make([]SessionSummary, 0, len(target.members))
	for _, member := range target.members {
		subSessions = append(subSessions, member.summary)
	}

	return &SessionDetail{
		Summary:         target.summary,
		Messages:        messages,
		GroupedMessages: grouped,
		ToolFrequency:   toolFreq,
		SubSessions:     subSessions,
	}, nil
}

// fetchAllSummaries pages through /v1/sessions/summary until the API
// returns no NextCursor, returning every session it finds. Capped at
// httpQueryMaxSummary to avoid pathological memory growth.
func (q *HTTPQuery) fetchAllSummaries(ctx context.Context) ([]SessionSummary, error) {
	var all []SessionSummary
	cursor := ""
	for {
		page, err := q.fetchSummaryPage(ctx, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextCursor == "" {
			break
		}
		if len(all) >= httpQueryMaxSummary {
			break
		}
		cursor = page.NextCursor
	}
	return all, nil
}

func (q *HTTPQuery) fetchSummaryPage(ctx context.Context, cursor string) (*httpSummaryResponse, error) {
	u, err := url.Parse(q.apiTarget + "/v1/sessions/summary")
	if err != nil {
		return nil, fmt.Errorf("invalid api target: %w", err)
	}
	qparams := u.Query()
	qparams.Set("limit", strconv.Itoa(httpQueryPageLimit))
	if cursor != "" {
		qparams.Set("cursor", cursor)
	}
	u.RawQuery = qparams.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching session summaries: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("api returned status %d: %s", resp.StatusCode, string(body))
	}

	var page httpSummaryResponse
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decoding summary response: %w", err)
	}
	return &page, nil
}

func (q *HTTPQuery) fetchSessionChain(ctx context.Context, hash string) ([]httpTurn, error) {
	u := q.apiTarget + "/v1/sessions/" + url.PathEscape(hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching session chain: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("session %s not found", hash)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("api returned status %d: %s", resp.StatusCode, string(body))
	}

	var sess httpSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&sess); err != nil {
		return nil, fmt.Errorf("decoding session response: %w", err)
	}
	return sess.Turns, nil
}

// summaryForID returns the cached summary for sessionID if available.
// Falls back to a synthesised minimal summary built from the chain so the
// detail view can still render even on a cold cache.
func (q *HTTPQuery) summaryForID(sessionID string, chain []httpTurn) SessionSummary {
	if c := q.cache.cachedSessionCandidate(sessionID); c != nil {
		return c.summary
	}
	if len(chain) == 0 {
		return SessionSummary{ID: sessionID}
	}
	// Minimal fallback so the detail view has at least the basic info.
	leaf := chain[len(chain)-1]
	return SessionSummary{
		ID:           sessionID,
		Label:        truncateID(sessionID, 12),
		Model:        leaf.Model,
		AgentName:    leaf.AgentName,
		StartTime:    chain[0].CreatedAt,
		EndTime:      leaf.CreatedAt,
		Duration:     leaf.CreatedAt.Sub(chain[0].CreatedAt),
		MessageCount: len(chain),
	}
}

// candidatesFromSummaries adapts the API's SessionSummary list into the
// existing sessionCandidate shape used by client-side grouping. The nodes
// field is left nil; HTTPQuery's group detail path uses Hash to refetch
// chains lazily rather than carrying them through the cache.
func candidatesFromSummaries(items []SessionSummary) []sessionCandidate {
	out := make([]sessionCandidate, len(items))
	for i, s := range items {
		out[i] = sessionCandidate{
			summary: s,
			modelCosts: map[string]ModelCost{
				s.Model: {
					Model:        s.Model,
					InputTokens:  s.InputTokens,
					OutputTokens: s.OutputTokens,
					InputCost:    s.InputCost,
					OutputCost:   s.OutputCost,
					TotalCost:    s.TotalCost,
					SessionCount: 1,
				},
			},
			status: s.Status,
		}
	}
	return out
}

// buildHTTPSessionMessages renders the per-turn SessionMessage list for the
// detail view. Per-turn cost is computed using pkg/sessions pricing helpers.
func buildHTTPSessionMessages(turns []httpTurn, pricing PricingTable) ([]SessionMessage, map[string]int) {
	messages := make([]SessionMessage, 0, len(turns))
	toolFrequency := map[string]int{}

	var lastTime time.Time
	var lastModel string
	for i, t := range turns {
		tokens := tokensForTurn(t)

		model := sessions.NormalizeModel(t.Model)
		if model == "" {
			model = lastModel
		}
		if model != "" {
			lastModel = model
		}

		var inputCost, outputCost, totalCost float64
		if model != "" {
			if price, ok := sessions.PricingForModel(pricing, model); ok {
				inputCost, outputCost, totalCost = sessions.CostForTokensWithCache(price, tokens.Input, tokens.Output, tokens.CacheCreation, tokens.CacheRead)
			}
		}

		toolCalls := sessions.ExtractToolCalls(t.Content)
		for _, tool := range toolCalls {
			toolFrequency[tool]++
		}

		text := sessions.ExtractText(t.Content)
		delta := time.Duration(0)
		if i > 0 {
			delta = t.CreatedAt.Sub(lastTime)
		}
		lastTime = t.CreatedAt

		messages = append(messages, SessionMessage{
			Hash:         t.Hash,
			Role:         t.Role,
			Model:        model,
			Timestamp:    t.CreatedAt,
			Delta:        delta,
			InputTokens:  tokens.Input,
			OutputTokens: tokens.Output,
			TotalTokens:  tokens.Total,
			InputCost:    inputCost,
			OutputCost:   outputCost,
			TotalCost:    totalCost,
			ToolCalls:    toolCalls,
			Text:         text,
		})
	}

	return messages, toolFrequency
}

// tokensForTurn extracts token usage from a Turn's optional Usage struct.
func tokensForTurn(t httpTurn) sessions.NodeTokens {
	var nt sessions.NodeTokens
	if t.Usage == nil {
		return nt
	}
	nt.Input = int64(t.Usage.PromptTokens)
	nt.Output = int64(t.Usage.CompletionTokens)
	nt.CacheCreation = int64(t.Usage.CacheCreationInputTokens)
	nt.CacheRead = int64(t.Usage.CacheReadInputTokens)
	nt.Total = nt.Input + nt.Output
	if t.Usage.TotalTokens > 0 {
		nt.Total = int64(t.Usage.TotalTokens)
	}
	return nt
}

// sortTurnsByTime sorts turns in place by CreatedAt, then by hash for stable
// ordering when timestamps are equal.
func sortTurnsByTime(turns []httpTurn) {
	if len(turns) < 2 {
		return
	}
	// Stable insertion sort is fine here; merged groups are typically small.
	for i := 1; i < len(turns); i++ {
		for j := i; j > 0 && turnLess(turns[j], turns[j-1]); j-- {
			turns[j], turns[j-1] = turns[j-1], turns[j]
		}
	}
}

func turnLess(a, b httpTurn) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.Hash < b.Hash
}

// ErrEmptyChain is returned when SessionDetail receives an empty chain back
// from the API. Exported so callers can branch on it if needed.
var ErrEmptyChain = errors.New("empty session chain")

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
