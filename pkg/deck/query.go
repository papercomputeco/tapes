package deck

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// Querier is an interface for querying session data.
// This allows for mock implementations in testing and sandboxes.
type Querier interface {
	Overview(ctx context.Context, filters Filters) (*Overview, error)
	SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error)
}

type Query struct {
	client  *ent.Client
	pricing PricingTable
	cache   sessionCache
}

// Ensure Query implements Querier
var _ Querier = (*Query)(nil)

func NewQuery(ctx context.Context, dbPath string, pricing PricingTable) (*Query, func() error, error) {
	driver, err := sqlite.NewDriver(ctx, dbPath)
	if err != nil {
		return nil, nil, err
	}

	if err := driver.Migrate(ctx); err != nil {
		driver.Close()
		return nil, nil, fmt.Errorf("running migrations: %w", err)
	}

	closeFn := func() error {
		return driver.Close()
	}

	return &Query{client: driver.Client, pricing: pricing}, closeFn, nil
}

type sessionCandidate struct {
	summary    SessionSummary
	modelCosts map[string]ModelCost
	status     string
	nodes      []*ent.Node
}

func (q *Query) loadSessionCandidates(ctx context.Context, allowCache bool) ([]sessionCandidate, error) {
	if allowCache {
		if cached := q.cachedSessionCandidates(); cached != nil {
			return cached, nil
		}
	}

	// Bulk-load all nodes in a single query and build ancestry chains
	// in memory. This replaces the previous N+1 pattern where each leaf
	// called loadAncestry with individual parent queries.
	allNodes, err := q.client.Node.Query().Select(
		node.FieldParentHash, node.FieldRole, node.FieldContent,
		node.FieldModel, node.FieldProvider, node.FieldAgentName,
		node.FieldStopReason, node.FieldPromptTokens, node.FieldCompletionTokens,
		node.FieldTotalTokens, node.FieldCacheCreationInputTokens,
		node.FieldCacheReadInputTokens, node.FieldProject, node.FieldCreatedAt,
	).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("load nodes: %w", err)
	}

	byID := make(map[string]*ent.Node, len(allNodes))
	hasChildren := make(map[string]bool)
	for _, n := range allNodes {
		byID[n.ID] = n
		if n.ParentHash != nil && *n.ParentHash != "" {
			hasChildren[*n.ParentHash] = true
		}
	}

	candidates := make([]sessionCandidate, 0)
	for _, n := range allNodes {
		if hasChildren[n.ID] {
			continue
		}

		chain := buildAncestryChain(n, byID)
		summary, modelCosts, status, err := q.buildSessionSummaryFromNodes(chain)
		if err != nil {
			continue
		}

		candidates = append(candidates, sessionCandidate{
			summary:    summary,
			modelCosts: modelCosts,
			status:     status,
			nodes:      chain,
		})
	}

	q.storeSessionCandidates(candidates)
	return candidates, nil
}

// buildAncestryChain walks from a leaf to root using the in-memory node map,
// returning nodes in root-first order.
func buildAncestryChain(leaf *ent.Node, byID map[string]*ent.Node) []*ent.Node {
	chain := []*ent.Node{}
	seen := map[string]bool{}
	current := leaf
	for current != nil {
		if seen[current.ID] {
			break
		}
		seen[current.ID] = true
		chain = append(chain, current)
		if current.ParentHash == nil || *current.ParentHash == "" {
			break
		}
		current = byID[*current.ParentHash]
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

func (q *Query) Overview(ctx context.Context, filters Filters) (*Overview, error) {
	candidates, err := q.loadSessionCandidates(ctx, true)
	if err != nil {
		return nil, err
	}

	// Pre-filter candidates by time before the expensive sort+group step.
	// This avoids O(N log N) sorting of all 30d data when only 24h is needed.
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

func (q *Query) SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
	if isGroupID(sessionID) {
		return q.groupSessionDetail(ctx, sessionID)
	}

	// Fast path: O(1) lookup in the cache index.
	if c := q.cachedSessionCandidate(sessionID); c != nil {
		messages, toolFrequency := q.buildSessionMessages(c.nodes)
		grouped := buildGroupedMessages(messages)
		return &SessionDetail{
			Summary:         c.summary,
			Messages:        messages,
			GroupedMessages: grouped,
			ToolFrequency:   toolFrequency,
		}, nil
	}

	// Slow path: reload candidates (cache miss or stale) and try again.
	candidates, err := q.loadSessionCandidates(ctx, false)
	if err != nil {
		return nil, err
	}
	if c, ok := candidateByID(candidates, sessionID); ok {
		messages, toolFrequency := q.buildSessionMessages(c.nodes)
		grouped := buildGroupedMessages(messages)
		return &SessionDetail{
			Summary:         c.summary,
			Messages:        messages,
			GroupedMessages: grouped,
			ToolFrequency:   toolFrequency,
		}, nil
	}

	// Fallback: session is brand-new and not yet in candidates.
	leaf, err := q.client.Node.Get(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}

	nodes, err := q.loadAncestry(ctx, leaf)
	if err != nil {
		return nil, err
	}

	summary, _, _, err := q.buildSessionSummaryFromNodes(nodes)
	if err != nil {
		return nil, err
	}

	messages, toolFrequency := q.buildSessionMessages(nodes)
	grouped := buildGroupedMessages(messages)
	return &SessionDetail{
		Summary:         summary,
		Messages:        messages,
		GroupedMessages: grouped,
		ToolFrequency:   toolFrequency,
	}, nil
}

func (q *Query) groupSessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
	candidates, err := q.loadSessionCandidates(ctx, true)
	if err != nil {
		return nil, err
	}

	groups := groupSessionCandidates(candidates)
	target := findGroupByID(groups, sessionID)
	if target == nil {
		return nil, fmt.Errorf("get session group: %s", sessionID)
	}

	nodes := groupNodes(target.members)
	messages, toolFrequency := q.buildSessionMessages(nodes)
	grouped := buildGroupedMessages(messages)

	subSessions := make([]SessionSummary, 0, len(target.members))
	for _, member := range target.members {
		subSessions = append(subSessions, member.summary)
	}
	sort.Slice(subSessions, func(i, j int) bool {
		return subSessions[i].StartTime.Before(subSessions[j].StartTime)
	})

	detail := &SessionDetail{
		Summary:         target.summary,
		Messages:        messages,
		GroupedMessages: grouped,
		ToolFrequency:   toolFrequency,
		SubSessions:     subSessions,
	}

	return detail, nil
}

func (q *Query) buildSessionSummaryFromNodes(nodes []*ent.Node) (SessionSummary, map[string]ModelCost, string, error) {
	if len(nodes) == 0 {
		return SessionSummary{}, nil, "", errors.New("empty session nodes")
	}

	start := nodes[0].CreatedAt
	end := nodes[len(nodes)-1].CreatedAt
	duration := max(end.Sub(start), 0)

	toolCalls := 0
	modelCosts := map[string]ModelCost{}
	inputTokens := int64(0)
	outputTokens := int64(0)

	// Parse content blocks once per node and collect label candidates
	// from user-role nodes (in forward order). Label building reverses
	// these later to pick the most recent prompts.
	var userLabels []userLabelCandidate

	hasToolError := false
	hasGitActivity := false
	var lastModel string
	for _, n := range nodes {
		blocks, _ := parseContentBlocks(n.Content)
		toolCalls += countToolCalls(blocks)
		if blocksHaveToolError(blocks) {
			hasToolError = true
		}
		if blocksHaveGitActivity(blocks) {
			hasGitActivity = true
		}

		// Collect label text from user-role nodes in the same pass
		if n.Role == roleUser {
			text := strings.TrimSpace(extractLabelText(blocks))
			if text != "" {
				userLabels = append(userLabels, userLabelCandidate{text: text})
			}
		}

		t := tokenCounts(n)
		inputTokens += t.Input
		outputTokens += t.Output

		model := normalizeModel(n.Model)
		if model == "" {
			model = lastModel
		}
		if model == "" {
			continue
		}
		lastModel = model

		pricing, ok := PricingForModel(q.pricing, model)
		if !ok {
			continue
		}

		inputCost, outputCost, totalCost := CostForTokensWithCache(pricing, t.Input, t.Output, t.CacheCreation, t.CacheRead)
		current := modelCosts[model]
		current.Model = model
		current.InputTokens += t.Input
		current.OutputTokens += t.Output
		current.InputCost += inputCost
		current.OutputCost += outputCost
		current.TotalCost += totalCost
		current.SessionCount = 1
		modelCosts[model] = current
	}

	// Build label from collected user prompts (most recent first)
	label := buildLabelFromCandidates(userLabels, nodes[len(nodes)-1].ID)

	model := dominantModel(modelCosts)
	if model == "" {
		model = firstModel(nodes)
	}
	inputCost, outputCost, totalCost := sumModelCosts(modelCosts)

	status := determineStatus(nodes[len(nodes)-1], hasToolError, hasGitActivity)

	// Extract project from the first node that has one set
	project := ""
	for _, n := range nodes {
		if n.Project != nil && *n.Project != "" {
			project = *n.Project
			break
		}
	}

	agentName := ""
	for _, n := range nodes {
		if n.AgentName != "" {
			agentName = n.AgentName
			break
		}
	}

	summary := SessionSummary{
		ID:           nodes[len(nodes)-1].ID,
		Label:        label,
		Model:        model,
		Project:      project,
		AgentName:    agentName,
		Status:       status,
		StartTime:    start,
		EndTime:      end,
		Duration:     duration,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		InputCost:    inputCost,
		OutputCost:   outputCost,
		TotalCost:    totalCost,
		ToolCalls:    toolCalls,
		MessageCount: len(nodes),
		SessionCount: 1,
	}

	return summary, modelCosts, status, nil
}

func (q *Query) loadAncestry(ctx context.Context, leaf *ent.Node) ([]*ent.Node, error) {
	nodes := []*ent.Node{}
	current := leaf
	for current != nil {
		nodes = append(nodes, current)
		parent, err := current.QueryParent().Only(ctx)
		if ent.IsNotFound(err) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("query parent: %w", err)
		}
		current = parent
	}

	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}

	return nodes, nil
}
