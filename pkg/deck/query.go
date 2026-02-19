package deck

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

const (
	blockTypeToolUse = "tool_use"
	roleAssistant    = "assistant"
	roleUser         = "user"
	groupIDPrefix    = "group:"
	groupWindow      = time.Hour
	sessionCacheTTL  = 10 * time.Second
)

// Querier is an interface for querying session data.
// This allows for mock implementations in testing and sandboxes.
type Querier interface {
	Overview(ctx context.Context, filters Filters) (*Overview, error)
	SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error)
	AnalyticsOverview(ctx context.Context, filters Filters) (*AnalyticsOverview, error)
	SessionAnalytics(ctx context.Context, sessionID string) (*SessionAnalytics, error)
}

type Query struct {
	client  *ent.Client
	pricing PricingTable
	cache   sessionCache
}

// Ensure Query implements Querier
var _ Querier = (*Query)(nil)

// EntClient returns the underlying ent client for use by subsystems
// like the facet store.
func (q *Query) EntClient() *ent.Client {
	return q.client
}

func NewQuery(ctx context.Context, dbPath string, pricing PricingTable) (*Query, func() error, error) {
	driver, err := sqlite.NewDriver(ctx, dbPath)
	if err != nil {
		return nil, nil, err
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

type sessionGroup struct {
	summary      SessionSummary
	modelCosts   map[string]ModelCost
	statusCounts map[string]int
	members      []sessionCandidate
}

type sessionCache struct {
	mu         sync.RWMutex
	candidates []sessionCandidate
	loadedAt   time.Time
}

func (q *Query) loadSessionCandidates(ctx context.Context, allowCache bool) ([]sessionCandidate, error) {
	if allowCache {
		if cached := q.cachedSessionCandidates(); cached != nil {
			return cached, nil
		}
	}

	leaves, err := q.client.Node.Query().Where(node.Not(node.HasChildren())).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list leaves: %w", err)
	}

	candidates := make([]sessionCandidate, 0, len(leaves))
	for _, leaf := range leaves {
		nodes, err := q.loadAncestry(ctx, leaf)
		if err != nil {
			return nil, err
		}

		summary, modelCosts, status, err := q.buildSessionSummaryFromNodes(nodes)
		if err != nil {
			continue
		}

		candidates = append(candidates, sessionCandidate{
			summary:    summary,
			modelCosts: modelCosts,
			status:     status,
			nodes:      nodes,
		})
	}

	q.storeSessionCandidates(candidates)
	return candidates, nil
}

func (q *Query) cachedSessionCandidates() []sessionCandidate {
	q.cache.mu.RLock()
	defer q.cache.mu.RUnlock()

	if len(q.cache.candidates) == 0 {
		return nil
	}
	if time.Since(q.cache.loadedAt) > sessionCacheTTL {
		return nil
	}

	return copySessionCandidates(q.cache.candidates)
}

func (q *Query) storeSessionCandidates(candidates []sessionCandidate) {
	q.cache.mu.Lock()
	defer q.cache.mu.Unlock()
	q.cache.candidates = copySessionCandidates(candidates)
	q.cache.loadedAt = time.Now()
}

func copySessionCandidates(candidates []sessionCandidate) []sessionCandidate {
	if len(candidates) == 0 {
		return nil
	}

	cloned := make([]sessionCandidate, len(candidates))
	copy(cloned, candidates)
	return cloned
}

func groupSessionCandidates(candidates []sessionCandidate) []*sessionGroup {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].summary.StartTime.Equal(candidates[j].summary.StartTime) {
			return candidates[i].summary.EndTime.Before(candidates[j].summary.EndTime)
		}
		return candidates[i].summary.StartTime.Before(candidates[j].summary.StartTime)
	})

	groups := []*sessionGroup{}
	byKey := map[string]*sessionGroup{}

	for _, candidate := range candidates {
		key := sessionGroupKey(candidate.summary)
		group := byKey[key]

		if group == nil || candidate.summary.StartTime.Sub(group.summary.EndTime) > groupWindow {
			groupID := makeGroupID(key, candidate.summary.StartTime)
			group = &sessionGroup{
				summary: SessionSummary{
					ID:           groupID,
					Label:        candidate.summary.Label,
					Model:        candidate.summary.Model,
					Project:      candidate.summary.Project,
					AgentName:    candidate.summary.AgentName,
					Status:       candidate.summary.Status,
					StartTime:    candidate.summary.StartTime,
					EndTime:      candidate.summary.EndTime,
					Duration:     candidate.summary.Duration,
					InputTokens:  candidate.summary.InputTokens,
					OutputTokens: candidate.summary.OutputTokens,
					InputCost:    candidate.summary.InputCost,
					OutputCost:   candidate.summary.OutputCost,
					TotalCost:    candidate.summary.TotalCost,
					ToolCalls:    candidate.summary.ToolCalls,
					MessageCount: candidate.summary.MessageCount,
					SessionCount: 1,
				},
				modelCosts:   copyModelCosts(candidate.modelCosts),
				statusCounts: map[string]int{candidate.summary.Status: 1},
				members:      []sessionCandidate{candidate},
			}
			groups = append(groups, group)
			byKey[key] = group
			continue
		}

		group.members = append(group.members, candidate)
		group.summary.EndTime = maxTime(group.summary.EndTime, candidate.summary.EndTime)
		group.summary.Duration = max(group.summary.EndTime.Sub(group.summary.StartTime), 0)
		group.summary.InputTokens += candidate.summary.InputTokens
		group.summary.OutputTokens += candidate.summary.OutputTokens
		group.summary.InputCost += candidate.summary.InputCost
		group.summary.OutputCost += candidate.summary.OutputCost
		group.summary.TotalCost += candidate.summary.TotalCost
		group.summary.ToolCalls += candidate.summary.ToolCalls
		group.summary.MessageCount += candidate.summary.MessageCount
		group.summary.SessionCount++
		group.statusCounts[candidate.summary.Status]++
		mergeModelCosts(group.modelCosts, candidate.modelCosts)
	}

	for _, group := range groups {
		group.summary.Status = summarizeGroupStatus(group.statusCounts)
		group.summary.Model = dominantModel(group.modelCosts)
		if group.summary.Model == "" {
			group.summary.Model = firstNonEmptyModel(group.members)
		}
	}

	return groups
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func summarizeGroupStatus(counts map[string]int) string {
	if counts[StatusFailed] > 0 {
		return StatusFailed
	}
	if counts[StatusAbandoned] > 0 {
		return StatusAbandoned
	}
	if counts[StatusCompleted] > 0 {
		return StatusCompleted
	}
	return StatusUnknown
}

func sessionGroupKey(summary SessionSummary) string {
	label := normalizeSessionLabel(summary.Label)
	if label == "" {
		label = summary.ID
	}
	agent := strings.ToLower(strings.TrimSpace(summary.AgentName))
	project := strings.ToLower(strings.TrimSpace(summary.Project))
	return strings.Join([]string{label, agent, project}, "|")
}

func normalizeSessionLabel(label string) string {
	parts := strings.Fields(strings.ToLower(strings.TrimSpace(label)))
	return strings.Join(parts, " ")
}

func makeGroupID(key string, start time.Time) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%s%x:%d", groupIDPrefix, sum, start.Unix())
}

func groupIDKeyHash(summary SessionSummary) string {
	key := sessionGroupKey(summary)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func parseGroupID(sessionID string) (string, int64, bool) {
	if !isGroupID(sessionID) {
		return "", 0, false
	}
	trimmed := strings.TrimPrefix(sessionID, groupIDPrefix)
	parts := strings.SplitN(trimmed, ":", 2)
	if len(parts) != 2 {
		return "", 0, false
	}
	startUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return parts[0], startUnix, true
}

func findGroupByID(groups []*sessionGroup, sessionID string) *sessionGroup {
	for _, group := range groups {
		if group.summary.ID == sessionID {
			return group
		}
	}

	hash, startUnix, ok := parseGroupID(sessionID)
	if !ok {
		return nil
	}

	var best *sessionGroup
	var bestDelta int64
	for _, group := range groups {
		if groupIDKeyHash(group.summary) != hash {
			continue
		}
		delta := group.summary.StartTime.Unix() - startUnix
		if delta < 0 {
			delta = -delta
		}
		if best == nil || delta < bestDelta {
			best = group
			bestDelta = delta
		}
	}

	return best
}

func isGroupID(sessionID string) bool {
	return strings.HasPrefix(sessionID, groupIDPrefix)
}

func copyModelCosts(costs map[string]ModelCost) map[string]ModelCost {
	if costs == nil {
		return map[string]ModelCost{}
	}
	copied := make(map[string]ModelCost, len(costs))
	maps.Copy(copied, costs)
	return copied
}

func mergeModelCosts(target map[string]ModelCost, costs map[string]ModelCost) {
	if target == nil {
		return
	}
	for model, cost := range costs {
		current := target[model]
		current.Model = model
		current.InputTokens += cost.InputTokens
		current.OutputTokens += cost.OutputTokens
		current.InputCost += cost.InputCost
		current.OutputCost += cost.OutputCost
		current.TotalCost += cost.TotalCost
		current.SessionCount += cost.SessionCount
		target[model] = current
	}
}

func firstNonEmptyModel(members []sessionCandidate) string {
	for _, member := range members {
		if member.summary.Model != "" {
			return member.summary.Model
		}
	}
	return ""
}

func (q *Query) Overview(ctx context.Context, filters Filters) (*Overview, error) {
	candidates, err := q.loadSessionCandidates(ctx, false)
	if err != nil {
		return nil, err
	}

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
	detail := &SessionDetail{
		Summary:       summary,
		Messages:      messages,
		ToolFrequency: toolFrequency,
	}

	return detail, nil
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

	subSessions := make([]SessionSummary, 0, len(target.members))
	for _, member := range target.members {
		subSessions = append(subSessions, member.summary)
	}
	sort.Slice(subSessions, func(i, j int) bool {
		return subSessions[i].StartTime.Before(subSessions[j].StartTime)
	})

	detail := &SessionDetail{
		Summary:       target.summary,
		Messages:      messages,
		ToolFrequency: toolFrequency,
		SubSessions:   subSessions,
	}

	return detail, nil
}

func groupNodes(members []sessionCandidate) []*ent.Node {
	total := 0
	for _, member := range members {
		total += len(member.nodes)
	}

	nodes := make([]*ent.Node, 0, total)
	for _, member := range members {
		nodes = append(nodes, member.nodes...)
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].CreatedAt.Equal(nodes[j].CreatedAt) {
			return nodes[i].ID < nodes[j].ID
		}
		return nodes[i].CreatedAt.Before(nodes[j].CreatedAt)
	})

	return nodes
}

func (q *Query) buildSessionMessages(nodes []*ent.Node) ([]SessionMessage, map[string]int) {
	messages := make([]SessionMessage, 0, len(nodes))
	toolFrequency := map[string]int{}

	var lastTime time.Time
	for i, node := range nodes {
		blocks, _ := parseContentBlocks(node.Content)
		t := tokenCounts(node)
		inputCost, outputCost, totalCost := q.costForNode(node, t)

		toolCalls := extractToolCalls(blocks)
		for _, tool := range toolCalls {
			toolFrequency[tool]++
		}

		text := extractText(blocks)
		delta := time.Duration(0)
		if i > 0 {
			delta = node.CreatedAt.Sub(lastTime)
		}
		lastTime = node.CreatedAt

		messages = append(messages, SessionMessage{
			Hash:         node.ID,
			Role:         node.Role,
			Model:        node.Model,
			Timestamp:    node.CreatedAt,
			Delta:        delta,
			InputTokens:  t.Input,
			OutputTokens: t.Output,
			TotalTokens:  t.Total,
			InputCost:    inputCost,
			OutputCost:   outputCost,
			TotalCost:    totalCost,
			ToolCalls:    toolCalls,
			Text:         text,
		})
	}

	return messages, toolFrequency
}

func (q *Query) buildSessionSummaryFromNodes(nodes []*ent.Node) (SessionSummary, map[string]ModelCost, string, error) {
	if len(nodes) == 0 {
		return SessionSummary{}, nil, "", errors.New("empty session nodes")
	}

	start := nodes[0].CreatedAt
	end := nodes[len(nodes)-1].CreatedAt
	duration := max(end.Sub(start), 0)

	label := buildLabel(nodes)
	toolCalls := 0
	modelCosts := map[string]ModelCost{}
	inputTokens := int64(0)
	outputTokens := int64(0)

	hasToolError := false
	for _, node := range nodes {
		blocks, _ := parseContentBlocks(node.Content)
		toolCalls += countToolCalls(blocks)
		if blocksHaveToolError(blocks) {
			hasToolError = true
		}

		t := tokenCounts(node)
		inputTokens += t.Input
		outputTokens += t.Output

		model := normalizeModel(node.Model)
		if model == "" {
			continue
		}

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

	model := dominantModel(modelCosts)
	if model == "" {
		model = firstModel(nodes)
	}
	inputCost, outputCost, totalCost := sumModelCosts(modelCosts)

	status := determineStatus(nodes[len(nodes)-1], hasToolError)

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

func (q *Query) costForNode(node *ent.Node, t nodeTokens) (float64, float64, float64) {
	model := normalizeModel(node.Model)
	if model == "" {
		return 0, 0, 0
	}

	pricing, ok := PricingForModel(q.pricing, model)
	if !ok {
		return 0, 0, 0
	}

	return CostForTokensWithCache(pricing, t.Input, t.Output, t.CacheCreation, t.CacheRead)
}

// nodeTokens holds all token counts for a node, including cache breakdown.
type nodeTokens struct {
	Input         int64
	Output        int64
	Total         int64
	CacheCreation int64
	CacheRead     int64
}

func tokenCounts(node *ent.Node) nodeTokens {
	var t nodeTokens
	if node.PromptTokens != nil {
		t.Input = int64(*node.PromptTokens)
	}
	if node.CompletionTokens != nil {
		t.Output = int64(*node.CompletionTokens)
	}
	if node.CacheCreationInputTokens != nil {
		t.CacheCreation = int64(*node.CacheCreationInputTokens)
	}
	if node.CacheReadInputTokens != nil {
		t.CacheRead = int64(*node.CacheReadInputTokens)
	}

	t.Total = t.Input + t.Output
	if node.TotalTokens != nil {
		t.Total = int64(*node.TotalTokens)
	}

	return t
}

func parseContentBlocks(raw []map[string]any) ([]llm.ContentBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var blocks []llm.ContentBlock
	if err := json.Unmarshal(data, &blocks); err != nil {
		return nil, err
	}

	return blocks, nil
}

func extractToolCalls(blocks []llm.ContentBlock) []string {
	tools := []string{}
	for _, block := range blocks {
		if block.Type == blockTypeToolUse && block.ToolName != "" {
			tools = append(tools, block.ToolName)
		}
	}
	return tools
}

func countToolCalls(blocks []llm.ContentBlock) int {
	count := 0
	for _, block := range blocks {
		if block.Type == blockTypeToolUse {
			count++
		}
	}
	return count
}

func blocksHaveToolError(blocks []llm.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == "tool_result" && block.IsError {
			return true
		}
	}
	return false
}

func extractText(blocks []llm.ContentBlock) string {
	texts := []string{}
	for _, block := range blocks {
		switch {
		case block.Text != "":
			texts = append(texts, block.Text)
		case block.ToolOutput != "":
			texts = append(texts, block.ToolOutput)
		case block.ToolName != "":
			texts = append(texts, "tool call: "+block.ToolName)
		}
	}
	return strings.Join(texts, "\n")
}

func buildLabel(nodes []*ent.Node) string {
	const labelLimit = 36
	const labelPrompts = 3

	labelLines := make([]string, 0, labelPrompts)
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		if node.Role != roleUser {
			continue
		}
		blocks, _ := parseContentBlocks(node.Content)
		text := strings.TrimSpace(extractLabelText(blocks))
		if text == "" {
			continue
		}
		line := firstLabelLine(text)
		if line == "" {
			continue
		}
		labelLines = append(labelLines, line)
		if len(labelLines) >= labelPrompts {
			break
		}
	}

	if len(labelLines) == 0 {
		return truncate(nodes[len(nodes)-1].ID, 12)
	}

	for i, j := 0, len(labelLines)-1; i < j; i, j = i+1, j-1 {
		labelLines[i], labelLines[j] = labelLines[j], labelLines[i]
	}

	label := strings.Join(labelLines, " / ")
	return truncate(label, labelLimit)
}

func firstLabelLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || isTagLine(line) || isCommandLine(line) {
			continue
		}
		return line
	}
	return ""
}

func isCommandLine(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	return strings.HasPrefix(value, "command:")
}

func extractLabelText(blocks []llm.ContentBlock) string {
	texts := []string{}
	for _, block := range blocks {
		if block.Text == "" {
			continue
		}
		texts = append(texts, block.Text)
	}

	text := strings.TrimSpace(strings.Join(texts, "\n"))
	text = stripTaggedSection(text, "system-reminder")
	text = stripTaggedSection(text, "local-command")
	return strings.TrimSpace(text)
}

func stripTaggedSection(text, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	for {
		start := strings.Index(text, openTag)
		if start == -1 {
			break
		}
		end := strings.Index(text[start:], closeTag)
		if end == -1 {
			text = strings.TrimSpace(text[:start])
			break
		}
		end = start + end + len(closeTag)
		text = strings.TrimSpace(text[:start] + text[end:])
	}

	return strings.TrimSpace(text)
}

func isTagLine(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">")
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}

func dominantModel(costs map[string]ModelCost) string {
	var model string
	maxCost := float64(0)
	for name, cost := range costs {
		if cost.TotalCost > maxCost {
			maxCost = cost.TotalCost
			model = name
		}
	}
	return model
}

func firstModel(nodes []*ent.Node) string {
	for _, node := range nodes {
		if node.Model != "" {
			return normalizeModel(node.Model)
		}
	}
	return ""
}

func sumModelCosts(costs map[string]ModelCost) (float64, float64, float64) {
	inputCost := 0.0
	outputCost := 0.0
	totalCost := 0.0
	for _, cost := range costs {
		inputCost += cost.InputCost
		outputCost += cost.OutputCost
		totalCost += cost.TotalCost
	}
	return inputCost, outputCost, totalCost
}

func matchesFilters(summary SessionSummary, filters Filters) bool {
	if filters.Model != "" {
		if normalizeModel(summary.Model) != normalizeModel(filters.Model) {
			return false
		}
	}
	if filters.Status != "" && summary.Status != filters.Status {
		return false
	}
	if filters.Project != "" && summary.Project != filters.Project {
		return false
	}
	if filters.From != nil && summary.EndTime.Before(*filters.From) {
		return false
	}
	if filters.To != nil && summary.StartTime.After(*filters.To) {
		return false
	}
	if filters.Since > 0 {
		cutoff := time.Now().Add(-filters.Since)
		if summary.EndTime.Before(cutoff) {
			return false
		}
	}
	return true
}

// SortSessions sorts session summaries in place by the given key and direction.
func SortSessions(sessions []SessionSummary, sortKey, sortDir string) {
	ascending := strings.EqualFold(sortDir, "asc")
	switch sortKey {
	case "date":
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].StartTime.Before(sessions[j].StartTime)
			}
			return sessions[i].StartTime.After(sessions[j].StartTime)
		})
	case "tokens":
		sort.Slice(sessions, func(i, j int) bool {
			left := sessions[i].InputTokens + sessions[i].OutputTokens
			right := sessions[j].InputTokens + sessions[j].OutputTokens
			if ascending {
				return left < right
			}
			return left > right
		})
	case "duration":
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].Duration < sessions[j].Duration
			}
			return sessions[i].Duration > sessions[j].Duration
		})
	default:
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].TotalCost < sessions[j].TotalCost
			}
			return sessions[i].TotalCost > sessions[j].TotalCost
		})
	}
}

func (q *Query) AnalyticsOverview(ctx context.Context, filters Filters) (*AnalyticsOverview, error) {
	candidates, err := q.loadSessionCandidates(ctx, false)
	if err != nil {
		return nil, err
	}

	groups := groupSessionCandidates(candidates)
	analytics := &AnalyticsOverview{
		ProviderBreakdown: map[string]int{},
	}

	toolGlobal := map[string]*ToolMetric{}
	toolErrors := map[string]int{}
	toolSessions := map[string]map[string]bool{}
	dayMap := map[string]*DayActivity{}
	modelMap := map[string]*modelAccumulator{}
	var filteredSummaries []SessionSummary

	for _, group := range groups {
		summary := group.summary
		if !matchesFilters(summary, filters) {
			continue
		}

		filteredSummaries = append(filteredSummaries, summary)
		analytics.TotalSessions++
		analytics.AvgSessionCost += summary.TotalCost
		analytics.AvgDurationNs += int64(summary.Duration)

		// Activity by day
		dayKey := summary.StartTime.Format("2006-01-02")
		day, ok := dayMap[dayKey]
		if !ok {
			day = &DayActivity{Date: dayKey}
			dayMap[dayKey] = day
		}
		day.Sessions++
		day.Cost += summary.TotalCost
		day.Tokens += summary.InputTokens + summary.OutputTokens

		// Tool and provider aggregation per session
		sessionTools := map[string]bool{}
		provider := ""
		for _, member := range group.members {
			for _, n := range member.nodes {
				blocks, _ := parseContentBlocks(n.Content)
				for _, tool := range extractToolCalls(blocks) {
					if _, ok := toolGlobal[tool]; !ok {
						toolGlobal[tool] = &ToolMetric{Name: tool}
					}
					toolGlobal[tool].Count++
					sessionTools[tool] = true
				}
				if blocksHaveToolError(blocks) {
					for _, tool := range extractToolCalls(blocks) {
						toolErrors[tool]++
					}
				}
				if n.Provider != "" {
					analytics.ProviderBreakdown[n.Provider]++
					if provider == "" {
						provider = n.Provider
					}
				}
			}
		}
		for tool := range sessionTools {
			if toolSessions[tool] == nil {
				toolSessions[tool] = map[string]bool{}
			}
			toolSessions[tool][summary.ID] = true
		}

		// Model performance
		model := normalizeModel(summary.Model)
		if model != "" {
			acc, ok := modelMap[model]
			if !ok {
				acc = &modelAccumulator{provider: provider}
				modelMap[model] = acc
			}
			acc.sessions++
			acc.totalCost += summary.TotalCost
			acc.totalDurationNs += int64(summary.Duration)
			acc.totalTokens += summary.InputTokens + summary.OutputTokens
			if summary.Status == StatusCompleted {
				acc.completedCount++
			}
		}
	}

	if analytics.TotalSessions > 0 {
		analytics.AvgSessionCost /= float64(analytics.TotalSessions)
		analytics.AvgDurationNs /= int64(analytics.TotalSessions)
	}

	// Build top tools sorted by count
	for name, metric := range toolGlobal {
		metric.ErrorCount = toolErrors[name]
		metric.Sessions = len(toolSessions[name])
		analytics.TopTools = append(analytics.TopTools, *metric)
	}
	sort.Slice(analytics.TopTools, func(i, j int) bool {
		return analytics.TopTools[i].Count > analytics.TopTools[j].Count
	})
	if len(analytics.TopTools) > 15 {
		analytics.TopTools = analytics.TopTools[:15]
	}

	// Ensure the last 7 days are always present so heatmaps render a full week
	today := time.Now()
	for i := 6; i >= 0; i-- {
		dayKey := today.AddDate(0, 0, -i).Format("2006-01-02")
		if _, ok := dayMap[dayKey]; !ok {
			dayMap[dayKey] = &DayActivity{Date: dayKey}
		}
	}

	// Build activity by day sorted by date
	for _, day := range dayMap {
		analytics.ActivityByDay = append(analytics.ActivityByDay, *day)
	}
	sort.Slice(analytics.ActivityByDay, func(i, j int) bool {
		return analytics.ActivityByDay[i].Date < analytics.ActivityByDay[j].Date
	})

	// Build model performance
	for model, acc := range modelMap {
		perf := ModelPerformance{
			Model:          model,
			Provider:       acc.provider,
			Sessions:       acc.sessions,
			TotalCost:      acc.totalCost,
			CompletedCount: acc.completedCount,
		}
		if acc.sessions > 0 {
			perf.AvgCost = acc.totalCost / float64(acc.sessions)
			perf.AvgDurationNs = acc.totalDurationNs / int64(acc.sessions)
			perf.AvgTokens = acc.totalTokens / int64(acc.sessions)
			perf.SuccessRate = float64(acc.completedCount) / float64(acc.sessions)
		}
		analytics.ModelPerformance = append(analytics.ModelPerformance, perf)
	}
	sort.Slice(analytics.ModelPerformance, func(i, j int) bool {
		return analytics.ModelPerformance[i].TotalCost > analytics.ModelPerformance[j].TotalCost
	})

	// Build duration and cost buckets from already-computed summaries
	analytics.DurationBuckets = buildDurationBucketsFromSummaries(filteredSummaries)
	analytics.CostBuckets = buildCostBucketsFromSummaries(filteredSummaries)

	return analytics, nil
}

func (q *Query) SessionAnalytics(ctx context.Context, sessionID string) (*SessionAnalytics, error) {
	if isGroupID(sessionID) {
		return q.groupSessionAnalytics(ctx, sessionID)
	}

	leaf, err := q.client.Node.Get(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}

	nodes, err := q.loadAncestry(ctx, leaf)
	if err != nil {
		return nil, err
	}

	return buildSessionAnalytics(sessionID, nodes), nil
}

func (q *Query) groupSessionAnalytics(ctx context.Context, sessionID string) (*SessionAnalytics, error) {
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
	return buildSessionAnalytics(sessionID, nodes), nil
}

func buildSessionAnalytics(sessionID string, nodes []*ent.Node) *SessionAnalytics {
	sa := &SessionAnalytics{SessionID: sessionID}
	uniqueTools := map[string]bool{}

	var lastTime time.Time
	var responseTimes []int64
	var promptLengths []int
	var responseLengths []int

	for i, n := range nodes {
		blocks, _ := parseContentBlocks(n.Content)
		text := extractText(blocks)

		switch n.Role {
		case roleUser:
			sa.UserMessageCount++
			promptLengths = append(promptLengths, len(text))
			if sa.FirstPrompt == "" && text != "" {
				labelText := extractLabelText(blocks)
				line := firstLabelLine(labelText)
				if line != "" {
					sa.FirstPrompt = truncate(line, 200)
				}
			}
		case roleAssistant:
			sa.AssistantMsgCount++
			responseLengths = append(responseLengths, len(text))
		}

		for _, tool := range extractToolCalls(blocks) {
			uniqueTools[tool] = true
		}
		if blocksHaveToolError(blocks) {
			sa.ToolErrorCount++
		}

		if i > 0 {
			delta := n.CreatedAt.Sub(lastTime).Nanoseconds()
			if delta > sa.LongestPauseNs {
				sa.LongestPauseNs = delta
			}
			if n.Role == roleAssistant {
				responseTimes = append(responseTimes, delta)
			}
		}
		lastTime = n.CreatedAt
	}

	sa.UniqueTools = len(uniqueTools)

	if len(responseTimes) > 0 {
		var total int64
		for _, rt := range responseTimes {
			total += rt
		}
		sa.AvgResponseTimeNs = total / int64(len(responseTimes))
	}

	if len(promptLengths) > 0 {
		var total int
		for _, pl := range promptLengths {
			total += pl
		}
		sa.AvgPromptLength = total / len(promptLengths)
	}

	if len(responseLengths) > 0 {
		var total int
		for _, rl := range responseLengths {
			total += rl
		}
		sa.AvgResponseLength = total / len(responseLengths)
	}

	// Tokens per minute
	if len(nodes) >= 2 {
		duration := nodes[len(nodes)-1].CreatedAt.Sub(nodes[0].CreatedAt)
		if duration.Minutes() > 0 {
			var totalTokens int64
			for _, n := range nodes {
				t := tokenCounts(n)
				totalTokens += t.Input + t.Output
			}
			sa.TokensPerMinute = float64(totalTokens) / duration.Minutes()
		}
	}

	return sa
}

type modelAccumulator struct {
	provider        string
	sessions        int
	totalCost       float64
	totalDurationNs int64
	totalTokens     int64
	completedCount  int
}

func buildDurationBucketsFromSummaries(summaries []SessionSummary) []Bucket {
	counts := map[string]int{}
	labels := []string{"<1m", "1-5m", "5-15m", "15-30m", "30-60m", ">1h"}
	for _, summary := range summaries {
		minutes := summary.Duration.Minutes()
		switch {
		case minutes < 1:
			counts["<1m"]++
		case minutes < 5:
			counts["1-5m"]++
		case minutes < 15:
			counts["5-15m"]++
		case minutes < 30:
			counts["15-30m"]++
		case minutes < 60:
			counts["30-60m"]++
		default:
			counts[">1h"]++
		}
	}
	buckets := make([]Bucket, len(labels))
	for i, label := range labels {
		buckets[i] = Bucket{Label: label, Count: counts[label]}
	}
	return buckets
}

func buildCostBucketsFromSummaries(summaries []SessionSummary) []Bucket {
	counts := map[string]int{}
	labels := []string{"<$0.01", "$0.01-0.10", "$0.10-0.50", "$0.50-1.00", "$1.00-5.00", ">$5.00"}
	for _, summary := range summaries {
		cost := summary.TotalCost
		switch {
		case cost < 0.01:
			counts["<$0.01"]++
		case cost < 0.10:
			counts["$0.01-0.10"]++
		case cost < 0.50:
			counts["$0.10-0.50"]++
		case cost < 1.00:
			counts["$0.50-1.00"]++
		case cost < 5.00:
			counts["$1.00-5.00"]++
		default:
			counts[">$5.00"]++
		}
	}
	buckets := make([]Bucket, len(labels))
	for i, label := range labels {
		buckets[i] = Bucket{Label: label, Count: counts[label]}
	}
	return buckets
}

func determineStatus(leaf *ent.Node, hasToolError bool) string {
	if hasToolError {
		return StatusFailed
	}

	role := strings.ToLower(leaf.Role)
	if role != roleAssistant {
		return StatusAbandoned
	}

	reason := strings.ToLower(strings.TrimSpace(leaf.StopReason))
	switch reason {
	case "stop", "end_turn", "end-turn", "eos":
		return StatusCompleted
	case "length", "max_tokens", "content_filter", "tool_use", "tool_use_response":
		return StatusFailed
	case "":
		return StatusUnknown
	default:
		if strings.Contains(reason, "error") {
			return StatusFailed
		}
	}

	return StatusUnknown
}
