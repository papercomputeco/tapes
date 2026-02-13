package deck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

const blockTypeToolUse = "tool_use"

// Querier is an interface for querying session data.
// This allows for mock implementations in testing and sandboxes.
type Querier interface {
	Overview(ctx context.Context, filters Filters) (*Overview, error)
	SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error)
}

type Query struct {
	client  *ent.Client
	pricing PricingTable
}

// Ensure Query implements Querier
var _ Querier = (*Query)(nil)

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

func (q *Query) Overview(ctx context.Context, filters Filters) (*Overview, error) {
	leaves, err := q.client.Node.Query().Where(node.Not(node.HasChildren())).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list leaves: %w", err)
	}

	overview := &Overview{
		Sessions:    make([]SessionSummary, 0, len(leaves)),
		CostByModel: map[string]ModelCost{},
	}

	for _, leaf := range leaves {
		summary, modelCosts, status, err := q.buildSessionSummary(ctx, leaf)
		if err != nil {
			return nil, err
		}

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

		switch status {
		case StatusCompleted:
			overview.Completed++
		case StatusFailed:
			overview.Failed++
		case StatusAbandoned:
			overview.Abandoned++
		}

		for model, cost := range modelCosts {
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

	sortSessions(overview.Sessions, filters.Sort, filters.SortDir)

	return overview, nil
}

func (q *Query) SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error) {
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

	detail := &SessionDetail{
		Summary:       summary,
		Messages:      make([]SessionMessage, 0, len(nodes)),
		ToolFrequency: map[string]int{},
	}

	var lastTime time.Time
	for i, node := range nodes {
		blocks, _ := parseContentBlocks(node.Content)
		inputTokens, outputTokens, totalTokens := tokenCounts(node)
		inputCost, outputCost, totalCost := q.costForNode(node, inputTokens, outputTokens)

		toolCalls := extractToolCalls(blocks)
		for _, tool := range toolCalls {
			detail.ToolFrequency[tool]++
		}

		text := extractText(blocks)
		delta := time.Duration(0)
		if i > 0 {
			delta = node.CreatedAt.Sub(lastTime)
		}
		lastTime = node.CreatedAt

		detail.Messages = append(detail.Messages, SessionMessage{
			Hash:         node.ID,
			Role:         node.Role,
			Model:        node.Model,
			Timestamp:    node.CreatedAt,
			Delta:        delta,
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			TotalTokens:  totalTokens,
			InputCost:    inputCost,
			OutputCost:   outputCost,
			TotalCost:    totalCost,
			ToolCalls:    toolCalls,
			Text:         text,
		})
	}

	return detail, nil
}

func (q *Query) buildSessionSummary(ctx context.Context, leaf *ent.Node) (SessionSummary, map[string]ModelCost, string, error) {
	nodes, err := q.loadAncestry(ctx, leaf)
	if err != nil {
		return SessionSummary{}, nil, "", err
	}

	summary, modelCosts, status, err := q.buildSessionSummaryFromNodes(nodes)
	if err != nil {
		return SessionSummary{}, nil, "", err
	}

	return summary, modelCosts, status, nil
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

		nodeInput, nodeOutput, _ := tokenCounts(node)
		inputTokens += nodeInput
		outputTokens += nodeOutput

		model := normalizeModel(node.Model)
		if model == "" {
			continue
		}

		pricing, ok := PricingForModel(q.pricing, model)
		if !ok {
			continue
		}

		inputCost, outputCost, totalCost := CostForTokens(pricing, nodeInput, nodeOutput)
		current := modelCosts[model]
		current.Model = model
		current.InputTokens += nodeInput
		current.OutputTokens += nodeOutput
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

	summary := SessionSummary{
		ID:           nodes[len(nodes)-1].ID,
		Label:        label,
		Model:        model,
		Project:      project,
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

func (q *Query) costForNode(node *ent.Node, inputTokens, outputTokens int64) (float64, float64, float64) {
	model := normalizeModel(node.Model)
	if model == "" {
		return 0, 0, 0
	}

	pricing, ok := PricingForModel(q.pricing, model)
	if !ok {
		return 0, 0, 0
	}

	return CostForTokens(pricing, inputTokens, outputTokens)
}

func tokenCounts(node *ent.Node) (int64, int64, int64) {
	var inputTokens, outputTokens int64
	if node.PromptTokens != nil {
		inputTokens = int64(*node.PromptTokens)
	}
	if node.CompletionTokens != nil {
		outputTokens = int64(*node.CompletionTokens)
	}

	totalTokens := inputTokens + outputTokens
	if node.TotalTokens != nil {
		totalTokens = int64(*node.TotalTokens)
	}

	return inputTokens, outputTokens, totalTokens
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
		if node.Role != "user" {
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

func sortSessions(sessions []SessionSummary, sortKey, sortDir string) {
	ascending := strings.EqualFold(sortDir, "asc")
	switch sortKey {
	case "time":
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].EndTime.Before(sessions[j].EndTime)
			}
			return sessions[i].EndTime.After(sessions[j].EndTime)
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

func determineStatus(leaf *ent.Node, hasToolError bool) string {
	if hasToolError {
		return StatusFailed
	}

	role := strings.ToLower(leaf.Role)
	if role != "assistant" {
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
