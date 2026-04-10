package sessions

import (
	"errors"
	"strings"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// BuildSummary computes a SessionSummary from a chain of nodes in
// chronological order (root first, leaf last). It also returns the
// per-model cost breakdown used for rollups, and the derived status
// (duplicated on the returned summary for convenience).
//
// Returns an error if the input chain is empty.
func BuildSummary(nodes []*merkle.Node, pricing PricingTable) (SessionSummary, map[string]ModelCost, string, error) {
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

	hasToolError := false
	hasGitActivity := false
	var lastModel string

	// Walk every node once: derive tool errors, git activity, per-model cost,
	// and running token totals in a single pass.
	for _, n := range nodes {
		blocks := n.Bucket.Content
		toolCalls += CountToolCalls(blocks)
		if BlocksHaveToolError(blocks) {
			hasToolError = true
		}
		if BlocksHaveGitActivity(blocks) {
			hasGitActivity = true
		}

		t := TokensForNode(n)
		inputTokens += t.Input
		outputTokens += t.Output

		model := NormalizeModel(n.Bucket.Model)
		if model == "" {
			model = lastModel
		}
		if model == "" {
			continue
		}
		lastModel = model

		pricingForModel, ok := PricingForModel(pricing, model)
		if !ok {
			continue
		}

		inputCost, outputCost, totalCost := CostForTokensWithCache(pricingForModel, t.Input, t.Output, t.CacheCreation, t.CacheRead)
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

	label := BuildLabel(nodes)

	model := DominantModel(modelCosts)
	if model == "" {
		model = FirstModel(nodes)
	}
	inputCost, outputCost, totalCost := SumModelCosts(modelCosts)

	leaf := nodes[len(nodes)-1]
	status := DetermineStatus(leaf, hasToolError, hasGitActivity)

	project := ""
	agentName := ""
	for _, n := range nodes {
		if project == "" && strings.TrimSpace(n.Project) != "" {
			project = n.Project
		}
		if agentName == "" && strings.TrimSpace(n.Bucket.AgentName) != "" {
			agentName = n.Bucket.AgentName
		}
		if project != "" && agentName != "" {
			break
		}
	}

	summary := SessionSummary{
		ID:           leaf.Hash,
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
