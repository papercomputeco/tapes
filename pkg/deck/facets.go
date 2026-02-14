package deck

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SessionFacet holds LLM-extracted qualitative data about a session.
type SessionFacet struct {
	SessionID      string    `json:"session_id"`
	UnderlyingGoal string    `json:"underlying_goal"`
	GoalCategory   string    `json:"goal_category"`
	Outcome        string    `json:"outcome"`
	SessionType    string    `json:"session_type"`
	FrictionTypes  []string  `json:"friction_types"`
	BriefSummary   string    `json:"brief_summary"`
	ExtractedAt    time.Time `json:"extracted_at"`
}

// FacetAnalytics holds aggregated facet data across sessions.
type FacetAnalytics struct {
	GoalDistribution    map[string]int `json:"goal_distribution"`
	OutcomeDistribution map[string]int `json:"outcome_distribution"`
	SessionTypes        map[string]int `json:"session_types"`
	TopFriction         []FrictionItem `json:"top_friction"`
	RecentSummaries     []FacetSummary `json:"recent_summaries"`
}

// FrictionItem represents a friction type with its count.
type FrictionItem struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// FacetSummary is a brief summary for display.
type FacetSummary struct {
	SessionID      string `json:"session_id"`
	UnderlyingGoal string `json:"underlying_goal"`
	BriefSummary   string `json:"brief_summary"`
	GoalCategory   string `json:"goal_category"`
	Outcome        string `json:"outcome"`
}

// LLMCallFunc is the signature for an LLM inference call.
type LLMCallFunc func(ctx context.Context, prompt string) (string, error)

// FacetStore is the interface for persisting and retrieving facets.
type FacetStore interface {
	SaveFacet(ctx context.Context, facet *SessionFacet) error
	GetFacet(ctx context.Context, sessionID string) (*SessionFacet, error)
	ListFacets(ctx context.Context) ([]*SessionFacet, error)
}

// FacetExtractor extracts qualitative facets from session transcripts.
type FacetExtractor struct {
	query   Querier
	llmCall LLMCallFunc
	store   FacetStore
}

// NewFacetExtractor creates a new FacetExtractor.
func NewFacetExtractor(query Querier, llmCall LLMCallFunc, store FacetStore) *FacetExtractor {
	return &FacetExtractor{
		query:   query,
		llmCall: llmCall,
		store:   store,
	}
}

// Extract runs facet extraction for a single session.
func (f *FacetExtractor) Extract(ctx context.Context, sessionID string) (*SessionFacet, error) {
	detail, err := f.query.SessionDetail(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("load session: %w", err)
	}

	transcript := buildTranscript(detail)

	// Chunk large transcripts
	const maxChars = 30000
	if len(transcript) > maxChars {
		transcript = transcript[:maxChars]
	}

	prompt := buildFacetPrompt(transcript)
	response, err := f.llmCall(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("llm call: %w", err)
	}

	facet, err := parseFacetResponse(response)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	facet.SessionID = sessionID
	facet.ExtractedAt = time.Now()

	if f.store != nil {
		if err := f.store.SaveFacet(ctx, facet); err != nil {
			return nil, fmt.Errorf("save facet: %w", err)
		}
	}

	return facet, nil
}

// AggregateFacets computes aggregated analytics from all stored facets.
func (f *FacetExtractor) AggregateFacets(ctx context.Context) (*FacetAnalytics, error) {
	if f.store == nil {
		return &FacetAnalytics{
			GoalDistribution:    map[string]int{},
			OutcomeDistribution: map[string]int{},
			SessionTypes:        map[string]int{},
		}, nil
	}

	facets, err := f.store.ListFacets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list facets: %w", err)
	}

	return aggregateFacets(facets), nil
}

func aggregateFacets(facets []*SessionFacet) *FacetAnalytics {
	analytics := &FacetAnalytics{
		GoalDistribution:    map[string]int{},
		OutcomeDistribution: map[string]int{},
		SessionTypes:        map[string]int{},
	}

	frictionCounts := map[string]int{}

	for _, facet := range facets {
		if facet.GoalCategory != "" {
			analytics.GoalDistribution[facet.GoalCategory]++
		}
		if facet.Outcome != "" {
			analytics.OutcomeDistribution[facet.Outcome]++
		}
		if facet.SessionType != "" {
			analytics.SessionTypes[facet.SessionType]++
		}
		for _, friction := range facet.FrictionTypes {
			frictionCounts[friction]++
		}
	}

	for frictionType, count := range frictionCounts {
		analytics.TopFriction = append(analytics.TopFriction, FrictionItem{
			Type:  frictionType,
			Count: count,
		})
	}
	sort.Slice(analytics.TopFriction, func(i, j int) bool {
		return analytics.TopFriction[i].Count > analytics.TopFriction[j].Count
	})
	if len(analytics.TopFriction) > 10 {
		analytics.TopFriction = analytics.TopFriction[:10]
	}

	// Recent summaries (last 20)
	sort.Slice(facets, func(i, j int) bool {
		return facets[i].ExtractedAt.After(facets[j].ExtractedAt)
	})
	limit := min(20, len(facets))
	for _, facet := range facets[:limit] {
		analytics.RecentSummaries = append(analytics.RecentSummaries, FacetSummary{
			SessionID:      facet.SessionID,
			UnderlyingGoal: facet.UnderlyingGoal,
			BriefSummary:   facet.BriefSummary,
			GoalCategory:   facet.GoalCategory,
			Outcome:        facet.Outcome,
		})
	}

	return analytics
}

func buildTranscript(detail *SessionDetail) string {
	var b strings.Builder
	for _, msg := range detail.Messages {
		fmt.Fprintf(&b, "[%s] %s\n", msg.Role, msg.Text)
	}
	return b.String()
}

func buildFacetPrompt(transcript string) string {
	return "Analyze this LLM coding session transcript and extract structured facets.\nReturn ONLY valid JSON with these fields:\n\n{\n  \"underlying_goal\": \"brief description of the user's primary goal\",\n  \"goal_category\": \"one of: debug_investigate, implement_feature, fix_bug, write_script_tool, refactor_code, configure_system, create_pr_commit, analyze_data, understand_codebase, write_tests, write_docs, deploy_infra, warmup_minimal\",\n  \"outcome\": \"one of: fully_achieved, mostly_achieved, partially_achieved, not_achieved\",\n  \"session_type\": \"one of: single_task, multi_task, iterative_refinement, exploration\",\n  \"friction_types\": [\"array of: wrong_approach, buggy_code, misunderstood_request, tool_failure, unclear_requirements, scope_creep, environment_issue\"],\n  \"brief_summary\": \"1-2 sentence summary of what happened\"\n}\n\nTranscript:\n" + transcript
}

func parseFacetResponse(response string) (*SessionFacet, error) {
	// Extract JSON from the response (may be wrapped in markdown code blocks)
	jsonStr := response
	if idx := strings.Index(response, "{"); idx >= 0 {
		endIdx := strings.LastIndex(response, "}")
		if endIdx > idx {
			jsonStr = response[idx : endIdx+1]
		}
	}

	var facet SessionFacet
	if err := json.Unmarshal([]byte(jsonStr), &facet); err != nil {
		return nil, fmt.Errorf("unmarshal facet JSON: %w", err)
	}

	return &facet, nil
}
