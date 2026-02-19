package backfill

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// Options configures backfill behavior.
type Options struct {
	DryRun  bool
	Verbose bool
}

// Backfiller matches Claude Code transcript usage data to tapes DB nodes.
type Backfiller struct {
	driver  *sqlite.Driver
	options Options
}

// NewBackfiller creates a Backfiller connected to the given SQLite database.
// The returned cleanup function closes the database.
func NewBackfiller(ctx context.Context, dbPath string, opts Options) (*Backfiller, func() error, error) {
	driver, err := sqlite.NewDriver(ctx, dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database: %w", err)
	}

	b := &Backfiller{
		driver:  driver,
		options: opts,
	}

	return b, driver.Close, nil
}

// Run scans transcripts and backfills usage data into the database.
func (b *Backfiller) Run(ctx context.Context, transcriptDir string) (*Result, error) {
	files, err := ScanTranscriptDir(transcriptDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan transcript directory: %w", err)
	}

	// Collect all transcript entries from all files.
	var allEntries []TranscriptEntry
	for _, f := range files {
		entries, err := ParseTranscript(f)
		if err != nil {
			if b.options.Verbose {
				fmt.Printf("  warning: skipping %s: %v\n", f, err)
			}
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	result, err := b.matchAndUpdate(ctx, allEntries)
	if err != nil {
		return nil, err
	}

	result.TranscriptFiles = len(files)
	result.TranscriptEntries = len(allEntries)

	return result, nil
}

func (b *Backfiller) matchAndUpdate(ctx context.Context, entries []TranscriptEntry) (*Result, error) {
	result := &Result{}

	// Query all assistant nodes where token fields are NULL.
	candidates, err := b.driver.Client.Node.Query().
		Where(
			node.RoleEQ("assistant"),
			node.PromptTokensIsNil(),
		).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}

	if b.options.Verbose {
		fmt.Printf("Found %d nodes with missing tokens\n", len(candidates))
		fmt.Printf("Found %d transcript entries to match\n", len(entries))
	}

	// Index candidates by model for fast lookup.
	type candidateInfo struct {
		node *ent.Node
	}
	byModel := make(map[string][]candidateInfo)
	for _, c := range candidates {
		byModel[c.Model] = append(byModel[c.Model], candidateInfo{node: c})
	}

	// Track which nodes have been matched to avoid double-matching.
	matched := make(map[string]bool)

	for _, entry := range entries {
		if entry.Message == nil || entry.Message.Usage == nil {
			result.Unmatched++
			continue
		}

		model := entry.Message.Model
		modelCandidates, ok := byModel[model]
		if !ok {
			result.Unmatched++
			continue
		}

		entryTime, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
		if err != nil {
			entryTime, err = time.Parse("2006-01-02T15:04:05.000Z", entry.Timestamp)
			if err != nil {
				result.Unmatched++
				continue
			}
		}

		entryText := entry.TextContent()
		var bestMatch *ent.Node
		bestDelta := 5 * time.Second

		for _, ci := range modelCandidates {
			if matched[ci.node.ID] {
				continue
			}

			delta := ci.node.CreatedAt.Sub(entryTime)
			if delta < 0 {
				delta = -delta
			}
			if delta > 5*time.Second {
				continue
			}

			// Verify by content prefix if we have text content.
			if entryText != "" && len(ci.node.Content) > 0 {
				nodeText := extractTextFromContent(ci.node.Content)
				if !contentPrefixMatch(entryText, nodeText, 200) {
					continue
				}
			}

			if delta < bestDelta {
				bestDelta = delta
				bestMatch = ci.node
			}
		}

		if bestMatch == nil {
			result.Unmatched++
			continue
		}

		matched[bestMatch.ID] = true
		// PromptTokens follows the proxy convention: total input tokens including
		// base input, cache creation, and cache read. The Anthropic API reports
		// input_tokens as only the non-cached portion, so we must sum all three.
		totalInput := entry.Message.Usage.InputTokens +
			entry.Message.Usage.CacheCreationInputTokens +
			entry.Message.Usage.CacheReadInputTokens
		usage := &llm.Usage{
			PromptTokens:             totalInput,
			CompletionTokens:         entry.Message.Usage.OutputTokens,
			TotalTokens:              totalInput + entry.Message.Usage.OutputTokens,
			CacheCreationInputTokens: entry.Message.Usage.CacheCreationInputTokens,
			CacheReadInputTokens:     entry.Message.Usage.CacheReadInputTokens,
		}

		if b.options.Verbose {
			fmt.Printf("  match: node=%s model=%s tokens=%d+%d\n",
				bestMatch.ID[:12], model, usage.PromptTokens, usage.CompletionTokens)
		}

		if !b.options.DryRun {
			if err := b.driver.UpdateUsage(ctx, bestMatch.ID, usage); err != nil {
				return nil, fmt.Errorf("failed to update node %s: %w", bestMatch.ID, err)
			}
		}

		result.Matched++
		result.TotalTokensBackfilled += usage.TotalTokens
	}

	// Count skipped nodes (already have tokens) for reporting.
	totalAssistant, err := b.driver.Client.Node.Query().
		Where(node.RoleEQ("assistant")).
		Count(ctx)
	if err == nil {
		result.Skipped = totalAssistant - len(candidates)
	}

	return result, nil
}

// extractTextFromContent concatenates text from content blocks.
func extractTextFromContent(content []map[string]any) string {
	var sb strings.Builder
	for _, block := range content {
		if t, ok := block["type"].(string); ok && t == "text" {
			if text, ok := block["text"].(string); ok {
				sb.WriteString(text)
			}
		}
	}
	return sb.String()
}

// contentPrefixMatch checks if the first n characters of two strings match.
func contentPrefixMatch(a, b string, n int) bool {
	if a == "" || b == "" {
		return false
	}
	pa := a
	if len(pa) > n {
		pa = pa[:n]
	}
	pb := b
	if len(pb) > n {
		pb = pb[:n]
	}
	return pa == pb
}
