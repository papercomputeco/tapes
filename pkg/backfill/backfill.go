package backfill

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
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

	if err := driver.Migrate(ctx); err != nil {
		driver.Close()
		return nil, nil, fmt.Errorf("running migrations: %w", err)
	}

	b := &Backfiller{
		driver:  driver,
		options: opts,
	}

	return b, driver.Close, nil
}

// sourcedEntry pairs a transcript entry with the file it was parsed from so
// the insert pass can resolve the matching subagent .meta.json (if any).
type sourcedEntry struct {
	Entry      TranscriptEntry
	SourceFile string
}

// Run scans transcripts and backfills usage data into the database.
func (b *Backfiller) Run(ctx context.Context, transcriptDir string) (*Result, error) {
	files, err := ScanTranscriptDir(transcriptDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan transcript directory: %w", err)
	}

	// Collect all transcript entries from all files.
	var allEntries []sourcedEntry
	for _, f := range files {
		entries, err := ParseTranscript(f)
		if err != nil {
			if b.options.Verbose {
				fmt.Printf("  warning: skipping %s: %v\n", f, err)
			}
			continue
		}
		for _, e := range entries {
			allEntries = append(allEntries, sourcedEntry{Entry: e, SourceFile: f})
		}
	}

	result, unmatched, err := b.matchAndUpdate(ctx, allEntries)
	if err != nil {
		return nil, err
	}

	if err := b.insertOrphans(ctx, unmatched, result); err != nil {
		return nil, err
	}

	result.TranscriptFiles = len(files)
	result.TranscriptEntries = len(allEntries)

	return result, nil
}

func (b *Backfiller) matchAndUpdate(ctx context.Context, entries []sourcedEntry) (*Result, []sourcedEntry, error) {
	result := &Result{}

	// Query all assistant nodes where token fields are NULL.
	candidates, err := b.driver.Client.Node.Query().
		Where(
			node.RoleEQ("assistant"),
			node.PromptTokensIsNil(),
		).
		All(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query nodes: %w", err)
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
	var unmatched []sourcedEntry

	for _, se := range entries {
		entry := se.Entry
		// Entries without usage data carry no information for either pass —
		// they're not insertion candidates and they're not match candidates.
		// Skip them silently rather than inflating Unmatched.
		if entry.Message == nil || entry.Message.Usage == nil {
			continue
		}

		model := entry.Message.Model
		modelCandidates, ok := byModel[model]
		if !ok {
			result.Unmatched++
			unmatched = append(unmatched, se)
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
			unmatched = append(unmatched, se)
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
				return nil, nil, fmt.Errorf("failed to update node %s: %w", bestMatch.ID, err)
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

	return result, unmatched, nil
}

// insertOrphans creates assistant nodes from transcript entries that have no
// matching node in the DB. Each entry becomes a parent_hash=NULL node with a
// content-addressable hash, so re-running the backfill is naturally idempotent
// (Put returns isNew=false for an already-present hash). Subagent .meta.json
// siblings, when present, supply the agent_name.
func (b *Backfiller) insertOrphans(ctx context.Context, entries []sourcedEntry, result *Result) error {
	if b.options.DryRun {
		return nil
	}

	metaCache := make(map[string]*AgentMeta)

	for _, se := range entries {
		entry := se.Entry
		if entry.Message == nil || entry.Message.Usage == nil {
			continue
		}

		entryTime, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
		if err != nil {
			entryTime, err = time.Parse("2006-01-02T15:04:05.000Z", entry.Timestamp)
			if err != nil {
				continue
			}
		}

		text := entry.TextContent()
		if text == "" {
			continue
		}

		// Subagent transcripts must have a sibling .meta.json before we can
		// insert them. Bucket.AgentName is part of the content-addressable
		// hash, so inserting once with the fallback name and again with the
		// real agentType would create two distinct rows for the same logical
		// turn. Skip until the meta lands; a later run will pick it up.
		agentName := "claude"
		if isSubagentPath(se.SourceFile) {
			meta, ok := metaCache[se.SourceFile]
			if !ok {
				m, err := LoadAgentMeta(se.SourceFile)
				if err == nil {
					meta = m
				}
				metaCache[se.SourceFile] = meta
			}
			if meta == nil || meta.AgentType == "" {
				result.InsertSkipped++
				continue
			}
			agentName = meta.AgentType
		}

		bucket := merkle.Bucket{
			Type:      "message",
			Role:      "assistant",
			Content:   []llm.ContentBlock{{Type: "text", Text: text}},
			Model:     entry.Message.Model,
			Provider:  "anthropic",
			AgentName: agentName,
		}

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

		n := merkle.NewNode(bucket, nil, merkle.NodeOptions{
			StopReason: "end_turn",
			Usage:      usage,
			Project:    entry.Cwd,
		})
		n.CreatedAt = entryTime

		isNew, err := b.driver.Put(ctx, n)
		if err != nil {
			return fmt.Errorf("failed to insert transcript node: %w", err)
		}
		if isNew {
			result.Inserted++
			result.TotalTokensBackfilled += usage.TotalTokens
		} else {
			result.InsertSkipped++
		}
	}

	return nil
}

// isSubagentPath reports whether p is a Claude Code subagent transcript:
// .../<session>/subagents/agent-*.jsonl.
func isSubagentPath(p string) bool {
	return filepath.Base(filepath.Dir(p)) == "subagents"
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

// contentPrefixMatch checks if the first n runes of two strings match.
// Uses runes (not bytes) so a multi-byte UTF-8 character straddling the cut
// point doesn't produce a false negative on otherwise-identical content.
func contentPrefixMatch(a, b string, n int) bool {
	if a == "" || b == "" {
		return false
	}
	return runePrefix(a, n) == runePrefix(b, n)
}

func runePrefix(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}
