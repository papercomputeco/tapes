package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Options configures backfill behavior.
type Options struct {
	DryRun      bool
	Verbose     bool
	Sessions    bool
	OrgID       string
	AuthSubject string
}

// APIRunRequest is the request payload for the API-backed usage sync flow.
type APIRunRequest struct {
	TranscriptDir string `json:"transcript_dir"`
	DryRun        bool   `json:"dry_run,omitempty"`
	Verbose       bool   `json:"verbose,omitempty"`
	Sessions      bool   `json:"sessions,omitempty"`
	OrgID         string `json:"org_id,omitempty"`
	AuthSubject   string `json:"auth_subject,omitempty"`
}

// Backfiller matches Claude Code transcript usage data to tapes DB nodes.
type Backfiller struct {
	driver  storage.Driver
	options Options
}

// NewBackfillerWithDriver creates a Backfiller using an existing storage driver.
func NewBackfillerWithDriver(driver storage.Driver, opts Options) *Backfiller {
	return &Backfiller{
		driver:  driver,
		options: opts,
	}
}

// RunViaAPI asks a tapes API server to perform the usage sync.
func RunViaAPI(ctx context.Context, apiTarget string, transcriptDir string, opts Options) (*Result, error) {
	payload, err := json.Marshal(APIRunRequest{
		TranscriptDir: transcriptDir,
		DryRun:        opts.DryRun,
		Verbose:       opts.Verbose,
		Sessions:      opts.Sessions,
		OrgID:         opts.OrgID,
		AuthSubject:   opts.AuthSubject,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal sync request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(apiTarget, "/")+"/v1/admin/backfill/usage", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create sync request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sync via api: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read sync response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sync api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result Result
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode sync response: %w", err)
	}

	return &result, nil
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
		for i := range entries {
			entries[i].SourcePath = f
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

	var (
		candidates     []*merkle.Node
		totalAssistant int
	)

	nodes, err := b.driver.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	for _, candidate := range nodes {
		if candidate.Bucket.Role != "assistant" {
			continue
		}
		totalAssistant++
		if !b.options.Sessions && candidate.Usage != nil && candidate.Usage.PromptTokens > 0 {
			continue
		}
		candidates = append(candidates, candidate)
	}

	if b.options.Verbose {
		fmt.Printf("Found %d nodes with missing tokens\n", len(candidates))
		fmt.Printf("Found %d transcript entries to match\n", len(entries))
	}

	type candidateInfo struct {
		node *merkle.Node
	}
	byModel := make(map[string][]candidateInfo)
	for _, c := range candidates {
		byModel[c.Bucket.Model] = append(byModel[c.Bucket.Model], candidateInfo{node: c})
	}

	matched := make(map[string]bool)
	sessionMatches := make(map[string]*sessionBackfillMatch)

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

		entryTime, err := parseTranscriptTime(entry.Timestamp)
		if err != nil {
			result.Unmatched++
			continue
		}

		entryText := entry.TextContent()
		var bestMatch *merkle.Node
		bestDelta := 5 * time.Second

		for _, ci := range modelCandidates {
			if matched[ci.node.Hash] {
				continue
			}

			delta := ci.node.CreatedAt.Sub(entryTime)
			if delta < 0 {
				delta = -delta
			}
			if delta > 5*time.Second {
				continue
			}

			if entryText != "" {
				nodeText := ci.node.Bucket.ExtractText()
				if nodeText == "" || !contentPrefixMatch(entryText, nodeText, 200) {
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

		matched[bestMatch.Hash] = true
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
				bestMatch.Hash[:12], model, usage.PromptTokens, usage.CompletionTokens)
		}

		if !b.options.DryRun && (bestMatch.Usage == nil || bestMatch.Usage.PromptTokens == 0) {
			if err := b.driver.UpdateUsage(ctx, bestMatch.Hash, usage); err != nil {
				return nil, fmt.Errorf("failed to update node %s: %w", bestMatch.Hash, err)
			}
		}

		if b.options.Sessions {
			if err := b.recordSessionMatch(ctx, sessionMatches, entry, bestMatch, usage); err != nil {
				return nil, err
			}
		}

		result.Matched++
		result.TotalTokensBackfilled += usage.TotalTokens
	}

	result.Skipped = totalAssistant - len(candidates)
	if b.options.Sessions {
		if err := b.backfillSessions(ctx, sessionMatches, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type sessionBackfillMatch struct {
	sessionID    string
	sourcePath   string
	cwd          string
	version      string
	startedAt    time.Time
	lastSeenAt   time.Time
	nodeHashes   map[string]struct{}
	inputTokens  int64
	outputTokens int64
	turnCount    int64
}

func (b *Backfiller) recordSessionMatch(
	ctx context.Context,
	matches map[string]*sessionBackfillMatch,
	entry TranscriptEntry,
	node *merkle.Node,
	usage *llm.Usage,
) error {
	sessionID := strings.TrimSpace(entry.SessionID)
	if sessionID == "" && entry.SourcePath != "" {
		sessionID = strings.TrimSuffix(filepath.Base(entry.SourcePath), filepath.Ext(entry.SourcePath))
	}
	if sessionID == "" {
		return nil
	}
	entryTime, err := parseTranscriptTime(entry.Timestamp)
	if err != nil {
		// An unparseable timestamp can't seed the session's
		// started_at/last_seen_at, so skip this entry rather than abort the
		// whole backfill. The node's token usage was already recorded by the
		// caller before this point, so only the session rollup is dropped.
		return nil //nolint:nilerr // malformed timestamp is a per-entry skip, not a backfill failure
	}

	match := matches[sessionID]
	if match == nil {
		match = &sessionBackfillMatch{
			sessionID:  sessionID,
			sourcePath: entry.SourcePath,
			cwd:        entry.Cwd,
			version:    entry.Version,
			startedAt:  entryTime,
			lastSeenAt: entryTime,
			nodeHashes: make(map[string]struct{}),
		}
		matches[sessionID] = match
	}
	if match.cwd == "" {
		match.cwd = entry.Cwd
	}
	if match.version == "" {
		match.version = entry.Version
	}
	if entryTime.Before(match.startedAt) {
		match.startedAt = entryTime
	}
	if entryTime.After(match.lastSeenAt) {
		match.lastSeenAt = entryTime
	}
	match.inputTokens += int64(usage.PromptTokens)
	match.outputTokens += int64(usage.CompletionTokens)
	match.turnCount++

	chain, err := b.driver.AncestryChain(ctx, node.Hash)
	if err != nil {
		return fmt.Errorf("load ancestry for session backfill node %s: %w", node.Hash, err)
	}
	for _, chainNode := range chain.Nodes {
		if chainNode != nil && chainNode.Hash != "" {
			match.nodeHashes[chainNode.Hash] = struct{}{}
		}
	}
	return nil
}

func (b *Backfiller) backfillSessions(ctx context.Context, matches map[string]*sessionBackfillMatch, result *Result) error {
	backfiller, ok := b.driver.(storage.SessionBackfiller)
	if !ok {
		if len(matches) > 0 && b.options.Verbose {
			fmt.Printf("  warning: storage driver does not support session backfill\n")
		}
		return nil
	}
	for _, match := range matches {
		nodeHashes := make([]string, 0, len(match.nodeHashes))
		for hash := range match.nodeHashes {
			nodeHashes = append(nodeHashes, hash)
		}
		metadata, err := json.Marshal(map[string]any{
			"source":          "claude-projects-backfill",
			"transcript_path": match.sourcePath,
		})
		if err != nil {
			return fmt.Errorf("marshal session metadata: %w", err)
		}
		req := storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            b.options.OrgID,
				AuthSubject:      firstNonEmpty(b.options.AuthSubject, "claude-projects-backfill"),
				HarnessID:        "claude",
				HarnessSessionID: match.sessionID,
				Name:             filepath.Base(match.sourcePath),
				Cwd:              match.cwd,
				HarnessVersion:   match.version,
				HarnessMetadata:  metadata,
			},
			NodeHashes:   nodeHashes,
			StartedAt:    match.startedAt,
			LastSeenAt:   match.lastSeenAt,
			InputTokens:  match.inputTokens,
			OutputTokens: match.outputTokens,
			TurnCount:    match.turnCount,
		}
		if b.options.DryRun {
			result.SessionsBackfilled++
			result.NodesLinked += len(nodeHashes)
			continue
		}
		backfilled, err := backfiller.BackfillSession(ctx, req)
		if err != nil {
			return fmt.Errorf("backfill session %s: %w", match.sessionID, err)
		}
		result.SessionsBackfilled++
		result.NodesLinked += backfilled.NodesLinked
	}
	return nil
}

func parseTranscriptTime(raw string) (time.Time, error) {
	entryTime, err := time.Parse(time.RFC3339Nano, raw)
	if err == nil {
		return entryTime, nil
	}
	return time.Parse("2006-01-02T15:04:05.000Z", raw)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
