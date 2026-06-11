package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// TranscriptUploadOptions configures a harness-transcript upload run.
type TranscriptUploadOptions struct {
	// ProjectDir is one Claude Code project directory
	// (~/.claude/projects/<flattened-cwd>) holding <session>.jsonl
	// files and per-session subagents/ directories.
	ProjectDir string

	// SessionIDs filters which sessions upload; empty means every
	// .jsonl in the directory.
	SessionIDs []string

	// IngestURL is the tapes-ingest base URL.
	IngestURL string

	// HarnessID tags the session envelope; Claude Code transcripts are
	// "claude".
	HarnessID string

	Verbose bool
	Logf    func(format string, args ...any)
}

// TranscriptUploadResult summarizes an upload run.
type TranscriptUploadResult struct {
	Sessions int      `json:"sessions"`
	Files    int      `json:"files"`
	Uploaded int      `json:"uploaded"`
	Deduped  int      `json:"deduped"`
	Failed   int      `json:"failed"`
	Failures []string `json:"failures,omitempty"`
}

// transcriptIngestPayload mirrors ingest.TranscriptPayload.
type transcriptIngestPayload struct {
	Session     *sessions.IngestEnvelope `json:"session"`
	AgentID     string                   `json:"agent_id,omitempty"`
	AgentType   string                   `json:"agent_type,omitempty"`
	Description string                   `json:"description,omitempty"`
	ToolUseID   string                   `json:"tool_use_id,omitempty"`
	Records     json.RawMessage          `json:"records"`
}

// subagentMetaFile mirrors the harness's subagents/agent-<id>.meta.json.
type subagentMetaFile struct {
	ToolUseID   string `json:"toolUseId"`
	AgentType   string `json:"agentType"`
	Description string `json:"description"`
}

// UploadTranscripts pushes harness transcripts (main + subagents) into
// the tapes raw layer via POST /v1/ingest/transcript. Idempotent: the
// server dedups by content version, so re-running uploads nothing for
// unchanged files and a new version for grown ones.
func UploadTranscripts(ctx context.Context, opts TranscriptUploadOptions) (*TranscriptUploadResult, error) {
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	if opts.HarnessID == "" {
		opts.HarnessID = "claude"
	}

	entries, err := os.ReadDir(opts.ProjectDir)
	if err != nil {
		return nil, fmt.Errorf("read project dir: %w", err)
	}
	want := map[string]bool{}
	for _, id := range opts.SessionIDs {
		want[id] = true
	}

	client := &http.Client{}
	result := &TranscriptUploadResult{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		sessionID := strings.TrimSuffix(e.Name(), ".jsonl")
		if len(want) > 0 && !want[sessionID] {
			continue
		}
		result.Sessions++

		envelope := &sessions.IngestEnvelope{
			HarnessID:        opts.HarnessID,
			HarnessSessionID: sessionID,
		}

		upload := func(payload transcriptIngestPayload, label string) {
			result.Files++
			deduped, err := postTranscript(ctx, client, opts.IngestURL, payload)
			switch {
			case err != nil:
				result.Failed++
				if len(result.Failures) < 25 {
					result.Failures = append(result.Failures, label+": "+err.Error())
				}
				opts.Logf("FAIL %s: %v", label, err)
			case deduped:
				result.Deduped++
				if opts.Verbose {
					opts.Logf("dedup %s", label)
				}
			default:
				result.Uploaded++
				if opts.Verbose {
					opts.Logf("uploaded %s", label)
				}
			}
		}

		records, err := jsonlToArray(filepath.Join(opts.ProjectDir, e.Name()))
		if err != nil {
			result.Failed++
			result.Failures = append(result.Failures, sessionID+": "+err.Error())
			continue
		}
		upload(transcriptIngestPayload{Session: envelope, Records: records}, sessionID+"/main")

		// Subagent transcripts + their fork metadata.
		subDir := filepath.Join(opts.ProjectDir, sessionID, "subagents")
		subEntries, err := os.ReadDir(subDir)
		if err != nil {
			continue // no subagents
		}
		for _, se := range subEntries {
			name := se.Name()
			if !strings.HasSuffix(name, ".jsonl") || !strings.HasPrefix(name, "agent-") {
				continue
			}
			agentID := strings.TrimSuffix(strings.TrimPrefix(name, "agent-"), ".jsonl")
			var meta subagentMetaFile
			if raw, err := os.ReadFile(filepath.Join(subDir, "agent-"+agentID+".meta.json")); err == nil {
				_ = json.Unmarshal(raw, &meta)
			}
			records, err := jsonlToArray(filepath.Join(subDir, name))
			if err != nil {
				result.Failed++
				result.Failures = append(result.Failures, sessionID+"/"+agentID+": "+err.Error())
				continue
			}
			upload(transcriptIngestPayload{
				Session:     envelope,
				AgentID:     agentID,
				AgentType:   meta.AgentType,
				Description: meta.Description,
				ToolUseID:   meta.ToolUseID,
				Records:     records,
			}, sessionID+"/agent-"+agentID)
		}
	}
	return result, nil
}

// jsonlToArray reads a JSONL file into a JSON array, skipping blank or
// malformed lines (the harness occasionally truncates the final line
// mid-write).
func jsonlToArray(path string) (json.RawMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var records []json.RawMessage
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || !json.Valid(line) {
			continue
		}
		records = append(records, json.RawMessage(line))
	}
	return json.Marshal(records)
}

func postTranscript(ctx context.Context, client *http.Client, ingestURL string, payload transcriptIngestPayload) (deduped bool, err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(ingestURL, "/")+"/v1/ingest/transcript", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return false, fmt.Errorf("ingest returned %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Deduped bool `json:"deduped"`
	}
	_ = json.Unmarshal(raw, &out)
	return out.Deduped, nil
}
