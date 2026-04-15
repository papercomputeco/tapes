// Package backfill extracts token usage from Claude Code transcripts
// and backfills historical nodes in the tapes database.
package backfill

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// TranscriptUsage contains token counts from a Claude Code transcript entry.
type TranscriptUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

// TranscriptMessage represents the message field within a JSONL entry.
type TranscriptMessage struct {
	ID         string            `json:"id"`
	Role       string            `json:"role"`
	Model      string            `json:"model"`
	Content    []TranscriptBlock `json:"content"`
	Usage      *TranscriptUsage  `json:"usage"`
	StopReason json.RawMessage   `json:"stop_reason"`
}

// TranscriptBlock represents a content block in a transcript message.
type TranscriptBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// TranscriptEntry represents a single line in a Claude Code JSONL transcript.
type TranscriptEntry struct {
	Type        string             `json:"type"`
	UUID        string             `json:"uuid"`
	ParentUUID  *string            `json:"parentUuid"`
	Timestamp   string             `json:"timestamp"`
	SessionID   string             `json:"sessionId"`
	Cwd         string             `json:"cwd"`
	IsSidechain bool               `json:"isSidechain"`
	AgentID     string             `json:"agentId"`
	Message     *TranscriptMessage `json:"message"`
}

// AgentMeta is the small JSON record Claude Code writes alongside each
// subagent transcript at <session>/subagents/agent-<id>.meta.json. It
// names the subagent's role so backfill can attach an agent_name to the
// inserted nodes.
type AgentMeta struct {
	AgentType   string `json:"agentType"`
	Description string `json:"description"`
}

// LoadAgentMeta returns the sibling .meta.json for a subagent transcript,
// or nil if the meta file does not exist. The transcript path looks like
// .../subagents/agent-abc.jsonl; the meta path is the same with the .jsonl
// suffix replaced by .meta.json.
func LoadAgentMeta(transcriptPath string) (*AgentMeta, error) {
	if !strings.HasSuffix(transcriptPath, ".jsonl") {
		return nil, nil
	}
	metaPath := strings.TrimSuffix(transcriptPath, ".jsonl") + ".meta.json"
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var m AgentMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// TextContent extracts the concatenated text from all text content blocks.
func (e *TranscriptEntry) TextContent() string {
	if e.Message == nil {
		return ""
	}
	var sb strings.Builder
	for _, block := range e.Message.Content {
		if block.Type == "text" {
			sb.WriteString(block.Text)
		}
	}
	return sb.String()
}

// ScanTranscriptDir finds all JSONL files under the given directory.
func ScanTranscriptDir(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".jsonl") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// ParseTranscript reads a JSONL file and returns assistant entries with usage data.
// It deduplicates by message ID, keeping the last (most complete) entry per message.
func ParseTranscript(path string) ([]TranscriptEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Collect entries by message ID to deduplicate streaming chunks.
	// The last entry per message ID has the most complete content.
	byMessageID := make(map[string]TranscriptEntry)
	var order []string

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		var entry TranscriptEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // skip malformed lines
		}

		if entry.Type != "assistant" {
			continue
		}
		if entry.Message == nil || entry.Message.Usage == nil {
			continue
		}
		if entry.Message.Role != "assistant" {
			continue
		}

		msgID := entry.Message.ID
		if msgID == "" {
			continue
		}

		if _, seen := byMessageID[msgID]; !seen {
			order = append(order, msgID)
		}
		byMessageID[msgID] = entry
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Return deduplicated entries in order of first appearance.
	entries := make([]TranscriptEntry, 0, len(order))
	for _, id := range order {
		entries = append(entries, byMessageID[id])
	}

	return entries, nil
}
