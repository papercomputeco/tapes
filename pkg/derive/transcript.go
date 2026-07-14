package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// blockThinking is shared by transcript parsers. The remaining canonical
// block discriminators live with the span emitter in spans.go.
const blockThinking = "thinking"

// ErrUnsupportedTranscriptHarness identifies transcript rows that this
// version of the derive worker cannot reconcile yet. Callers may skip these
// rows without suppressing parse failures for supported harnesses.
var ErrUnsupportedTranscriptHarness = errors.New("unsupported transcript harness")

// TranscriptFile is one parsed harness transcript: the main session
// file or one subagent's. ToolUseID (from the harness's subagent
// meta.json) is the Task tool_use that forked the agent — the causal
// fork edge reconciliation attaches to the wire capture.
type TranscriptFile struct {
	Session     SessionKey
	AgentID     string // "" for the main transcript
	AgentType   string
	Description string
	ToolUseID   string

	// signatures are the projected-content signatures of every block
	// in the transcript — the join key against wire-derived nodes.
	signatures map[string]struct{}
	// spawnEdges are emitted by Codex's parent rollout. Keys include both
	// child agent_path and child thread id; values are the spawn tool call id.
	spawnEdges map[string]string
}

// transcriptMetaFields mirrors the meta block transcript ingest writes.
type transcriptMetaFields struct {
	Transcript  bool   `json:"transcript"`
	AgentID     string `json:"agent_id"`
	AgentType   string `json:"agent_type"`
	Description string `json:"description"`
	ToolUseID   string `json:"tool_use_id"`
}

// IsTranscriptMeta reports whether a raw row's meta marks it as a
// transcript file.
func IsTranscriptMeta(meta json.RawMessage) bool {
	var m transcriptMetaFields
	if len(meta) == 0 || json.Unmarshal(meta, &m) != nil {
		return false
	}
	return m.Transcript
}

// ParseTranscriptFile decodes one transcript raw row into the
// reconciler's working shape, building its projected-content signature
// set.
func ParseTranscriptFile(rec *storage.RawTurnRecord) (*TranscriptFile, error) {
	var m transcriptMetaFields
	if len(rec.Meta) > 0 {
		if err := json.Unmarshal(rec.Meta, &m); err != nil {
			return nil, err
		}
	}
	file := &TranscriptFile{
		Session:     SessionKey{HarnessID: rec.HarnessID, HarnessSessionID: rec.HarnessSessionID},
		AgentID:     m.AgentID,
		AgentType:   m.AgentType,
		Description: m.Description,
		ToolUseID:   m.ToolUseID,
		signatures:  map[string]struct{}{},
		spawnEdges:  map[string]string{},
	}
	switch rec.HarnessID {
	case "codex":
		if err := parseCodexTranscript(rec.RawRequest, file); err != nil {
			return nil, err
		}
	case "claude", "claude-code":
		if err := parseClaudeTranscript(rec.RawRequest, file); err != nil {
			return nil, err
		}
	case "pi":
		if err := parsePiTranscript(rec.RawRequest, file); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w %q", ErrUnsupportedTranscriptHarness, rec.HarnessID)
	}
	return file, nil
}

// blockSignature canonicalizes ONE projected content block into a
// stable join key. Both sides of the reconciliation — wire-derived
// node blocks and transcript blocks — pass through merkle's
// ProjectContent, so the same logical block signs identically
// regardless of source. Thinking blocks return "" (the harness omits
// signatures on re-send; presence carries no join value).
func blockSignature(block llm.ContentBlock) string {
	projected := merkle.ProjectContent([]llm.ContentBlock{block})
	if len(projected) == 0 {
		return ""
	}
	p := projected[0]
	if p.Type == blockThinking {
		return ""
	}
	// Tool ids are harness-stable across both sources and already part
	// of the block; marshal the whole projected block canonically.
	data, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	v := jsontext.Value(data)
	if err := v.Canonicalize(); err != nil {
		return ""
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:])
}
