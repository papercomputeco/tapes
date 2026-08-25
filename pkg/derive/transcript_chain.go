package derive

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// TranscriptTurn is one synthetic conversation turn reconstructed from a
// transcript file: the node chain plus the provenance AddTranscriptTurn
// needs to fold it into the derived set exactly like a wire turn.
type TranscriptTurn struct {
	// Chain is the root-to-leaf node chain for the turn, produced by
	// the same TurnChain constructor the wire path uses, so node hashes
	// and dedup are shared with wire capture.
	Chain []*merkle.Node

	// CapturedAt is the turn's start instant, from the transcript
	// record's timestamp (falling back to the raw row's receive time).
	CapturedAt time.Time

	// RequestID is a deterministic per-turn id (the transcript record
	// UUID) so span/trace ids are stable across re-derives. Empty when
	// the record carries no UUID; callIdentity's hash fallback then
	// applies.
	RequestID string

	// ThreadID is the harness sub-thread the turn belongs to ("" for
	// the main transcript, the subagent id for subagent files).
	ThreadID string
}

// TranscriptTurns reconstructs the conversation in a transcript file as
// wire-shaped turns — one per assistant record — so the transcript-only
// derive path can project a session with no proxy capture. It mirrors
// the wire lane's shape: each turn re-sends the full message history as
// its request and carries one assistant record as its response, which is
// exactly what makes the shared dedup + span-emit machinery produce
// correct trace boundaries and fork anchors.
//
// Transcripts carry the conversation spine only: no model, no token
// usage, no stop reason. Those stay empty (fidelity is the Source=="transcript"
// signal), and the request envelope parameters that ClassifyCall keys on are
// synthesized so every turn classifies as a main conversation call.
func TranscriptTurns(file *TranscriptFile, project string, fallback time.Time) []TranscriptTurn {
	if file == nil {
		return nil
	}

	out := make([]TranscriptTurn, 0, len(file.Records))
	for i := range file.Records {
		rec := &file.Records[i]
		if rec.Message.Role != "assistant" {
			continue
		}
		blocks := transcriptBlocks(rec.Message.Content)
		if len(blocks) == 0 {
			continue
		}

		// Request = every message before this assistant record, converted
		// to wire-shaped messages (tool_use/tool_result field renames and
		// flattening live in transcriptBlocks).
		msgs := make([]llm.Message, 0, i)
		for j := 0; j < i; j++ {
			prev := &file.Records[j]
			if prev.Message.Role == "" {
				continue // Codex anchor/event records carry no message
			}
			msgs = append(msgs, llm.Message{
				Role:    prev.Message.Role,
				Content: transcriptBlocks(prev.Message.Content),
			})
		}

		req := &llm.ChatRequest{
			Messages:  msgs,
			Stream:    boolPtr(true),
			MaxTokens: intPtr(32000),
			// A single placeholder tool is enough for ClassifyCall to type
			// the turn as main (streaming && toolCount > 0). Tools never
			// enter the node hash — they are a classification lever only.
			Tools: []json.RawMessage{json.RawMessage(`{"name":"Bash"}`)},
		}
		resp := &llm.ChatResponse{
			Message: llm.Message{Role: "assistant", Content: blocks},
		}

		chain := TurnChain(CallContext{
			Provider: "anthropic",
			ThreadID: file.AgentID,
			Project:  project,
		}, req, resp)
		if len(chain) == 0 {
			continue
		}

		// Subagent files fork from a Task tool_use; the wire reconciler
		// stamps that edge on wire chains, but a transcript-only session
		// has no wire chains to reconcile, so stamp it directly here.
		if file.AgentID != "" && file.ToolUseID != "" {
			for _, n := range chain {
				if n.ParentHash == nil {
					n.ParentToolUseID = file.ToolUseID
					break
				}
			}
		}

		out = append(out, TranscriptTurn{
			Chain:      chain,
			CapturedAt: transcriptRecordTime(rec.Timestamp, fallback),
			RequestID:  transcriptRequestID(file.AgentID, rec.UUID),
			ThreadID:   file.AgentID,
		})
	}
	return out
}

// transcriptRecordTime parses a transcript record's RFC 3339 timestamp,
// falling back to the raw row's receive time when absent or malformed.
func transcriptRecordTime(ts string, fallback time.Time) time.Time {
	if ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return t
		}
	}
	return fallback
}

// transcriptRequestID mints a deterministic per-turn id from the
// transcript record's UUID so span/trace ids are stable across
// re-derives. The agent key disambiguates main vs subagent (UUIDs are
// globally unique, so the prefix is only for readability). Empty when no
// UUID is present — callIdentity then falls back to the response hash.
func transcriptRequestID(agentID, uuid string) string {
	if uuid == "" {
		return ""
	}
	agentKey := agentID
	if agentKey == "" {
		agentKey = "main"
	}
	return fmt.Sprintf("transcript:%s:%s", agentKey, uuid)
}

func boolPtr(v bool) *bool { return &v }
func intPtr(v int) *int    { return &v }
