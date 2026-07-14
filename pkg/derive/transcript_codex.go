package derive

import (
	"encoding/json"

	openaiProvider "github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

type codexTranscriptRecord struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type codexSubagentActivity struct {
	Type          string `json:"type"`
	Kind          string `json:"kind"`
	EventID       string `json:"event_id"`
	AgentThreadID string `json:"agent_thread_id"`
	AgentPath     string `json:"agent_path"`
}

// parseCodexTranscript reads rollout JSONL records. response_item payloads are
// actual Responses items, so the shared provider mapper gives byte-shape
// parity with the wire parser. Parent-rollout activity events supply the
// otherwise-missing spawn call id for child transcripts.
func parseCodexTranscript(raw json.RawMessage, file *TranscriptFile) error {
	var records []codexTranscriptRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return err
	}
	for _, record := range records {
		switch record.Type {
		case "response_item":
			for _, block := range openaiProvider.ResponsesItemContentBlocks(record.Payload) {
				if sig := blockSignature(block); sig != "" {
					file.signatures[sig] = struct{}{}
				}
			}
		case "event_msg":
			var activity codexSubagentActivity
			if json.Unmarshal(record.Payload, &activity) != nil ||
				activity.Type != "sub_agent_activity" || activity.Kind != "started" ||
				activity.EventID == "" {
				continue
			}
			if activity.AgentThreadID != "" {
				file.spawnEdges[activity.AgentThreadID] = activity.EventID
			}
			if activity.AgentPath != "" {
				file.spawnEdges[activity.AgentPath] = activity.EventID
			}
		}
	}
	return nil
}
