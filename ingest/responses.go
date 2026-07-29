package ingest

// The acknowledgement bodies this surface returns. They are tiny and they are
// contract: a capture adapter branches on them to decide whether to retry, so
// each one exists as a named type rather than an inline map.

// pingResponse is the liveness probe body.
type pingResponse struct {
	// Status is "ok" when the process is serving.
	Status string `json:"status" oas:"example=ok"`
}

// ingestAcceptedResponse acknowledges a turn that reached the raw layer.
//
// The write is acknowledged, not completed: 202 means the turn was captured and
// queued, and the derived projection follows asynchronously.
type ingestAcceptedResponse struct {
	// Status is "accepted" when the turn reached the raw layer.
	Status string `json:"status" oas:"example=accepted"`
}

// transcriptAcceptedResponse acknowledges one transcript file.
//
// Deduped reports that this exact content version was already stored, which is
// the normal result of re-uploading an unchanged transcript — a success, not a
// no-op to retry.
type transcriptAcceptedResponse struct {
	// Status is "accepted" when the transcript reached the raw layer.
	Status string `json:"status" oas:"example=accepted"`

	// Deduped reports that this exact content version was already stored.
	Deduped bool `json:"deduped"`

	// Records is the number of transcript records in the uploaded file.
	Records int `json:"records"`

	// AgentID names the subagent whose transcript this is, empty for the main
	// session file.
	AgentID string `json:"agent_id,omitempty"`
}
