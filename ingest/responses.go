package ingest

// The acknowledgement bodies this surface returns. They are tiny and they are
// contract: a capture adapter branches on them to decide whether to retry, so
// each one exists as a named type rather than an inline map.

// pingResponse is the liveness probe body.
type pingResponse struct {
	// Status is "ok" when the process is serving.
	Status string `json:"status" oas:"example=ok"`

	// Capabilities names the optional envelope behaviours this server
	// implements, so a capture adapter can tell what a field will mean before
	// it sends one. An unknown field is ignored rather than refused, so an
	// adapter that sends a field to a server which predates it gets no error,
	// only a quieter misfile; asking first is how it avoids that. Absent from
	// servers that predate the list, which an adapter should read as "none".
	Capabilities []string `json:"capabilities"`
}

// CapabilityParentHarnessID is advertised by a server that resolves a fork
// parent under the envelope's parent_harness_id. A server without it ignores
// the field and looks the parent up in the child's own harness, where a
// cross-harness parent is not found and a placeholder is created in its place.
const CapabilityParentHarnessID = "parent_harness_id"

// Capabilities lists what GET /ping advertises, in a stable order. Adapters
// branch on these strings, so they are pinned by a test: an entry is added
// with the behaviour it names, and never renamed or removed while servers
// that implement the behaviour are still deployed.
func Capabilities() []string {
	return []string{CapabilityParentHarnessID}
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
