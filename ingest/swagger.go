package ingest

// Swagger general info for the ingest write surface.
//
// This is a SEPARATE contract from the read API (api/openapi.yaml). The two are
// different servers on different ports with different trust models: the read API
// is reached by clients through the edge gateway, while ingest trusts the org in
// its payload envelope because its only legitimate caller is an in-cluster
// capture adapter. Publishing them as one document would imply a surface that is
// reachable and safe to call from outside, which this one is not.
//
// It is published anyway because it is the contract every capture path writes:
// tapes-extproc, tapesctl, and paperd all produce this envelope, and "identical
// fidelity whichever path captured it" is unenforceable while the shape those
// paths must agree on lives only in Go structs.

//	@title			Tapes Ingest API
//	@version		1.0
//	@description	Write surface for captured LLM turns and harness transcripts.
//	@description
//	@description	Capture adapters (tapes-extproc, tapesctl, paperd) POST completed turns here; the server appends them to the immutable raw-turn log and the deriver projects them into sessions, traces, and spans.
//	@description
//	@description	**Not internet-facing.** These endpoints trust the org identity in the request envelope, so the only legitimate caller is an in-cluster capture adapter. Exposing them through an edge gateway would let any JWT holder write turns under an arbitrary org.
//	@BasePath		/
//	@schemes		http

// pingResponse is the liveness probe body.
type pingResponse struct {
	Status string `json:"status" example:"ok"`
}

// ingestAcceptedResponse acknowledges a turn that reached the raw layer.
//
// The write is acknowledged, not completed: 202 means the turn was captured and
// queued, and the derived projection follows asynchronously.
type ingestAcceptedResponse struct {
	Status string `json:"status" example:"accepted"`
}

// transcriptAcceptedResponse acknowledges one transcript file.
//
// Deduped reports that this exact content version was already stored, which is
// the normal result of re-uploading an unchanged transcript — a success, not a
// no-op to retry.
type transcriptAcceptedResponse struct {
	Status  string `json:"status" example:"accepted"`
	Deduped bool   `json:"deduped"`
	Records int    `json:"records"`
	AgentID string `json:"agent_id,omitempty"`
}
