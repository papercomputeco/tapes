// Package ingest provides an HTTP server that accepts completed LLM conversation
// turns and appends them to the immutable raw-turn capture log. This enables
// "sidecar mode" where an external gateway (e.g., Envoy AI Gateway) handles
// upstream LLM traffic and tapes only captures the turns for the deriver.
// Embeddings are written downstream by the derive worker family
// (pkg/spanembed), never at ingest time.
package ingest

import "time"

// Config is the ingest server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8082")
	ListenAddr string

	// Project is the git repository or project name to tag on captured turns.
	Project string

	// SessionIdleGap splits a synthetic session — a turn whose envelope
	// carries no harness session id is keyed by its conversation root —
	// into a new session when the turn was captured more than this long
	// after the conversation's previous turn. Zero disables the split; a
	// harness-provided session id is never split.
	SessionIdleGap time.Duration

	// SubjectHeader names the server-trusted header a gateway stamps with
	// the caller's subject on transcript pushes. Off when empty, and then
	// the payload's own identity fields stand. The wire-capture path reads
	// the same header in tapes-extproc, configured by the same variable.
	SubjectHeader string
}

// EnvTrustedSubjectHeader names the trusted subject header.
const EnvTrustedSubjectHeader = "TAPES_TRUSTED_SUBJECT_HEADER"
