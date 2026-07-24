// Package fixtures exposes the capture reducer test fixtures as an embed.FS
// so every consumer exercises the SAME bytes instead of hand-copied inline
// duplicates.
//
// The fixtures are recorded/spec-crafted provider wire captures:
//   - anthropic/*.sse, anthropic/*.json — Anthropic Messages streams and
//     one-shot bodies, plus canonical_equivalence/ oneshot↔stream pairs.
//   - openai_responses/*.sse, *.json — OpenAI Responses wire (incl. the
//     Codex chatgpt stream).
//
// The reducer lives in one place but runs in more than one: the in-process
// capture path and any external service that embeds the same reducer to
// process the same traffic. Exposing the fixtures as a package lets every such
// consumer import and exercise the exact same bytes instead of pasting a copy
// inline, so a fixture change is picked up everywhere at once.
//
// The set is small (~80 KiB of text) and imported only from test code, so it
// is never linked into a production binary.
package fixtures

import "embed"

// FS is the embedded fixture tree, rooted so paths are provider-relative
// (e.g. "anthropic/messages_stream.sse", "openai_responses/oneshot.json").
// Paths use forward slashes on every OS, as embed.FS requires.
//
//go:embed anthropic openai_responses
var FS embed.FS

// ReadFile returns the fixture bytes at name, a forward-slash path relative to
// the fixture root (e.g. "anthropic/messages_stream.sse"). It is a thin
// wrapper over FS.ReadFile; the fixtures are compiled in, so a missing name is
// a programming error the caller should treat as fatal in a test.
func ReadFile(name string) ([]byte, error) {
	return FS.ReadFile(name)
}
