// Package spanembed embeds the span projection for semantic search.
//
// Embeddings are a derived-side concern: the derive/worker family is
// the single writer (the ingest hot path never embeds), and every
// embedding is keyed by deterministic span identity (org_id, trace_id,
// span_id) so the pass is idempotent — re-deriving or re-running
// embeds each span exactly once, skipping identities whose content,
// model, and dimensions are already current.
//
// Only call_kind="main" llm spans embed. Shadow calls (permission
// checks, title generation) are the plurality of llm calls in a
// harness session and poison search relevance; tool and event spans
// carry payloads better served by structured queries. The embedded
// text is the span's delta-only content — the fresh input blocks plus
// the response blocks rendered to text — never the re-sent
// conversation history, which is what keeps selective embedding cheap.
package spanembed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Candidate is one main llm span considered for embedding, joined with
// its existing embedding row (zero values when not yet embedded).
type Candidate struct {
	OrgID     string
	TraceID   string
	SpanID    string
	SessionID string // "" when the span derived without attribution

	// Input and Output are the span's stored delta-only content-block
	// arrays (JSONB), nil when empty.
	Input  json.RawMessage
	Output json.RawMessage

	// ExistingHash and ExistingModel describe the current embedding
	// row; both empty when the span has never been embedded.
	ExistingHash  string
	ExistingModel string

	// ExistingFailHash and ExistingFailModel describe a recorded
	// deterministic embed failure; both empty when the span has no
	// failure marker. A span whose current content and model match a
	// recorded failure is skipped instead of re-attempted.
	ExistingFailHash  string
	ExistingFailModel string
}

// Key orders candidates for keyset pagination.
type Key struct {
	OrgID   string
	TraceID string
	SpanID  string
}

// Key returns the candidate's pagination key.
func (c *Candidate) Key() Key {
	return Key{OrgID: c.OrgID, TraceID: c.TraceID, SpanID: c.SpanID}
}

// ChunkRecord is one span's embedding write. A span that fit the model's
// context window has a single embedding; an oversized span was split into
// pieces and carries one embedding per piece, stored under chunk_idx 0..N-1 in
// slice order.
type ChunkRecord struct {
	OrgID       string
	TraceID     string
	SpanID      string
	SessionID   string
	Model       string
	ContentHash string
	Embeddings  [][]float32
}

// FailureRecord captures a deterministic embed failure so the pass can stop
// retrying the span while keeping the loss observable: why it failed, how big
// the input was, and (via the store) how many times it has failed.
type FailureRecord struct {
	OrgID       string
	TraceID     string
	SpanID      string
	SessionID   string
	Model       string
	ContentHash string
	Reason      string
	ErrorDetail string
	TokenCount  int
}

// Hit is one similarity-search result with its trace/turn context.
type Hit struct {
	TraceID    string
	SpanID     string
	SessionID  string
	Score      float32
	UserPrompt string
	Snippet    string
	Model      string
	StartedAt  time.Time
}

// Report summarizes one embed pass.
type Report struct {
	// Scanned counts every candidate span considered.
	Scanned int `json:"scanned"`
	// Embedded counts spans embedded this pass (new or re-embedded
	// after a content/model change).
	Embedded int `json:"embedded"`
	// UpToDate counts spans skipped because their embedding already
	// matches the current content and model.
	UpToDate int `json:"up_to_date"`
	// Empty counts spans skipped because their delta content renders
	// to no text at all (e.g. a pure tool-call response).
	Empty int `json:"empty"`
	// Poisoned counts spans skipped because they already failed
	// deterministically under this content and model; they are not
	// re-attempted until their content or model changes.
	Poisoned int `json:"poisoned"`
	// Chunked counts spans whose text exceeded the model's context
	// window and was split into multiple embedded pieces.
	Chunked int `json:"chunked"`
	// ChunkRows counts total embedding rows written this pass (one per
	// piece), so chunked spans contribute more than one.
	ChunkRows int `json:"chunk_rows"`
	// Oversized counts spans the model rejected as too large (whether the
	// split then succeeded or exhausted the depth cap).
	Oversized int `json:"oversized"`
	// Failed counts spans whose embed or write failed this pass — both
	// transient faults (retried next pass) and deterministic ones (also
	// recorded and skipped thereafter).
	Failed int `json:"failed"`
	// Pruned counts orphaned embedding rows removed (their span was
	// pruned or reclassified by a re-derive).
	Pruned int64 `json:"pruned"`

	// OversizeTokens holds the reported/estimated token count of each
	// oversized span this pass — the per-observation source for the embed
	// worker's oversize-tokens histogram. Excluded from JSON to keep the
	// logged summary scalar.
	OversizeTokens []int `json:"-"`
	// FailuresByReason counts deterministic failures recorded this pass,
	// keyed by reason (e.g. "oversize", "api_400"). Excluded from JSON.
	FailuresByReason map[string]int `json:"-"`
}

// action is the decision for one candidate.
type action int

const (
	actionEmbed action = iota
	actionUpToDate
	actionEmpty
	actionPoisoned
)

// decide computes the candidate's embed text and content hash and
// returns whether it needs embedding under the configured model. The
// content hash covers only the rendered text; the model is compared
// separately so switching embedding models re-embeds everything.
//
// A span that already failed deterministically under this exact content
// and model is poisoned: re-attempting it would just fail again and burn
// an API call, so it is skipped until its content or model changes.
func decide(c *Candidate, model string) (text, hash string, act action) {
	text = RenderSpanText(c.Input, c.Output)
	if text == "" {
		return "", "", actionEmpty
	}
	hash = ContentHash(text)
	if c.ExistingHash == hash && c.ExistingModel == model {
		return text, hash, actionUpToDate
	}
	if c.ExistingFailHash == hash && c.ExistingFailModel == model {
		return text, hash, actionPoisoned
	}
	return text, hash, actionEmbed
}

// ContentHash returns the hex sha256 of the rendered span text — the
// change detector that makes re-derives cheap: unchanged content under
// an unchanged model never re-embeds.
func ContentHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// RenderSpanText renders a span's stored delta-only content to the
// text that gets embedded: the text blocks of the fresh input followed
// by the text blocks of the output. Tool payloads, thinking, and
// images are deliberately excluded — they are structured data, not
// prose, and they drown the signal the search exists for.
func RenderSpanText(input, output json.RawMessage) string {
	var sb strings.Builder
	appendBlocks(&sb, input)
	appendBlocks(&sb, output)
	return sb.String()
}

// appendBlocks appends the text blocks of one stored content-block
// array. Undecodable payloads contribute nothing: the raw layer is the
// source of truth and a malformed projection row will be rewritten by
// the next derive.
func appendBlocks(sb *strings.Builder, raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var blocks []llm.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return
	}
	for _, b := range blocks {
		if b.Type != "text" || b.Text == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(b.Text)
	}
}
