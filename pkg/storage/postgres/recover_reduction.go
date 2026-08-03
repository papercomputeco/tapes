package postgres

// Recovering a turn whose reduction failed at ingest.
//
// The raw layer's promise is that the projection is rebuildable from it at any
// time — a bug is a re-run away from repair, not a data-loss event. Ingest
// leans on that promise directly: when it cannot reduce a raw-only payload
// (unknown provider, undecodable encoding, a reducer that errored) it logs and
// stores the row anyway, on the grounds that the bytes are what a later build
// re-derives from.
//
// Nothing made that true. The derive read did not fetch raw_response, and the
// deriver skips a turn whose reduced response has no content blocks, so such a
// turn read back exactly as empty as it was written and a fixed reducer could
// not reach it. The bytes were preserved and unreachable, which is the worst of
// both: the cost of keeping them without the property they were kept for.
//
// This closes it on the read side. A turn whose reduction is absent gets
// reduced from its stored bytes before the deriver sees it, so re-deriving
// after a reducer fix recovers the projection.
//
// It belongs here rather than in pkg/derive on purpose. The deriver is a pure
// function of the rows it is handed: given the same rows it produces
// byte-identical output. Reduction is a capture concern — it depends on
// provider, content type and content-encoding, none of which are projection
// facts — so doing it inside the deriver would make derived output depend on
// state the deriver must not read. Reducing here keeps that boundary and means
// rederiveChain needs no change at all; it simply stops being handed an empty
// reduction.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// newRecoveryReducers builds the provider→reducer table used to recover a
// failed reduction at read time. Constructed explicitly and dispatched by
// provider name, the same way ingest and proxy do it — pkg/capture keeps no
// global registry so import order and init() stay out of the call graph.
//
// A provider with no entry simply never recovers: the turn stays as it was
// stored, which is the behaviour before this path existed.
func newRecoveryReducers() map[string]capture.Reducer {
	return map[string]capture.Reducer{
		capture.ProviderAnthropic: capture.NewAnthropicReducer(),
		capture.ProviderOpenAI:    capture.NewOpenAIResponsesReducer(),
	}
}

// recoverReduction fills rec.Response by reducing rec.RawResponse when the
// stored reduction is unusable and the verbatim bytes survived.
//
// It is a no-op in every other case — including the overwhelmingly common one,
// where the reduction is fine and the query returned no raw bytes to begin
// with. Recovery is best-effort by design: a turn that cannot be reduced now is
// left exactly as it was stored, so this can never make a session worse than
// not calling it. Failures are logged rather than returned for the same reason
// ingest logs them — one unreducible turn must not fail the derive of every
// other turn in the session.
func recoverReduction(
	ctx context.Context,
	reducers map[string]capture.Reducer,
	logger *slog.Logger,
	rec *storage.RawTurnRecord,
) {
	if len(rec.RawResponse) == 0 || !reducedResponseUnusable(rec.Response) {
		return
	}

	reducer, ok := reducers[rec.Provider]
	if !ok {
		return
	}

	body, err := decodeStoredEncoding(rec.RawResponse, rec.RawResponseEncoding)
	if err != nil {
		logger.Warn("raw turn not recovered: decode failed",
			"raw_turn_id", rec.ID,
			"provider", rec.Provider,
			"encoding", rec.RawResponseEncoding,
			"error", err,
		)
		return
	}

	resp, err := reducer.Reduce(ctx,
		bytes.NewReader(rec.RawRequest),
		bytes.NewReader(body),
		contentTypeFromMeta(rec.Meta),
	)
	if err != nil || resp == nil {
		logger.Warn("raw turn not recovered: reducer failed",
			"raw_turn_id", rec.ID,
			"provider", rec.Provider,
			"error", err,
		)
		return
	}

	// Only accept a reduction that is actually better than what we had.
	// Reducing truncated or unexpected bytes can succeed and still produce
	// nothing usable, and swapping one empty response for another would turn a
	// clean no-op into a write of equal emptiness.
	out, err := json.Marshal(resp)
	if err != nil || reducedResponseUnusable(out) {
		logger.Warn("raw turn not recovered: reduction still empty",
			"raw_turn_id", rec.ID,
			"provider", rec.Provider,
		)
		return
	}
	rec.Response = out
}

// reducedResponseUnusable reports whether a stored reduction is one the deriver
// would skip. It mirrors the gate in pkg/derive (rederiveChain): a response
// without a role or without content blocks produced no chain, so recovering it
// is the difference between a projected turn and a missing one.
//
// Kept in terms of the *parsed* response rather than the envelope JSON, because
// the key is always present in the envelope and its presence says nothing.
func reducedResponseUnusable(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var resp llm.ChatResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		// Unparseable is unusable; the deriver would fail on it too.
		return true
	}
	return resp.Message.Role == "" || len(resp.Message.Content) == 0
}

// decodeStoredEncoding returns the stored bytes decoded per encoding, for
// handing to a reducer. The column keeps the ENCODED bytes — re-compression is
// not byte-identical, so "verbatim" has to mean verbatim — and only the
// reduction sees the decoded form.
//
// It delegates to pkg/capture so recovery decodes exactly what ingest decodes.
// A narrower decoder here would be the worst possible place for the gap: these
// are precisely the rows whose reduction already failed once, so an encoding
// recovery cannot read is an encoding no build ever recovers.
func decodeStoredEncoding(body []byte, encoding string) ([]byte, error) {
	out, _, err := capture.DecodeContentEncoding(body, encoding)
	return out, err
}

// contentTypeFromMeta pulls the capture adapter's recorded content type out of
// the verbatim meta block. The reducers use it to tell a stream from a one-shot
// body; an empty value lets them fall back to sniffing.
func contentTypeFromMeta(meta json.RawMessage) string {
	if len(meta) == 0 {
		return ""
	}
	var m struct {
		ContentType string `json:"content_type"`
	}
	if err := json.Unmarshal(meta, &m); err != nil {
		return ""
	}
	return m.ContentType
}
