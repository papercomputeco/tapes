package capture

// The capture-policy half of the drop-reason vocabulary.
//
// A capture path that declines to record a turn owes an answer to "why". The
// gateway adapter has carried a closed enum of those answers for a long time,
// and the client has carried none — it logs a sentence per site. Neither was
// specified anywhere, so nothing decided which of those answers were rules
// every implementation of tapes capture must share and which were artefacts of
// how one deployment moves bytes.
//
// That line is the whole point of this file. The reasons below are the shared
// ones: they say what makes a turn capturable at all, and two implementations
// that disagree about any of them capture different sessions from identical
// traffic. They are specified as data in fixtures/drop-reason/ — one case per
// reason, with the transport reasons specified alongside them precisely so that
// "this one is not contract" is written down rather than assumed — and this is
// the Go home the specification names.
//
// The reasons NOT here are the deliberate other half: a full ingest queue, a
// disconnected client, an ingest POST that timed out, an envelope that failed
// to marshal. Those are real and must stay observable, but they belong to the
// deployment that has a queue, a client connection, and a remote ingest. A
// client writing turns to a local file cannot have them, and requiring it to
// would be specifying one deployment's plumbing as everyone's contract. They
// stay declared where they occur.
//
// This vocabulary is deliberately not an interface. It is the strings, because
// the strings are what travels: they are Prometheus label values, they appear
// in logs, and dashboards and alerts are written against them. Two
// implementations agreeing on an enum but not on its spelling would still
// produce two vocabularies.

// DropReason is one answer to "why was this turn not captured". Values are the
// wire-visible strings: metric label values and log fields, so they are part of
// the contract rather than an internal detail.
type DropReason string

// The capture-policy reasons, in the order a turn meets them.
//
// The order is part of the specification: a turn can satisfy several of these
// at once — a HEAD probe that returned 500 with no body satisfies three — and
// two implementations that report different reasons for the same turn have
// produced two different answers to the same question, even though both
// correctly declined to capture it.
const (
	// DropUpstreamStatus: the upstream did not return a success status.
	// A turn is a completed exchange with a provider; an error response is
	// a record of one failing to happen. Capturing them would put failed
	// requests in the same log as conversations and leave every consumer to
	// re-derive the difference.
	DropUpstreamStatus DropReason = "upstream_status"

	// DropNonTurnRequest: the request is not a turn. Adjacent endpoints on
	// the same host are not conversation (token counting, model listing),
	// and neither is a non-POST method on a turn path — a health probe
	// against the chat endpoint is still a probe.
	DropNonTurnRequest DropReason = "non_turn_request"

	// DropRequestDecode: the request body could not be decoded to the bytes
	// a reducer would parse. The decode policy itself is specified
	// separately, in fixtures/content-encoding/; this is the reason a turn
	// carries when that policy refuses it.
	DropRequestDecode DropReason = "request_decode"

	// DropEmptyResponse: the response phase completed with zero body bytes.
	// There is nothing to reduce, no encoding to undo, and no preview that
	// would tell a reader anything. Distinct from a reduction that came out
	// empty, which had bytes and lost them.
	DropEmptyResponse DropReason = "empty_response"

	// DropUnknownProvider: no reducer claims this provider and endpoint.
	// The set of shapes capture can read is a property of the build, and a
	// turn refused for this reason is a coverage gap rather than a defect
	// in the traffic.
	DropUnknownProvider DropReason = "unknown_provider"

	// DropResponseDecode: the response body could not be decoded. Same
	// policy and same separation as DropRequestDecode, one side over.
	DropResponseDecode DropReason = "response_decode"

	// DropReducerError: the bytes decoded but the reducer refused them.
	// The last policy gate: everything upstream of it said the turn was
	// capturable in principle, and the content said otherwise.
	DropReducerError DropReason = "reducer_error"
)

// PolicyDropReasons enumerates the reasons above, in precedence order.
//
// It exists so a consumer can assert its own vocabulary against this one
// exhaustively rather than reason by reason: a policy reason added here and
// nowhere else, or added somewhere else and not here, is then a test failure
// instead of a discovery.
func PolicyDropReasons() []DropReason {
	return []DropReason{
		DropUpstreamStatus,
		DropNonTurnRequest,
		DropRequestDecode,
		DropEmptyResponse,
		DropUnknownProvider,
		DropResponseDecode,
		DropReducerError,
	}
}

// IsPolicyDropReason reports whether reason is one of the shared capture-policy
// reasons rather than a deployment's own transport or runtime reason.
func IsPolicyDropReason(reason DropReason) bool {
	for _, r := range PolicyDropReasons() {
		if r == reason {
			return true
		}
	}
	return false
}
