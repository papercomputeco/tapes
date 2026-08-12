package extproc

import (
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/ingest"
)

// RawResponseMode selects what the dispatch envelope carries for the response
// half of a turn. It exists to make the migration from "extproc reduces" to
// "tapes reduces" a config change with a proving step in the middle, rather
// than a flag day.
//
//	off  — the historical shape: the adapter's reduction only. No verbatim
//	       bytes leave the process. Stored rows land fidelity:reduced.
//	dual — reduction AND verbatim bytes. Ingest keeps the adapter's reduction
//	       (it consumed the live stream and may have seen framing the stored
//	       bytes no longer show) and stores the bytes alongside it, so rows
//	       land fidelity:raw while the rendered response is unchanged.
//	raw  — verbatim bytes only. Ingest reduces server-side with the shared
//	       pkg/capture reducers. This is the end state: one reducer for every
//	       capture path means two paths cannot reduce differently.
//
// Rollout is deliberately ratcheted: off → dual (prove equivalence on real
// traffic, since dual changes nothing an operator sees while making the bytes
// available for comparison) → raw (delete the second reducer). See README.
type RawResponseMode string

const (
	// RawResponseOff is the default. Changing the default is a separate,
	// deliberate decision from making the mode available.
	RawResponseOff RawResponseMode = "off"
	// RawResponseDual sends both halves.
	RawResponseDual RawResponseMode = "dual"
	// RawResponseRaw sends verbatim bytes and no reduction.
	RawResponseRaw RawResponseMode = "raw"
)

// ParseRawResponseMode maps a config string onto the enum. An unrecognized
// value is an error rather than a silent fallback: a typo'd mode that quietly
// disables the raw lane would look exactly like a working deployment right up
// until someone asks why nothing is fidelity:raw.
func ParseRawResponseMode(s string) (RawResponseMode, error) {
	switch RawResponseMode(strings.ToLower(strings.TrimSpace(s))) {
	case "", RawResponseOff:
		return RawResponseOff, nil
	case RawResponseDual:
		return RawResponseDual, nil
	case RawResponseRaw:
		return RawResponseRaw, nil
	default:
		return RawResponseOff, fmt.Errorf(
			"unknown raw response mode %q (want one of: %s, %s, %s)",
			s, RawResponseOff, RawResponseDual, RawResponseRaw)
	}
}

// sendsRawBytes reports whether the mode puts verbatim bytes on the wire.
func (m RawResponseMode) sendsRawBytes() bool {
	return m == RawResponseDual || m == RawResponseRaw
}

// The two size limits this lane steers by belong to ingest and are read from
// it directly: ingest.MaxRawResponseBytes, the 8 MiB cap beyond which ingest
// stores no bytes and marks the row raw_response_dropped (what surfaces as
// fidelity:degraded), and ingest.MaxIngestBodyBytes, the body limit on
// /v1/ingest.
//
// They used to be mirrored constants here, because extproc built against a
// published tapes module that predated the raw lane and could not name them.
// Sharing a module removed the reason, and with it the drift the mirror's
// pinning test existed to catch.
//
// Two properties of the lane follow from these being ingest's numbers rather
// than extproc's. extproc deliberately does NOT pre-drop at the storage cap:
// ingest owns the drop-and-mark decision, and a producer that dropped first
// would make the marker unreachable, leaving the row indistinguishable from
// one whose producer never captured bytes at all. It does refuse to attach
// past the body limit, because exceeding that is a transport-level rejection
// that loses the WHOLE turn — reduction, request and session attribution
// included — with no fidelity marker recorded anywhere, which is strictly
// worse than sending no bytes at all. See rawResponseFits.

// rawLaneEnvelopeReserve is the slack rawResponseFits holds back for the parts
// of the envelope it does not measure: the reduced response, the meta block,
// the session block, and JSON scaffolding. It is an estimate — Dispatch
// re-checks the marshalled payload exactly and strips the bytes if this proved
// optimistic, so an imprecise reserve costs a wasted marshal, never a turn.
const rawLaneEnvelopeReserve = 1 << 20

// requestCaptureBudget is the most request bytes worth accumulating: a request
// above ingest.MaxIngestBodyBytes − rawLaneEnvelopeReserve cannot land at
// ingest at all, so buffering past it spends exactly the memory this budget
// exists to bound. Derived, never mirrored, so it tracks ingest's limits.
const requestCaptureBudget = ingest.MaxIngestBodyBytes - rawLaneEnvelopeReserve

// base64Len returns the encoded length of n bytes under standard padded
// base64, which is how encoding/json renders a []byte field.
func base64Len(n int) int { return ((n + 2) / 3) * 4 }

// rawResponseFits reports whether rawLen verbatim bytes can ride along with a
// reqLen-byte request without pushing the envelope past ingest's body limit.
//
// Note what this does NOT check: the 8 MiB storage cap. Bytes between the cap
// and the transport budget are attached on purpose so ingest performs the
// authoritative drop-and-mark and the row lands fidelity:degraded. Refusing
// them here would trade a recorded "bytes existed but were too large" for an
// unrecorded "producer sent nothing" — the exact ambiguity the marker exists
// to resolve.
func rawResponseFits(rawLen, reqLen int) bool {
	budget := ingest.MaxIngestBodyBytes - reqLen - rawLaneEnvelopeReserve
	if budget <= 0 {
		return false
	}
	return base64Len(rawLen) <= budget
}

// The raw-only interlock used to be a list of encoding names — empty,
// identity, gzip, x-gzip, one layer — maintained here as extproc's belief
// about what the far side could decode. A list is the wrong shape for that
// question. It was accurate when written, went stale the moment ingest moved
// onto capture.DecodeContentEncoding (zstd, stacked layers, salvage), and
// nothing failed when it did: a stale list in this direction only over-sends,
// so it could drift indefinitely while every test stayed green.
//
// What replaced it is not a better list, it is the absence of one. extproc and
// ingest are one module and decode with one function, so the honest way to ask
// "can the receiver decode these bytes?" is to notice that the receiver's
// decoder has already decoded them: processor.dispatchTurn runs
// capture.DecodeContentEncoding over the same (bytes, encoding) pair the
// envelope carries, and drops the turn if it fails. decideRawLane is handed
// that outcome. Two encodings cannot disagree when there is one decoder and
// one execution of it.
//
// DEPLOYMENT ORDERING. Widening this means raw-only now ships zstd and stacked
// bodies, which the receiving ingest must be able to decode. Both binaries
// build from this repo and land in the same release, but their pods roll
// independently, so for a window a widened extproc can sit in front of an
// older ingest. Under mode=dual that is harmless in either order — the
// reduction always ships, so the bytes are a bonus. Under mode=raw it is not:
// raw-only bytes a pre-zstd ingest cannot reduce store as unreducible archive.
// mode=raw is enabled in no environment today and gates on the equivalence
// proof, so this needs no machinery, only a rule: never switch an environment
// to raw while its tapes image predates this change.

// rawLaneDecision is the resolved shape of one envelope's response half.
type rawLaneDecision struct {
	// attachRaw puts the verbatim bytes on the envelope.
	attachRaw bool
	// omitReduction drops the adapter's reduction, leaving ingest to
	// reduce server-side.
	omitReduction bool
	// skipReason names why bytes were withheld. "" when attached or when
	// the mode never wanted them.
	skipReason string
	// fallbackReason names why a raw-only turn kept its reduction anyway.
	// "" when no fallback fired.
	fallbackReason string
}

const (
	rawSkipTransportBudget = "transport_budget"
	// rawSkipOversizeStripped is recorded by the post-marshal backstop in
	// Dispatch, when the real envelope turned out larger than the estimate
	// allowed for.
	rawSkipOversizeStripped = "oversize_stripped"
	rawFallbackEncoding     = "encoding_not_decodable"
)

// rawShapeDual and rawShapeRawOnly label the attached-bytes metric.
const (
	rawShapeDual    = "dual"
	rawShapeRawOnly = "raw_only"
)

// decideRawLane resolves mode plus per-turn facts into the envelope shape.
//
// Both remaining interlocks answer one question: would dropping our reduction
// leave ingest unable to produce one? If yes, the turn degrades to dual rather
// than raw-only — bytes are still captured, and the content still lands.
//
// ingestCanDecode is that question for the bytes themselves, and the caller
// answers it by having already decoded them with ingest's decoder rather than
// by inspecting the encoding name. On today's only call path it is therefore
// always true, because a body that failed to decode never reaches dispatch.
// The branch stays because "the caller decoded first" is a property of that
// path, not of this function, and a future caller that attaches bytes it did
// not decode should degrade to dual rather than ship unreducible archive.
//
// Salvage is deliberately NOT an interlock any more. It was one while ingest
// decoded with a plain gzip.Reader that refused a stream ending early; ingest
// now salvages on exactly extproc's rule — partial output plus
// io.ErrUnexpectedEOF — and reduces the result rather than discarding it. A
// truncated capture reduces to most of a turn on either side, so forcing dual
// bought nothing and cost the bytes their raw-only path.
func decideRawLane(mode RawResponseMode, rawLen, reqLen int, ingestCanDecode bool) rawLaneDecision {
	if !mode.sendsRawBytes() {
		return rawLaneDecision{}
	}
	if !rawResponseFits(rawLen, reqLen) {
		// No bytes, and emphatically no raw-only: without the reduction
		// this turn would carry nothing at all.
		return rawLaneDecision{skipReason: rawSkipTransportBudget}
	}
	d := rawLaneDecision{attachRaw: true}
	if mode != RawResponseRaw {
		return d
	}
	if !ingestCanDecode {
		d.fallbackReason = rawFallbackEncoding
		return d
	}
	d.omitReduction = true
	return d
}
