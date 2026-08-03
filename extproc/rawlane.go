package main

import (
	"fmt"
	"strings"
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

// MaxRawResponseBytes mirrors tapes' ingest.MaxRawResponseBytes (8 MiB), the
// cap beyond which ingest stores no bytes and marks the row
// raw_response_dropped — which is what surfaces as fidelity:degraded.
//
// This is a MIRROR, not an import: extproc pins a published tapes module and
// the raw lane is not in a released tag yet, so the constant cannot be
// referenced directly. rawlane_test.go pins the value so a drift shows up as a
// test failure and not as silently mis-sized traffic.
//
// extproc deliberately does NOT pre-drop at this cap. Ingest owns the
// drop-and-mark decision, and a producer that dropped first would make the
// marker unreachable: the row would be indistinguishable from one whose
// producer never captured bytes at all. See rawResponseFits.
const MaxRawResponseBytes = 8 << 20

// MaxIngestBodyBytes mirrors tapes' ingest.MaxIngestBodyBytes — the Fiber body
// limit on /v1/ingest. Exceeding it is a transport-level rejection that loses
// the WHOLE turn (reduction, request, and session attribution included) with
// no fidelity marker recorded anywhere. That is strictly worse than sending no
// bytes at all, which is why rawResponseFits refuses to attach past it.
const MaxIngestBodyBytes = MaxRawResponseBytes*4/3 + 4<<20

// rawLaneEnvelopeReserve is the slack rawResponseFits holds back for the parts
// of the envelope it does not measure: the reduced response, the meta block,
// the session block, and JSON scaffolding. It is an estimate — Dispatch
// re-checks the marshalled payload exactly and strips the bytes if this proved
// optimistic, so an imprecise reserve costs a wasted marshal, never a turn.
const rawLaneEnvelopeReserve = 1 << 20

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
	budget := MaxIngestBodyBytes - reqLen - rawLaneEnvelopeReserve
	if budget <= 0 {
		return false
	}
	return base64Len(rawLen) <= budget
}

// ingestCanDecodeEncoding mirrors the encodings tapes' ingest can undo before
// handing bytes to a reducer (ingest.decodeContentEncoding): empty, identity,
// gzip, x-gzip — one layer only.
//
// extproc's own decoder is strictly more capable: it handles zstd and peels
// comma-stacked layers. That asymmetry is the reason raw-only is interlocked.
// Shipping bytes ingest cannot decode with no reduction attached would store
// the bytes and lose the turn's content, which is worse than either half
// alone.
func ingestCanDecodeEncoding(contentEncoding string) bool {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity", "gzip", "x-gzip":
		return true
	default:
		return false
	}
}

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
	rawFallbackSalvaged     = "decode_salvaged"
)

// rawShapeDual and rawShapeRawOnly label the attached-bytes metric.
const (
	rawShapeDual    = "dual"
	rawShapeRawOnly = "raw_only"
)

// decideRawLane resolves mode plus per-turn facts into the envelope shape.
//
// The interlocks all answer one question: would dropping our reduction leave
// ingest unable to produce one? If yes, the turn degrades to dual rather than
// raw-only — bytes are still captured, and the content still lands.
//
//   - salvaged: extproc recovered content from a truncated gzip body that
//     ingest's plain gzip.Reader + io.ReadAll would reject outright.
//   - encoding: ingest cannot undo zstd or stacked encodings.
func decideRawLane(mode RawResponseMode, rawLen, reqLen int, contentEncoding string, salvaged bool) rawLaneDecision {
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
	switch {
	case !ingestCanDecodeEncoding(contentEncoding):
		d.fallbackReason = rawFallbackEncoding
	case salvaged:
		d.fallbackReason = rawFallbackSalvaged
	default:
		d.omitReduction = true
	}
	return d
}
