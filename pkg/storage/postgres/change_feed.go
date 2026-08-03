package postgres

// Change-feed and provenance stamping for the span projection.
//
// Re-derivation rewrites every row of a covered session in place, so "was this
// row written" says nothing about whether it changed. content_hash makes the
// difference observable, derive_seq turns it into a cursor, and fidelity
// records whether the row can be re-derived from stored bytes at all.
//
// All three are computed here, in the storage layer, and never in pkg/derive.
// The deriver is a pure function of the raw rows it is handed; provenance is a
// property of how those bytes were captured and stored, which the deriver
// cannot see and must not read. Computing it at write time is
// what keeps that boundary intact while still getting the fact onto the row.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Provenance tiers stamped on the projection. See the 1781470000 migration for
// what each one means and why 'reduced' and 'degraded' stay distinct.
const (
	// FidelityUnbacked marks a row with no raw turn behind it — synthetic or
	// transcript-derived. Not a gap: there were never wire bytes to keep.
	FidelityUnbacked = ""

	// FidelityRaw marks a row whose raw turn holds verbatim response bytes,
	// so it can be re-derived from what the upstream actually sent.
	FidelityRaw = "raw"

	// FidelityReduced marks a row whose raw turn holds only an adapter's
	// reduction. Re-derivation is bounded by what that adapter chose to keep.
	FidelityReduced = "reduced"

	// FidelityDegraded marks a row whose verbatim bytes existed and are gone:
	// either they arrived and exceeded the ingest cap, or the producer
	// captured them and withheld them to keep the envelope under the
	// transport limit. Distinct from reduced: this is a capture path that
	// HAD the bytes and a limit that took them, which is a tuning signal
	// rather than a deployment fact.
	//
	// Both causes share this tier on purpose. Fidelity answers what can be
	// re-derived from a row, and neither can — the reason a limit bit does
	// not change the answer. Which limit bit is an operations question,
	// answered by the ingest logs that record each cause separately; giving
	// it a tier would put a distinction nobody re-derives differently into
	// every rollup, and rollups take the worst tier, so the split would be
	// lost at the trace level anyway.
	FidelityDegraded = "degraded"
)

// fidelityRank orders the tiers worst-first for the trace rollup. A trace is
// only as trustworthy as its least-recoverable span.
var fidelityRank = map[string]int{
	FidelityDegraded: 0,
	FidelityReduced:  1,
	FidelityRaw:      2,
	FidelityUnbacked: 3,
}

// resolveFidelity maps each raw turn referenced by the set to its provenance
// tier, in one round trip rather than per span.
//
// Raw turn ids the query does not return (a row pruned between derive and this
// write) are simply absent from the map and fall back to unbacked — a missing
// raw row is not evidence of anything about the bytes.
func resolveFidelity(ctx context.Context, qtx *gensqlc.Queries, spans *derive.SpanSet) (map[int64]string, error) {
	seen := map[int64]struct{}{}
	var ids []int64
	for _, turn := range spans.Turns {
		for _, s := range turn.Spans {
			if s.RawTurnID == 0 {
				continue
			}
			if _, ok := seen[s.RawTurnID]; ok {
				continue
			}
			seen[s.RawTurnID] = struct{}{}
			ids = append(ids, s.RawTurnID)
		}
	}
	if len(ids) == 0 {
		return map[int64]string{}, nil
	}

	rows, err := qtx.RawTurnFidelityByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("resolve raw turn fidelity: %w", err)
	}

	out := make(map[int64]string, len(rows))
	for _, r := range rows {
		switch {
		case r.RawResponseDropped:
			out[r.ID] = FidelityDegraded
		case r.HasRawResponse:
			out[r.ID] = FidelityRaw
		default:
			out[r.ID] = FidelityReduced
		}
	}
	return out, nil
}

// spanFidelity is the tier for one span, given the resolved map.
func spanFidelity(tiers map[int64]string, rawTurnID int64) string {
	if rawTurnID == 0 {
		return FidelityUnbacked
	}
	if tier, ok := tiers[rawTurnID]; ok {
		return tier
	}
	return FidelityUnbacked
}

// rollupFidelity reduces a trace's span tiers to the trace's own: the worst
// tier present. A trace containing one degraded span is not "raw" — the point
// of the marker is to answer "can I re-derive this faithfully", and one
// missing payload makes the answer no.
func rollupFidelity(tiers []string) string {
	worst := FidelityUnbacked
	for _, t := range tiers {
		if fidelityRank[t] < fidelityRank[worst] {
			worst = t
		}
	}
	return worst
}

// contentHasher builds a digest over a row's mutable content.
//
// Every field is length-prefixed. Without it, adjacent fields concatenate
// ambiguously — ("ab", "c") and ("a", "bc") would hash identically — and a
// change feed that reports "unchanged" for a real edit is worse than no feed.
type contentHasher struct{ h hash.Hash }

func newContentHasher() *contentHasher { return &contentHasher{h: sha256.New()} }

func (c *contentHasher) bytes(b []byte) *contentHasher {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = c.h.Write(n[:])
	_, _ = c.h.Write(b)
	return c
}

func (c *contentHasher) str(s string) *contentHasher { return c.bytes([]byte(s)) }

func (c *contentHasher) i64(v int64) *contentHasher {
	var b [8]byte
	// Two's-complement reinterpretation, which is what we want: this is a
	// digest of the bit pattern, not arithmetic, and the mapping is bijective
	// so distinct values stay distinct.
	//nolint:gosec // intentional lossless bit reinterpretation for hashing
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return c.bytes(b[:])
}

func (c *contentHasher) i32(v int32) *contentHasher { return c.i64(int64(v)) }

func (c *contentHasher) boolean(v bool) *contentHasher {
	if v {
		return c.bytes([]byte{1})
	}
	return c.bytes([]byte{0})
}

// tstamp hashes a timestamp by its wall value. A zero/invalid timestamp hashes
// distinctly from the epoch so "unset" and "1970" are not the same row.
func (c *contentHasher) tstamp(t time.Time, valid bool) *contentHasher {
	if !valid {
		return c.bytes(nil)
	}
	return c.i64(t.UnixNano())
}

// numeric hashes a pgtype.Numeric without assuming its big.Int is set.
//
// Today's caller always supplies a valid one (numericFromFloat returns
// big.NewInt(0) even for zero), so the nil branch is unreachable from the
// write path. It is guarded anyway because the zero Numeric is a legal value
// of the type and reaching into its nil Int panics — a hash helper that
// crashes the derive transaction on an ordinary-looking input is a poor trade
// for one branch.
//
// Every branch writes the same field count so an unset value cannot
// frame-alias with a set one.
func (c *contentHasher) numeric(n pgtype.Numeric) *contentHasher {
	c.boolean(n.Valid).boolean(n.NaN).i32(n.Exp)
	if n.Int == nil {
		return c.bytes(nil)
	}
	return c.bytes(n.Int.Bytes())
}

func (c *contentHasher) sum() string { return hex.EncodeToString(c.h.Sum(nil)) }

// spanContentHash digests the mutable content of a span row.
//
// Identity columns (org_id, trace_id, span_id) are excluded: they are the key,
// so they cannot change without the row being a different row, and including
// them would only make the hash unstable across a rename of the key space.
// session_id is excluded for a subtler reason — the upsert COALESCEs it, so
// the value stored may not be the value supplied, and hashing the supplied one
// would report a change the table did not make.
//
// fidelity IS included: a row whose bytes went from stored to dropped has
// changed in a way a consumer cares about, even if nothing else moved.
func spanContentHash(p gensqlc.UpsertSpanParams) string {
	return newContentHasher().
		str(p.ParentSpanID).
		str(p.Kind).
		str(p.Name).
		str(p.Status).
		str(p.CallKind).
		str(p.ThreadID).
		str(p.Model).
		str(p.StopReason).
		tstamp(p.StartedAt.Time, p.StartedAt.Valid).
		i64(p.DurationNs).
		i64(p.Seq).
		bytes(p.Input).
		bytes(p.Output).
		bytes(p.Usage).
		bytes(p.Verdict).
		i64(p.RawTurnID.Int64).
		boolean(p.RawTurnID.Valid).
		str(p.NodeHash).
		str(p.Fidelity).
		sum()
}

// spanTurnContentHash digests the mutable content of a trace row. Same
// exclusions as spanContentHash.
func spanTurnContentHash(p gensqlc.UpsertSpanTurnParams) string {
	return newContentHasher().
		str(p.UserPrompt).
		str(p.ResponsePreview).
		str(p.Synthetic).
		str(p.Status).
		tstamp(p.StartedAt.Time, p.StartedAt.Valid).
		tstamp(p.EndedAt.Time, p.EndedAt.Valid).
		i64(p.DurationNs).
		i64(p.TotalInputTokens).
		i64(p.TotalOutputTokens).
		i64(p.MainInputTokens).
		i64(p.MainOutputTokens).
		i64(p.CacheReadTokens).
		i64(p.CacheCreationTokens).
		numeric(p.TotalCostUsd).
		str(p.Source).
		str(p.Fidelity).
		sum()
}
