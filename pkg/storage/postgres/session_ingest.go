package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// nilOrgID is the sentinel UUID used for non-session-aware writers
// (legacy Driver.Put) so the (org_id, hash) composite PK on nodes can
// remain NOT NULL. Deployments that enforce per-org isolation can
// filter on org_id != nilOrgID to detect rows that landed without a
// session envelope.
var nilOrgID = pgtype.UUID{Bytes: [16]byte{}, Valid: true}

// Compile-time guarantee that the Postgres driver continues to satisfy
// the optional SessionIngester capability. A rename or signature
// change to IngestTurn would otherwise silently downgrade the worker
// dispatch path to the legacy per-node Put loop — the runtime type
// assertion in proxy/worker would just fall through without flagging.
var _ storage.SessionIngester = (*Driver)(nil)

// IngestTurn implements storage.SessionIngester for the Postgres
// driver. The session-tracking flow runs in a single transaction:
// resolve / UPSERT a sessions row (keyed by the envelope's natural key
// or a synthetic harness_session_id from the turn's Merkle root),
// resolve the optional fork-parent FK (placeholder-inserting the parent
// when its own first turn hasn't landed yet), and fold derived_status
// from the in-memory turn chain. It no longer persists nodes — the
// merkle layer is in-memory only — and token/turn/cost counters are
// owned by the derive-time span fold.
func (d *Driver) IngestTurn(ctx context.Context, req storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	if d == nil || d.conn == nil {
		return storage.IngestTurnResult{}, errors.New("postgres driver not open")
	}
	if len(req.Nodes) == 0 {
		return storage.IngestTurnResult{}, errors.New("ingest turn: no nodes supplied")
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("begin ingest turn tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success, rollback on failure

	qtx := d.q.WithTx(tx)
	now := time.Now().UTC()
	nowTS := pgtype.Timestamptz{Time: now, Valid: true}

	envelope, harnessSessionID, err := resolveHarnessSessionID(req.Session, req.Nodes[0])
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("resolve harness_session_id: %w", err)
	}

	orgID, err := orgIDFromEnvelope(envelope)
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("decode org_id: %w", err)
	}

	parentSessionID, err := resolveParentSessionID(ctx, qtx, envelope, orgID, nowTS)
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("resolve parent session: %w", err)
	}

	sessionUUID, err := newAppUUID()
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("mint session uuid: %w", err)
	}

	metadata := []byte(envelope.HarnessMetadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	sessionRow, err := qtx.UpsertSession(ctx, gensqlc.UpsertSessionParams{
		ID:               sessionUUID,
		OrgID:            orgID,
		AuthSubject:      envelope.AuthSubject,
		HarnessID:        envelope.HarnessIDOrUnknown(),
		HarnessSessionID: harnessSessionID,
		Name:             nullStringValue(envelope.Name),
		Cwd:              nullStringValue(envelope.Cwd),
		HarnessVersion:   nullStringValue(envelope.HarnessVersion),
		ParentSessionID:  parentSessionID,
		Now:              nowTS,
		HarnessMetadata:  metadata,
	})
	if err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("upsert session: %w", err)
	}

	if req.DerivedTitle != "" {
		if err := qtx.UpdateSessionDerivedTitle(ctx, gensqlc.UpdateSessionDerivedTitleParams{
			DerivedTitle: nullStringValue(req.DerivedTitle),
			ID:           sessionRow.ID,
		}); err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("fold derived title: %w", err)
		}
	}

	// Node persistence is retired: nodes are no longer written here. The
	// session's token/turn/cost counters are owned by the derive-time span
	// fold (FoldSessionRollupsFromSpans) — the ingest path's per-call
	// counters double-counted re-sent history anyway — so this path no
	// longer touches them.
	//
	// derived_status is still folded here from the in-memory turn chain,
	// because status is not a span-fold output (has_git_activity is a
	// content heuristic the spans don't carry). The full conversation is
	// re-sent every turn, so absolute tool counts over req.Nodes (SET, not
	// accumulated) converge to the conversation total without the per-row
	// dedup the node writes used to provide; has_git_activity stays sticky
	// by OR-ing the pre-turn session value.
	hasGitActivity := sessionRow.HasGitActivity
	toolResultCount := 0
	toolErrorCount := 0
	for _, n := range req.Nodes {
		if n == nil {
			continue
		}
		if !hasGitActivity && sessions.BlocksHaveGitActivity(n.Bucket.Content) {
			hasGitActivity = true
		}
		toolResultCount += sessions.CountToolResults(n.Bucket.Content)
		toolErrorCount += sessions.CountToolResultErrors(n.Bucket.Content)
	}
	leaf := req.Nodes[len(req.Nodes)-1]
	status := sessions.DetermineStatus(leaf, hasGitActivity, toolResultCount, toolErrorCount)
	if err := qtx.UpdateSessionStatus(ctx, gensqlc.UpdateSessionStatusParams{
		HasGitActivity:  hasGitActivity,
		ToolResultCount: int32Count(toolResultCount),
		ToolErrorCount:  int32Count(toolErrorCount),
		DerivedStatus:   status,
		ID:              sessionRow.ID,
	}); err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("update session status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("commit ingest turn tx: %w", err)
	}

	return storage.IngestTurnResult{
		SessionID: uuidString(sessionRow.ID),
	}, nil
}

// resolveHarnessSessionID returns the envelope to persist (always
// non-nil) and the harness_session_id value to write. When the
// inbound envelope is nil or signals it lacks a usable
// harness_session_id, a synthetic id is derived from the root node's
// Merkle hash prefix.
//
// The synthetic-id logic lives in pkg/sessions so the proxy capture
// path (proxy/worker) attributes envelope-less turns identically; this
// is a thin adapter from the node-path's *merkle.Node root.
func resolveHarnessSessionID(envelope *sessions.IngestEnvelope, root *merkle.Node) (*sessions.IngestEnvelope, string, error) {
	var rootHash string
	if root != nil {
		rootHash = root.Hash
	}
	return sessions.ResolveHarnessSessionID(envelope, rootHash)
}

// resolveParentSessionID maps an envelope's optional
// ParentHarnessSessionID hint to a concrete sessions.id, inserting a
// placeholder row when the parent's first turn hasn't landed yet so
// the FK can be set immediately. Returns an invalid pgtype.UUID when
// no parent hint is set.
//
// The parent is resolved within the *child's* harness namespace: both
// the natural-key lookup and the placeholder use
// envelope.HarnessIDOrUnknown(). This assumes parent and child share a
// harness_id, which holds because a parent_harness_session_id is only
// meaningful inside the harness that emitted it — a harness names its
// fork-parent using an id drawn from its own session space. Cross-
// harness fork lineage is intentionally not representable: the envelope
// carries no parent harness_id, and dropping harness_id from the lookup
// would be unsafe since harness_session_id is unique only within a
// harness (an unscoped match could FK to the wrong parent). The one
// degenerate case is a parent whose own first turn later lands under a
// different harness_id than the child reported: its natural key won't
// match this placeholder, so a second row is created and the child's
// parent_session_id is left pointing at the orphan. Supporting that
// would require a parent harness_id on the envelope; out of scope here.
func resolveParentSessionID(
	ctx context.Context,
	qtx *gensqlc.Queries,
	envelope *sessions.IngestEnvelope,
	orgID pgtype.UUID,
	now pgtype.Timestamptz,
) (pgtype.UUID, error) {
	if envelope == nil || envelope.ParentHarnessSessionID == nil {
		return pgtype.UUID{}, nil
	}
	// The ingest HTTP boundary's IngestEnvelope.Validate already rejects
	// the empty-string case (the pointer model means "absent" = nil, not
	// ""), so a non-nil pointer here is guaranteed non-empty.
	parentKey := *envelope.ParentHarnessSessionID

	parent, err := qtx.GetSessionByNaturalKey(ctx, gensqlc.GetSessionByNaturalKeyParams{
		OrgID:            orgID,
		HarnessID:        envelope.HarnessIDOrUnknown(),
		HarnessSessionID: parentKey,
	})
	if err == nil {
		return parent.ID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return pgtype.UUID{}, fmt.Errorf("lookup parent session: %w", err)
	}

	placeholderID, err := newAppUUID()
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("mint placeholder uuid: %w", err)
	}
	id, err := qtx.InsertSessionPlaceholder(ctx, gensqlc.InsertSessionPlaceholderParams{
		ID:               placeholderID,
		OrgID:            orgID,
		AuthSubject:      envelope.AuthSubject,
		HarnessID:        envelope.HarnessIDOrUnknown(),
		HarnessSessionID: parentKey,
		Now:              now,
	})
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("insert parent placeholder: %w", err)
	}
	return id, nil
}

// orgIDFromEnvelope decodes the envelope's text org_id into a
// pgtype.UUID. When the envelope is nil or carries an empty OrgID
// (no session block; local-only proxy callers), the nil-UUID sentinel
// is returned so the NOT NULL constraint on both sessions.org_id and
// the new nodes.org_id PK half is satisfied without crashing.
//
// With the composite (org_id, hash) PK introduced by this migration,
// sentinel rows form their own (nilOrgID, hash) bucket, distinct from
// any real org's rows. Deployments that need to enforce per-org
// isolation can match org_id != nilOrgID.
func orgIDFromEnvelope(envelope *sessions.IngestEnvelope) (pgtype.UUID, error) {
	if envelope == nil {
		return nilOrgID, nil
	}
	return orgIDFromString(envelope.OrgID)
}

// orgIDFromString decodes a text org_id into a pgtype.UUID with the same
// sentinel/parse semantics as orgIDFromEnvelope: an empty string maps to
// the nil-UUID bucket, a non-empty value must parse. Shared by the read
// path (SessionIdentityByHash), which scopes its lookup to a single org so
// a content hash several orgs share cannot resolve to the wrong tenant.
func orgIDFromString(orgID string) (pgtype.UUID, error) {
	if orgID == "" {
		return nilOrgID, nil
	}
	parsed, err := uuid.Parse(orgID)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse org_id %q: %w", orgID, err)
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}, nil
}

// newAppUUID mints a v7 UUID app-side. Postgres 17 has no native
// uuidv7() generator, so ingest mints ids client-side.
func newAppUUID() (pgtype.UUID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return pgtype.UUID{}, err
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

// uuidString renders a pgtype.UUID as its canonical 36-char hyphenated
// form, or "" for the not-valid zero value.
func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	var u uuid.UUID
	copy(u[:], id.Bytes[:])
	return u.String()
}

// int32Count clamps a non-negative running count into int32 for the session
// counter columns. Tool-result counts are realistically tiny; the clamp only
// guards against a pathological overflow and keeps gosec G115 satisfied.
func int32Count(n int) int32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// numericFromFloat encodes a float64 dollars amount into pgtype.Numeric
// at 4-decimal scale to match the NUMERIC(12,4) column. A 0.0 input
// becomes Int=0, Exp=0 so the write is a true no-op delta. Used by the
// span writer to encode the derive-time cost fold.
func numericFromFloat(v float64) (pgtype.Numeric, error) {
	if v == 0 {
		return pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Valid: true}, nil
	}
	// Format with up to 4 fractional digits (matches NUMERIC(12,4));
	// Postgres normalizes trailing zeros on store.
	s := strconv.FormatFloat(v, 'f', 4, 64)
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("scan numeric %q: %w", s, err)
	}
	return n, nil
}
