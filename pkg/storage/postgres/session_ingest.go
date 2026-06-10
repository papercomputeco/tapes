package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// syntheticHarnessSessionIDPrefixLen is the number of leading hex
// characters of the root node's Merkle hash used to derive a synthetic
// harness_session_id when the inbound envelope doesn't carry one.
//
// 16 hex chars = 64 bits of entropy. Each captured turn's root hash is
// a SHA-256 of canonicalized JSON, so the 64-bit prefix is effectively
// uniformly random. The birthday bound for a 50% collision at 64 bits
// is ~2^32 ≈ 4 billion synthetic sessions per org; any plausible org
// will be many orders of magnitude under that, so collisions are not
// a real concern. We deliberately keep the prefix short so the synthetic
// id stays human-grep-able in logs.
const syntheticHarnessSessionIDPrefixLen = 16

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
var (
	_ storage.SessionIngester   = (*Driver)(nil)
	_ storage.SessionBackfiller = (*Driver)(nil)
)

// IngestTurn implements storage.SessionIngester for the Postgres
// driver. The session-tracking flow runs in a single transaction:
// resolve / UPSERT a sessions row, resolve the optional fork-parent FK
// (placeholder-inserting the parent when its own first turn hasn't
// landed yet), insert every node in the supplied chain, stamp
// session_id onto each newly-inserted node, and roll up the per-turn
// counters.
func (d *Driver) IngestTurn(ctx context.Context, req storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	if d == nil || d.conn == nil {
		return storage.IngestTurnResult{}, errors.New("postgres driver not open")
	}
	if len(req.Nodes) == 0 {
		return storage.IngestTurnResult{}, errors.New("ingest turn: no nodes supplied")
	}
	if err := validateChainOrdering(req.Nodes); err != nil {
		// Synthetic id derivation (req.Nodes[0].Hash) trusts the caller's
		// root-to-leaf ordering — a scrambled chain would derive the
		// wrong "root" and silently land turns on the wrong session.
		// Reject at the boundary; do not attempt to repair.
		return storage.IngestTurnResult{}, fmt.Errorf("ingest turn: %w", err)
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

	var newNodes []*merkle.Node
	for _, node := range req.Nodes {
		if node == nil {
			continue
		}
		params, err := insertNodeParamsFromMerkle(orgID, node)
		if err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("marshal node %s: %w", node.Hash, err)
		}
		rows, err := qtx.InsertNode(ctx, params)
		if err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("insert node %s: %w", node.Hash, err)
		}
		if rows == 0 {
			// Duplicate (org_id, hash): the row already exists from a
			// retry for the same org. Its session_id was set on the
			// original write; do not re-stamp here because the existing
			// row already FKs to the correct session.
			continue
		}
		if err := qtx.SetNodeSessionID(ctx, gensqlc.SetNodeSessionIDParams{
			SessionID: sessionRow.ID,
			OrgID:     orgID,
			Hash:      node.Hash,
		}); err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("stamp session_id on node %s: %w", node.Hash, err)
		}
		newNodes = append(newNodes, node)
	}

	countersUpdated := false
	if len(newNodes) > 0 {
		costNumeric, err := numericFromFloat(req.CostUSD)
		if err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("encode cost_usd delta: %w", err)
		}
		if err := qtx.UpdateSessionCounters(ctx, gensqlc.UpdateSessionCountersParams{
			Now:               nowTS,
			TurnCountDelta:    1,
			InputTokensDelta:  req.InputTokens,
			OutputTokensDelta: req.OutputTokens,
			CostUsdDelta:      costNumeric,
			ID:                sessionRow.ID,
		}); err != nil {
			return storage.IngestTurnResult{}, fmt.Errorf("update session counters: %w", err)
		}
		countersUpdated = true

		// Recompute the session's chain-aware status. has_git_activity is
		// sticky and the tool-result counts are cumulative across every turn
		// and stem; sessionRow carries the pre-turn totals via UpsertSession's
		// RETURNING. We accumulate over newNodes (the genuinely new rows this
		// turn) rather than req.Nodes — the full chain is re-sent every turn,
		// so counting it would multiply the totals; OR-only booleans tolerated
		// that, counts do not. derived_status mirrors
		// pkg/sessions.DetermineStatus over the totals and this turn's leaf
		// (req.Nodes is validated root->leaf, so the last node is the leaf).
		hasGitActivity := sessionRow.HasGitActivity
		toolResultCount := int(sessionRow.ToolResultCount)
		toolErrorCount := int(sessionRow.ToolErrorCount)
		for _, n := range newNodes {
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
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.IngestTurnResult{}, fmt.Errorf("commit ingest turn tx: %w", err)
	}

	return storage.IngestTurnResult{
		SessionID:       uuidString(sessionRow.ID),
		NewNodes:        newNodes,
		CountersUpdated: countersUpdated,
	}, nil
}

// BackfillSession links existing legacy nodes to a session table row. It is
// used by transcript backfills where the node DAG already exists but the
// original ingest path predated session envelopes.
func (d *Driver) BackfillSession(ctx context.Context, req storage.SessionBackfillRequest) (storage.SessionBackfillResult, error) {
	if d == nil || d.conn == nil {
		return storage.SessionBackfillResult{}, errors.New("postgres driver not open")
	}
	if req.Session == nil {
		return storage.SessionBackfillResult{}, errors.New("backfill session: missing session envelope")
	}
	if req.Session.HarnessSessionID == "" {
		return storage.SessionBackfillResult{}, errors.New("backfill session: missing harness_session_id")
	}
	if len(req.NodeHashes) == 0 {
		return storage.SessionBackfillResult{}, nil
	}

	orgID, err := orgIDFromEnvelope(req.Session)
	if err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("decode org_id: %w", err)
	}
	sessionUUID, err := newAppUUID()
	if err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("mint session uuid: %w", err)
	}

	startedAt := req.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	lastSeenAt := req.LastSeenAt.UTC()
	if lastSeenAt.IsZero() || lastSeenAt.Before(startedAt) {
		lastSeenAt = startedAt
	}
	metadata := []byte(req.Session.HarnessMetadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("begin backfill session tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var sessionID pgtype.UUID
	err = tx.QueryRow(ctx, `
INSERT INTO sessions (
    id, org_id, auth_subject, harness_id, harness_session_id,
    name, cwd, harness_version, started_at, last_seen_at, harness_metadata,
    total_input_tokens, total_output_tokens, turn_count
) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), $9, $10, $11, $12, $13, $14)
ON CONFLICT (org_id, harness_id, harness_session_id) DO UPDATE
SET last_seen_at = GREATEST(sessions.last_seen_at, EXCLUDED.last_seen_at),
    auth_subject = EXCLUDED.auth_subject,
    harness_metadata = sessions.harness_metadata || EXCLUDED.harness_metadata,
    name = COALESCE(EXCLUDED.name, sessions.name),
    cwd = COALESCE(EXCLUDED.cwd, sessions.cwd),
    harness_version = COALESCE(EXCLUDED.harness_version, sessions.harness_version),
    total_input_tokens = GREATEST(sessions.total_input_tokens, EXCLUDED.total_input_tokens),
    total_output_tokens = GREATEST(sessions.total_output_tokens, EXCLUDED.total_output_tokens),
    turn_count = GREATEST(sessions.turn_count, EXCLUDED.turn_count)
RETURNING id`,
		sessionUUID,
		orgID,
		req.Session.AuthSubject,
		req.Session.HarnessIDOrUnknown(),
		req.Session.HarnessSessionID,
		req.Session.Name,
		req.Session.Cwd,
		req.Session.HarnessVersion,
		startedAt,
		lastSeenAt,
		metadata,
		req.InputTokens,
		req.OutputTokens,
		req.TurnCount,
	).Scan(&sessionID)
	if err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("upsert session: %w", err)
	}

	tag, err := tx.Exec(ctx, `
UPDATE nodes
   SET session_id = $1
 WHERE org_id = $2
   AND hash = ANY($3::text[])
   AND session_id IS NULL`,
		sessionID,
		orgID,
		req.NodeHashes,
	)
	if err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("stamp legacy nodes: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return storage.SessionBackfillResult{}, fmt.Errorf("commit backfill session tx: %w", err)
	}
	return storage.SessionBackfillResult{
		SessionID:   uuidString(sessionID),
		NodesLinked: int(tag.RowsAffected()),
	}, nil
}

// validateChainOrdering enforces the root-to-leaf invariant documented
// on storage.IngestTurnRequest: each node's ParentHash must point at
// the previous node's Hash. Nodes[0] is the conversation root, so its
// ParentHash must be nil or empty.
//
// The synthetic harness_session_id derivation depends on Nodes[0]
// actually being the root, and the (org_id, hash) PK on nodes means a
// scrambled chain produces real duplicate-hash conflicts on the wrong
// session; we reject at the boundary rather than attempt a repair.
func validateChainOrdering(nodes []*merkle.Node) error {
	for i, node := range nodes {
		if node == nil {
			return fmt.Errorf("nodes[%d] is nil", i)
		}
		if i == 0 {
			if node.ParentHash != nil && *node.ParentHash != "" {
				return fmt.Errorf("nodes[0] must be the conversation root: ParentHash=%q", *node.ParentHash)
			}
			continue
		}
		prev := nodes[i-1]
		if node.ParentHash == nil || *node.ParentHash == "" {
			return fmt.Errorf("nodes[%d] has no ParentHash but expected %q", i, prev.Hash)
		}
		if *node.ParentHash != prev.Hash {
			return fmt.Errorf("nodes[%d].ParentHash=%q does not chain to nodes[%d].Hash=%q", i, *node.ParentHash, i-1, prev.Hash)
		}
	}
	return nil
}

// resolveHarnessSessionID returns the envelope to persist (always
// non-nil) and the harness_session_id value to write. When the
// inbound envelope is nil or signals it lacks a usable
// harness_session_id, a synthetic id is derived from the root node's
// Merkle hash prefix.
func resolveHarnessSessionID(envelope *sessions.IngestEnvelope, root *merkle.Node) (*sessions.IngestEnvelope, string, error) {
	if envelope != nil && !envelope.NeedsSyntheticHarnessSessionID() {
		return envelope, envelope.HarnessSessionID, nil
	}

	if root == nil || root.Hash == "" {
		return nil, "", errors.New("cannot derive synthetic harness_session_id: missing root node hash")
	}
	prefix := root.Hash
	if len(prefix) > syntheticHarnessSessionIDPrefixLen {
		prefix = prefix[:syntheticHarnessSessionIDPrefixLen]
	}

	// Build a non-nil envelope so downstream code can treat envelope
	// fields uniformly. If the caller passed nothing at all, we
	// degrade to the "unknown" harness with empty identity fields;
	// otherwise we preserve whatever identity fields the caller did
	// supply (e.g. org_id) and only synthesize the harness_session_id
	// slot.
	out := &sessions.IngestEnvelope{}
	if envelope != nil {
		*out = *envelope
	}
	if out.HarnessID == "" {
		out.HarnessID = "unknown"
	}
	out.HarnessSessionID = prefix
	return out, prefix, nil
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

// numericFromFloat encodes a float64 dollars amount into pgtype.Numeric
// at 4-decimal scale to match the NUMERIC(12,4) column. A 0.0 input
// becomes Int=0, Exp=0 so the UPDATE writes a true no-op delta. The
// worker currently stubs CostUSD at 0 (no pricing table is wired in
// this repo); this function exists so the path is ready when a
// pricing lookup is added.
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

// insertNodeParamsFromMerkle marshals a merkle.Node into the InsertNode
// param struct. Mirrors the construction in (*Driver).Put so the
// transactional ingest path projects the same columns as the legacy
// per-node Put loop. orgID is the cloud-trusted org for the row's half
// of the composite (org_id, hash) PK.
func insertNodeParamsFromMerkle(orgID pgtype.UUID, n *merkle.Node) (gensqlc.InsertNodeParams, error) {
	if n == nil {
		return gensqlc.InsertNodeParams{}, errors.New("nil node")
	}
	bucketJSON, err := json.Marshal(n.Bucket)
	if err != nil {
		return gensqlc.InsertNodeParams{}, fmt.Errorf("marshal bucket: %w", err)
	}
	contentJSON, err := json.Marshal(n.Bucket.Content)
	if err != nil {
		return gensqlc.InsertNodeParams{}, fmt.Errorf("marshal content: %w", err)
	}

	createdAt := n.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	reqSystem, reqMaxTokens, reqTemperature, reqStream, reqToolCount := requestParamColumns(n.Request)

	return gensqlc.InsertNodeParams{
		OrgID:                    orgID,
		Hash:                     n.Hash,
		Bucket:                   bucketJSON,
		Type:                     nullStringValue(n.Bucket.Type),
		Role:                     nullStringValue(n.Bucket.Role),
		Content:                  contentJSON,
		Model:                    nullStringValue(n.Bucket.Model),
		Provider:                 nullStringValue(n.Bucket.Provider),
		AgentName:                nullStringValue(n.Bucket.AgentName),
		StopReason:               nullStringValue(n.StopReason),
		PromptTokens:             nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.PromptTokens }),
		CompletionTokens:         nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CompletionTokens }),
		TotalTokens:              nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.TotalTokens }),
		CacheCreationInputTokens: nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheCreationInputTokens }),
		CacheReadInputTokens:     nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheReadInputTokens }),
		TotalDurationNs:          nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.TotalDurationNs }),
		PromptDurationNs:         nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.PromptDurationNs }),
		Project:                  nullStringValue(n.Project),
		CreatedAt:                pgtype.Timestamptz{Time: createdAt, Valid: true},
		ParentHash:               nullStringPtr(n.ParentHash),
		RequestSystem:            reqSystem,
		RequestMaxTokens:         reqMaxTokens,
		RequestTemperature:       reqTemperature,
		RequestStream:            reqStream,
		RequestToolCount:         reqToolCount,
		NodeKind:                 nullStringValue(n.Kind),
		ParentToolUseID:          nullStringValue(n.ParentToolUseID),
		ThreadID:                 nullStringValue(n.ThreadID),
	}, nil
}
