package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

var _ storage.RawTurnAttributionRepairer = (*Driver)(nil)

// repairRederive is the synchronous rebuild the repair flow runs for each
// affected session. A package variable purely as a test seam: the repair
// contract under a MID-FLIGHT rederive failure (correction committed, one
// projection rebuilt, the other stale) is otherwise unreachable from a
// black-box test, since RederiveSession has no injectable failure mode.
var repairRederive = (*Driver).RederiveSession

// repairSourceCleanup removes the emptied previous-session row once a repair
// has applied. A package variable purely as a test seam, for the same reason
// as repairRederive: a cleanup-only failure (correction committed, both
// projections rebuilt, deletion failed) has no black-box trigger.
var repairSourceCleanup = (*Driver).deleteEmptyUnreferencedRepairSource

type repairSessionKey struct {
	harnessID        string
	harnessSessionID string
}

// RepairRawTurnAttribution appends an attribution correction and rebuilds both
// affected projections. raw_turns remains byte-for-byte untouched.
func (d *Driver) RepairRawTurnAttribution(
	ctx context.Context,
	project string,
	req storage.RawTurnAttributionRepairRequest,
) (storage.RawTurnAttributionRepairResult, error) {
	var zero storage.RawTurnAttributionRepairResult
	if d == nil || d.conn == nil {
		return zero, errors.New("postgres driver not open")
	}
	if err := validateAttributionRepair(req); err != nil {
		return zero, err
	}
	orgID, err := orgIDFromString(orgKeyForLookup(req.OrgID))
	if err != nil {
		return zero, fmt.Errorf("decode org_id: %w", err)
	}
	rawTurnID, err := d.resolveRepairRawTurnID(ctx, orgID, req)
	if err != nil {
		return zero, err
	}

	for {
		observed, err := d.q.GetRawTurnAttributionForUpdate(ctx, gensqlc.GetRawTurnAttributionForUpdateParams{
			OrgID: orgID, RawTurnID: rawTurnID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, storage.ErrRawTurnNotFound
		}
		if err != nil {
			return zero, fmt.Errorf("read effective raw-turn attribution: %w", err)
		}

		oldKey := repairSessionKey{observed.HarnessID, observed.HarnessSessionID}
		newKey := repairSessionKey{req.HarnessID, req.HarnessSessionID}
		release, err := d.acquireRepairSessionLocks(ctx, req.OrgID, oldKey, newKey)
		if err != nil {
			return zero, err
		}

		result, retry, err := d.recordAttributionRepair(ctx, orgID, rawTurnID, req, oldKey, newKey)
		if retry {
			release()
			continue
		}
		if err != nil {
			release()
			return zero, err
		}

		_, oldErr := repairRederive(d, ctx, project, req.OrgID, oldKey.harnessID, oldKey.harnessSessionID)
		var cleanupErr error
		if oldErr == nil && oldKey != newKey {
			cleanupErr = repairSourceCleanup(d, ctx, orgID, oldKey)
		}
		var newErr error
		if oldKey != newKey {
			_, newErr = repairRederive(d, ctx, project, req.OrgID, newKey.harnessID, newKey.harnessSessionID)
		}

		// The correction is already committed and effective at read time —
		// from here the repair cannot "fail", only settle asynchronously. A
		// rederive failure leaves that session's projection stale, but the
		// correction transaction marked both sessions derive-dirty, and the
		// worker cannot have cleared those marks yet (clearing requires the
		// per-session advisory locks still held here), so the queue converges
		// them. Re-mark under the locks anyway: it makes this path's
		// convergence locally evident instead of resting on the transaction
		// two calls away, at worst re-bumping dirtied_at.
		var pendingErrs []error
		if oldErr != nil {
			result.ProjectionsPending = append(result.ProjectionsPending,
				storage.RepairPendingSession{HarnessID: oldKey.harnessID, HarnessSessionID: oldKey.harnessSessionID})
			pendingErrs = append(pendingErrs, fmt.Errorf("rederive previous session: %w", oldErr))
		}
		if newErr != nil {
			result.ProjectionsPending = append(result.ProjectionsPending,
				storage.RepairPendingSession{HarnessID: newKey.harnessID, HarnessSessionID: newKey.harnessSessionID})
			pendingErrs = append(pendingErrs, fmt.Errorf("rederive effective session: %w", newErr))
		}
		for _, pending := range result.ProjectionsPending {
			if markErr := d.MarkDeriveDirty(ctx, req.OrgID, pending.HarnessID, pending.HarnessSessionID); markErr != nil {
				pendingErrs = append(pendingErrs, fmt.Errorf("re-mark %s/%s derive-dirty: %w", pending.HarnessID, pending.HarnessSessionID, markErr))
			}
		}
		release()
		// A cleanup failure is strictly cosmetic: the repair has applied and
		// the emptied source row anchors no effective turns. No worker
		// retries the deletion — the flag keeps the response honest about
		// the leftover rather than promising later cleanup — so it must
		// never surface as a failure of an already-applied repair.
		result.SourceCleanupPending = cleanupErr != nil
		if len(pendingErrs) > 0 {
			if cleanupErr != nil {
				pendingErrs = append(pendingErrs, cleanupErr)
			}
			return result, fmt.Errorf("%w: %w", storage.ErrRepairProjectionsPending, errors.Join(pendingErrs...))
		}
		if cleanupErr != nil {
			d.logger.Warn("repaired source session cleanup failed; leaving empty source row",
				"harness_id", oldKey.harnessID,
				"harness_session_id", oldKey.harnessSessionID,
				"error", cleanupErr)
		}
		return result, nil
	}
}

func (d *Driver) deleteEmptyUnreferencedRepairSource(
	ctx context.Context,
	orgID pgtype.UUID,
	key repairSessionKey,
) error {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin repaired source cleanup: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := d.q.WithTx(tx).DeleteEmptyUnreferencedSession(
		ctx,
		gensqlc.DeleteEmptyUnreferencedSessionParams{
			OrgID:            orgID,
			HarnessID:        key.harnessID,
			HarnessSessionID: key.harnessSessionID,
		},
	); err != nil {
		return fmt.Errorf("delete empty repaired source session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit repaired source cleanup: %w", err)
	}
	return nil
}

func validateAttributionRepair(req storage.RawTurnAttributionRepairRequest) error {
	if (req.RawTurnID > 0) == (strings.TrimSpace(req.PaperProxyRequestID) != "") {
		return errors.New("exactly one of raw_turn_id or paper_proxy_request_id is required")
	}
	if req.HarnessID == "" {
		return errors.New("harness_id is required")
	}
	if req.HarnessSessionID == "" {
		return errors.New("harness_session_id is required")
	}
	if req.ParentHarnessSessionID != nil && *req.ParentHarnessSessionID == "" {
		return errors.New("parent_harness_session_id must be omitted instead of empty")
	}
	if req.ParentHarnessSessionID != nil && *req.ParentHarnessSessionID == req.HarnessSessionID {
		return errors.New("a session cannot parent itself")
	}
	if strings.TrimSpace(req.Reason) == "" {
		return errors.New("reason is required")
	}
	return nil
}

func (d *Driver) resolveRepairRawTurnID(
	ctx context.Context,
	orgID pgtype.UUID,
	req storage.RawTurnAttributionRepairRequest,
) (int64, error) {
	if req.RawTurnID > 0 {
		return req.RawTurnID, nil
	}
	ids, err := d.q.FindRawTurnIDsByPaperProxyRequestID(ctx, gensqlc.FindRawTurnIDsByPaperProxyRequestIDParams{
		OrgID: orgID, PaperProxyRequestID: req.PaperProxyRequestID,
	})
	if err != nil {
		return 0, fmt.Errorf("resolve paper proxy request id: %w", err)
	}
	switch len(ids) {
	case 0:
		return 0, storage.ErrRawTurnNotFound
	case 1:
		return ids[0], nil
	default:
		return 0, storage.ErrRawTurnAmbiguous
	}
}

func (d *Driver) acquireRepairSessionLocks(
	ctx context.Context,
	orgID string,
	keys ...repairSessionKey,
) (func(), error) {
	unique := make(map[repairSessionKey]struct{}, len(keys))
	ordered := make([]repairSessionKey, 0, len(keys))
	for _, key := range keys {
		if _, ok := unique[key]; ok {
			continue
		}
		unique[key] = struct{}{}
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].harnessID+"\x00"+ordered[i].harnessSessionID < ordered[j].harnessID+"\x00"+ordered[j].harnessSessionID
	})
	releases := make([]func(), 0, len(ordered))
	for _, key := range ordered {
		release, err := d.AcquireDeriveSessionLock(ctx, orgID, key.harnessID, key.harnessSessionID)
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, fmt.Errorf("acquire repair session lock %s/%s: %w", key.harnessID, key.harnessSessionID, err)
		}
		releases = append(releases, release)
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, nil
}

func (d *Driver) recordAttributionRepair(
	ctx context.Context,
	orgID pgtype.UUID,
	rawTurnID int64,
	req storage.RawTurnAttributionRepairRequest,
	lockedOld, newKey repairSessionKey,
) (storage.RawTurnAttributionRepairResult, bool, error) {
	var zero storage.RawTurnAttributionRepairResult
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return zero, false, fmt.Errorf("begin attribution repair: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	qtx := d.q.WithTx(tx)
	current, err := qtx.GetRawTurnAttributionForUpdate(ctx, gensqlc.GetRawTurnAttributionForUpdateParams{
		OrgID: orgID, RawTurnID: rawTurnID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, storage.ErrRawTurnNotFound
	}
	if err != nil {
		return zero, false, fmt.Errorf("lock raw turn for repair: %w", err)
	}
	if current.HarnessID != lockedOld.harnessID || current.HarnessSessionID != lockedOld.harnessSessionID {
		return zero, true, nil
	}

	previous := attributionFromRow(current)
	effective := storage.RawTurnAttribution{
		RawTurnID: rawTurnID, HarnessID: req.HarnessID,
		HarnessSessionID: req.HarnessSessionID, ThreadID: req.ThreadID,
		ParentHarnessSessionID: cloneStringPointer(req.ParentHarnessSessionID),
	}
	recorded := !sameAttribution(previous, effective)

	var envelope sessions.IngestEnvelope
	if len(current.SessionEnvelope) > 0 {
		if err := json.Unmarshal(current.SessionEnvelope, &envelope); err != nil {
			return zero, false, fmt.Errorf("decode raw session envelope: %w", err)
		}
	}
	envelope.OrgID = req.OrgID
	envelope.HarnessID = req.HarnessID
	envelope.HarnessSessionID = req.HarnessSessionID
	envelope.ParentHarnessSessionID = cloneStringPointer(req.ParentHarnessSessionID)
	capturedAt := current.ReceivedAt
	parentID, err := resolveParentSessionID(ctx, qtx, &envelope, orgID, capturedAt)
	if err != nil {
		return zero, false, fmt.Errorf("resolve repaired parent session: %w", err)
	}
	sessionID, err := newAppUUID()
	if err != nil {
		return zero, false, fmt.Errorf("mint repaired session uuid: %w", err)
	}
	metadata := []byte(envelope.HarnessMetadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}
	target, err := qtx.UpsertSessionForAttributionRepair(ctx, gensqlc.UpsertSessionForAttributionRepairParams{
		ID: sessionID, OrgID: orgID, AuthSubject: envelope.AuthSubject,
		HarnessID: req.HarnessID, HarnessSessionID: req.HarnessSessionID,
		Name: nullStringValue(envelope.Name), Cwd: nullStringValue(envelope.Cwd),
		HarnessVersion: nullStringValue(envelope.HarnessVersion), ParentSessionID: parentID,
		CapturedAt: capturedAt, HarnessMetadata: metadata,
	})
	if err != nil {
		return zero, false, fmt.Errorf("upsert repaired session: %w", err)
	}
	if err := qtx.SetSessionParent(ctx, gensqlc.SetSessionParentParams{ParentSessionID: parentID, ID: target.ID}); err != nil {
		return zero, false, fmt.Errorf("set repaired session parent: %w", err)
	}
	if recorded {
		if err := qtx.InsertRawTurnAttributionCorrection(ctx, gensqlc.InsertRawTurnAttributionCorrectionParams{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: req.HarnessID,
			HarnessSessionID: req.HarnessSessionID, ThreadID: req.ThreadID,
			ParentHarnessSessionID: textFromPointer(req.ParentHarnessSessionID), Reason: req.Reason,
		}); err != nil {
			return zero, false, fmt.Errorf("append raw-turn attribution correction: %w", err)
		}
	}
	for _, key := range []repairSessionKey{lockedOld, newKey} {
		if err := qtx.MarkDeriveDirty(ctx, gensqlc.MarkDeriveDirtyParams{
			OrgID: orgID, HarnessID: key.harnessID, HarnessSessionID: key.harnessSessionID,
		}); err != nil {
			return zero, false, fmt.Errorf("mark repaired session dirty: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, false, fmt.Errorf("commit attribution repair: %w", err)
	}
	return storage.RawTurnAttributionRepairResult{Recorded: recorded, Previous: previous, Effective: effective}, false, nil
}

func attributionFromRow(row gensqlc.GetRawTurnAttributionForUpdateRow) storage.RawTurnAttribution {
	parent := row.RawParentHarnessSessionID
	if row.HasCorrection {
		parent = row.CorrectedParentHarnessSessionID
	}
	return storage.RawTurnAttribution{
		RawTurnID: row.ID, HarnessID: row.HarnessID,
		HarnessSessionID: row.HarnessSessionID, ThreadID: row.ThreadID,
		ParentHarnessSessionID: pointerFromString(parent),
	}
}

func sameAttribution(a, b storage.RawTurnAttribution) bool {
	return a.HarnessID == b.HarnessID && a.HarnessSessionID == b.HarnessSessionID &&
		a.ThreadID == b.ThreadID && pointerValue(a.ParentHarnessSessionID) == pointerValue(b.ParentHarnessSessionID)
}

func pointerValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func cloneStringPointer(v *string) *string {
	if v == nil {
		return nil
	}
	cloned := *v
	return &cloned
}

func pointerFromString(v string) *string {
	if v == "" {
		return nil
	}
	return cloneStringPointer(&v)
}

func textFromPointer(v *string) pgtype.Text {
	if v == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *v, Valid: true}
}
