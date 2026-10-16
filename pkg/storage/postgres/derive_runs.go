package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// deriveRunLockClass is the first key of the two-key advisory lock a run's
// executor holds. Two-key advisory locks occupy a different keyspace from the
// single-bigint per-session derive locks, so the two can never collide.
const deriveRunLockClass int32 = 0x74617065 // "tape"

// deriveRunPageSize is how many unfinished sessions an executor reads per
// keyset page.
const deriveRunPageSize = 100

// deriveRunErrorSample bounds the per-session failures kept on the run.
const deriveRunErrorSample = 20

// uniqueViolation is SQLSTATE 23505.
const uniqueViolation = "23505"

// StartDeriveRun creates a derive run and snapshots the session set it covers,
// or returns the run already active. created reports which happened: a store
// has at most one active run, and asking for a second one is an idempotent
// request for the first.
//
// The run is only recorded here; ExecuteDeriveRun does the work.
func (d *Driver) StartDeriveRun(ctx context.Context, project string) (run derive.Run, created bool, err error) {
	if d == nil || d.conn == nil {
		return derive.Run{}, false, errors.New("postgres driver not open")
	}
	if active, err := d.q.GetActiveDeriveRun(ctx); err == nil {
		out, err := deriveRunFromRow(active)
		return out, false, err
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return derive.Run{}, false, fmt.Errorf("look up active derive run: %w", err)
	}

	id, err := newAppUUID()
	if err != nil {
		return derive.Run{}, false, fmt.Errorf("mint derive run id: %w", err)
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return derive.Run{}, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	row, err := qtx.InsertDeriveRun(ctx, gensqlc.InsertDeriveRunParams{ID: id, Project: project})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			// Lost a race with a concurrent start: answer with its run.
			_ = tx.Rollback(ctx)
			active, getErr := d.q.GetActiveDeriveRun(ctx)
			if getErr != nil {
				return derive.Run{}, false, fmt.Errorf("look up active derive run: %w", getErr)
			}
			out, convErr := deriveRunFromRow(active)
			return out, false, convErr
		}
		return derive.Run{}, false, fmt.Errorf("insert derive run: %w", err)
	}
	total, err := qtx.SnapshotDeriveRunSessions(ctx, id)
	if err != nil {
		return derive.Run{}, false, fmt.Errorf("snapshot derive run sessions: %w", err)
	}
	if err := qtx.SetDeriveRunTotal(ctx, gensqlc.SetDeriveRunTotalParams{ID: id, SessionsTotal: int32(total)}); err != nil { //nolint:gosec // a session count fits in int32
		return derive.Run{}, false, fmt.Errorf("record derive run total: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return derive.Run{}, false, fmt.Errorf("commit derive run: %w", err)
	}
	row.SessionsTotal = int32(total) //nolint:gosec // as above
	out, err := deriveRunFromRow(row)
	return out, true, err
}

// GetDeriveRun returns one run, or derive.ErrRunNotFound.
func (d *Driver) GetDeriveRun(ctx context.Context, id string) (derive.Run, error) {
	if d == nil || d.conn == nil {
		return derive.Run{}, errors.New("postgres driver not open")
	}
	runID, err := parseDeriveRunID(id)
	if err != nil {
		return derive.Run{}, err
	}
	row, err := d.q.GetDeriveRun(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return derive.Run{}, derive.ErrRunNotFound
	}
	if err != nil {
		return derive.Run{}, fmt.Errorf("get derive run: %w", err)
	}
	return deriveRunFromRow(row)
}

// ListDeriveRuns returns the most recent runs, newest first.
func (d *Driver) ListDeriveRuns(ctx context.Context, limit int) ([]derive.Run, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	rows, err := d.q.ListDeriveRuns(ctx, int32(limit)) //nolint:gosec // callers bound limit
	if err != nil {
		return nil, fmt.Errorf("list derive runs: %w", err)
	}
	runs := make([]derive.Run, 0, len(rows))
	for _, row := range rows {
		run, err := deriveRunFromRow(row)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// ListActiveDeriveRunIDs returns the ids of runs that still have work to do,
// oldest first.
func (d *Driver) ListActiveDeriveRunIDs(ctx context.Context) ([]string, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	ids, err := d.q.ListActiveDeriveRunIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active derive runs: %w", err)
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, uuidString(id))
	}
	return out, nil
}

// CancelDeriveRun stops an active run. An executor notices between sessions.
// Canceling a run that is no longer active returns it unchanged.
func (d *Driver) CancelDeriveRun(ctx context.Context, id string) (derive.Run, error) {
	if d == nil || d.conn == nil {
		return derive.Run{}, errors.New("postgres driver not open")
	}
	runID, err := parseDeriveRunID(id)
	if err != nil {
		return derive.Run{}, err
	}
	row, err := d.q.CancelDeriveRun(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return d.GetDeriveRun(ctx, id)
	}
	if err != nil {
		return derive.Run{}, fmt.Errorf("cancel derive run: %w", err)
	}
	return deriveRunFromRow(row)
}

// ExecuteDeriveRun advances a run to completion: it re-derives every
// unfinished session in the run's snapshot, marking each finished as it goes,
// and closes the run once none remain.
//
// It holds the run's advisory lock for as long as it executes, on a connection
// of its own, and returns derive.ErrRunBusy without doing anything when
// another process holds it. The lock is released when this returns or when
// this process's connection goes away, so a run whose executor died is picked
// up by the next caller.
//
// A session whose derive fails is recorded as finished with its error and the
// run moves on: one bad session must not hold a whole-store run hostage. Any
// other error — the database itself failing, or ctx ending — returns with the
// run still running, to be resumed from its first unfinished session.
//
// worker names this process on the run record.
func (d *Driver) ExecuteDeriveRun(ctx context.Context, id, worker string) (derive.Run, error) {
	if d == nil || d.conn == nil {
		return derive.Run{}, errors.New("postgres driver not open")
	}
	runID, err := parseDeriveRunID(id)
	if err != nil {
		return derive.Run{}, err
	}

	lockConn, err := d.conn.Acquire(ctx)
	if err != nil {
		return derive.Run{}, fmt.Errorf("acquire conn for derive run lock: %w", err)
	}
	defer lockConn.Release()
	lockKey := deriveRunLockKey(id)
	var locked bool
	if err := lockConn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, $2)", deriveRunLockClass, lockKey).Scan(&locked); err != nil {
		return derive.Run{}, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	if !locked {
		return derive.Run{}, derive.ErrRunBusy
	}
	defer func() { //nolint:contextcheck // unlock even when ctx has ended
		_, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, $2)", deriveRunLockClass, lockKey)
	}()

	row, err := d.q.MarkDeriveRunRunning(ctx, gensqlc.MarkDeriveRunRunningParams{ID: runID, Worker: worker})
	if errors.Is(err, pgx.ErrNoRows) {
		// Not active (completed or canceled since it was listed), or gone.
		return d.GetDeriveRun(ctx, id)
	}
	if err != nil {
		return derive.Run{}, fmt.Errorf("mark derive run running: %w", err)
	}

	report := newRederiveReport()
	if len(row.Report) > 0 {
		if err := json.Unmarshal(row.Report, report); err != nil {
			return derive.Run{}, fmt.Errorf("decode derive run report: %w", err)
		}
		if report.CallKinds == nil {
			report.CallKinds = map[string]int{}
		}
		if report.NodeKinds == nil {
			report.NodeKinds = map[string]int{}
		}
	}
	var sample []string
	if len(row.Errors) > 0 {
		if err := json.Unmarshal(row.Errors, &sample); err != nil {
			return derive.Run{}, fmt.Errorf("decode derive run errors: %w", err)
		}
	}
	project := row.Project

	after := gensqlc.ListPendingDeriveRunSessionsParams{RunID: runID, FirstPage: true, AfterOrgID: pgtype.UUID{Valid: true}, PageLimit: deriveRunPageSize}
	for {
		page, err := d.q.ListPendingDeriveRunSessions(ctx, after)
		if err != nil {
			return derive.Run{}, fmt.Errorf("list pending derive run sessions: %w", err)
		}
		if len(page) == 0 {
			break
		}
		for _, key := range page {
			orgID := uuidString(key.OrgID)
			sessionReport, deriveErr := d.RederiveSessionLocked(ctx, project, orgID, key.HarnessID, key.HarnessSessionID)
			if ctx.Err() != nil {
				// Interrupted mid-session: leave it unfinished for the resume.
				return derive.Run{}, ctx.Err()
			}
			var sessionErr pgtype.Text
			if deriveErr != nil {
				sessionErr = pgtype.Text{String: deriveErr.Error(), Valid: true}
				if len(sample) < deriveRunErrorSample {
					sample = append(sample, fmt.Sprintf("%s/%s: %s", key.HarnessID, key.HarnessSessionID, deriveErr))
				}
				d.logger.Warn("derive run session failed",
					"run_id", id, "harness_id", key.HarnessID, "harness_session_id", key.HarnessSessionID, "error", deriveErr)
			} else if sessionReport != nil {
				mergeRederiveReport(report, sessionReport)
			}
			reportJSON, err := json.Marshal(report)
			if err != nil {
				return derive.Run{}, fmt.Errorf("encode derive run report: %w", err)
			}
			errorsJSON, err := json.Marshal(nonNilStrings(sample))
			if err != nil {
				return derive.Run{}, fmt.Errorf("encode derive run errors: %w", err)
			}
			state, err := d.q.FinishDeriveRunSession(ctx, gensqlc.FinishDeriveRunSessionParams{
				Error:            sessionErr,
				Report:           reportJSON,
				Errors:           errorsJSON,
				RunID:            runID,
				OrgID:            key.OrgID,
				HarnessID:        key.HarnessID,
				HarnessSessionID: key.HarnessSessionID,
			})
			if err != nil {
				return derive.Run{}, fmt.Errorf("finish derive run session: %w", err)
			}
			if derive.RunState(state) == derive.RunCanceled {
				d.logger.Info("derive run canceled", "run_id", id)
				return d.GetDeriveRun(ctx, id)
			}
		}
		last := page[len(page)-1]
		after.FirstPage = false
		after.AfterOrgID = last.OrgID
		after.AfterHarnessID = last.HarnessID
		after.AfterHarnessSessionID = last.HarnessSessionID
	}

	if _, err := d.q.CompleteDeriveRun(ctx, runID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return derive.Run{}, fmt.Errorf("complete derive run: %w", err)
	}
	return d.GetDeriveRun(ctx, id)
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// deriveRunLockKey folds a run id into the second advisory-lock key.
func deriveRunLockKey(id string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int32(h.Sum32()) //nolint:gosec // deliberate wraparound into the signed advisory keyspace
}

func parseDeriveRunID(id string) (pgtype.UUID, error) {
	runID, err := orgIDFromString(id)
	if err != nil || id == "" {
		return pgtype.UUID{}, derive.ErrRunNotFound
	}
	return runID, nil
}

func deriveRunFromRow(row gensqlc.DeriveRun) (derive.Run, error) {
	run := derive.Run{
		ID:             uuidString(row.ID),
		Project:        row.Project,
		State:          derive.RunState(row.State),
		SessionsTotal:  int(row.SessionsTotal),
		SessionsDone:   int(row.SessionsDone),
		SessionsFailed: int(row.SessionsFailed),
		Worker:         row.Worker,
		CreatedAt:      row.CreatedAt.Time,
		UpdatedAt:      row.UpdatedAt.Time,
		StartedAt:      optionalTime(row.StartedAt),
		FinishedAt:     optionalTime(row.FinishedAt),
		Errors:         []string{},
	}
	if len(row.Report) > 0 && string(row.Report) != "{}" {
		report := &derive.RederiveReport{}
		if err := json.Unmarshal(row.Report, report); err != nil {
			return derive.Run{}, fmt.Errorf("decode derive run report: %w", err)
		}
		run.Report = report
	}
	if len(row.Errors) > 0 {
		if err := json.Unmarshal(row.Errors, &run.Errors); err != nil {
			return derive.Run{}, fmt.Errorf("decode derive run errors: %w", err)
		}
		run.Errors = nonNilStrings(run.Errors)
	}
	return run, nil
}

func optionalTime(ts pgtype.Timestamptz) *time.Time {
	if !ts.Valid {
		return nil
	}
	t := ts.Time
	return &t
}
