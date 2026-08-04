package rawcmder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/rawequiv"
	"github.com/papercomputeco/tapes/pkg/storage"
)

type equivalenceCommander struct {
	flags config.FlagSet

	postgresDSN string
	since       string
	limit       int
	session     string
	jsonOut     bool
	maxDiffs    int
	maxReport   int
}

var equivalenceFlags = config.FlagSet{
	config.FlagPostgres: {
		Name:        "postgres",
		ViperKey:    "storage.postgres_dsn",
		Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)",
	},
}

const equivalenceLongDesc string = `Prove that stored capture bytes re-reduce to the stored reduction.

Capture is moving through a ratchet: off (the adapter ships only its
reduction) -> dual (it ships the reduction AND the verbatim upstream
bytes) -> raw (it ships only bytes and tapes reduces server-side). The
end state is the goal, because one reducer for every capture path is the
only thing that makes two paths incapable of reducing differently.

Dual is where that gets proven. Every dual row carries both halves of the
same turn, so this command can decode the stored bytes, re-reduce them
through the exact path mode=raw would run, and compare the result against
the reduction the adapter stored beside them — over real traffic, without
changing anything an operator sees.

Two fields are excluded from the comparison because neither can survive
moving the reduction off the capture host: created_at, which reducers
stamp at reduction time, and usage.total_duration_ns, the wall clock only
the party that watched the stream can measure. Everything else is
compared strictly. The report prints both exclusions and, separately,
which capture-side stamps mode=raw would have been able to restore --
a window can be perfectly equivalent and still lose durations on the
flip, and those are different questions.

Exits non-zero if any row diverged or failed to reduce, so it can gate a
ratchet step in CI.

The comparison is read-only and never prints response content: a
difference is reported as a JSON path plus the shape of what differs.

Run it against a tenant's database from inside the cluster:

  kubectl exec -n <tenant-ns> deploy/tapes-api -- \
    tapes raw equivalence --since 24h --limit 5000

or locally against a forwarded database:

  tapes raw equivalence \
    --postgres "postgres://user:pass@127.0.0.1:15432/tapes" \
    --since 24h --json`

func newEquivalenceCmd() *cobra.Command {
	cmder := &equivalenceCommander{flags: equivalenceFlags}

	cmd := &cobra.Command{
		Use:   "equivalence",
		Short: "Compare stored reductions against a re-reduction of the stored bytes",
		Long:  equivalenceLongDesc,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{config.FlagPostgres})
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			return cmder.run(cmd)
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	cmd.Flags().StringVar(&cmder.since, "since", "24h",
		"Only turns received within this window (duration like 24h, or an RFC 3339 instant)")
	cmd.Flags().IntVar(&cmder.limit, "limit", 1000,
		"Maximum turns to examine, newest first")
	cmd.Flags().StringVar(&cmder.session, "session", "",
		"Restrict to one harness session id")
	cmd.Flags().BoolVar(&cmder.jsonOut, "json", false,
		"Emit the report as JSON")
	cmd.Flags().IntVar(&cmder.maxDiffs, "max-diffs", rawequiv.DefaultMaxDiffs,
		"Maximum differences recorded per divergent turn")
	cmd.Flags().IntVar(&cmder.maxReport, "max-report", 20,
		"Maximum blocking turns detailed in the report (counts stay exact)")
	return cmd
}

// ErrDivergence is returned when the window contained rows that would not
// survive the flip to mode=raw. It carries no detail of its own: the report is
// already on stdout, and this exists to make the process exit non-zero.
var ErrDivergence = errors.New("raw-response equivalence failed")

func (c *equivalenceCommander) run(cmd *cobra.Command) error {
	if c.postgresDSN == "" {
		return errors.New("equivalence requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}
	since, err := parseSince(c.since)
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	pool, err := openReadOnly(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	report := rawequiv.NewReport(rawequiv.Window{
		Since:   c.since,
		Limit:   c.limit,
		Session: c.session,
	}, c.maxReport)

	opts := rawequiv.Options{MaxDiffs: c.maxDiffs}
	err = scanWireTurns(ctx, pool, since, c.limit, c.session, func(row rawequiv.Row) {
		report.Add(rawequiv.Check(ctx, row, opts))
	})
	if err != nil {
		return err
	}

	if err := writeReport(cmd.OutOrStdout(), report, c.jsonOut); err != nil {
		return err
	}
	if n := report.Blocking(); n > 0 {
		return fmt.Errorf("%w: %d of %d turn(s) would not survive mode=raw",
			ErrDivergence, n, report.Total)
	}
	return nil
}

func writeReport(w io.Writer, report *rawequiv.Report, asJSON bool) error {
	if !asJSON {
		report.WriteText(w)
		return nil
	}
	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	_, err = fmt.Fprintln(w, string(out))
	return err
}

// parseSince accepts either a duration back from now or an absolute RFC 3339
// instant. Operators reach for both: "the last day" while iterating, and a
// fixed instant when re-running the same window after a change.
func parseSince(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d < 0 {
			d = -d
		}
		return time.Now().Add(-d), nil
	}
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"--since %q is neither a duration (24h) nor an RFC 3339 instant", s)
	}
	return ts, nil
}

// openReadOnly opens a connection pool that performs no schema work.
//
// It deliberately does not use postgres.NewDriver or postgres.Open: both run
// golang-migrate before returning the pool, so opening a database through them
// is a DDL write. This command is a diagnostic an operator points at a live
// tenant database — frequently one belonging to a deployment running a
// different build — and having it migrate that schema as a side effect of
// being run would be a genuinely dangerous surprise. Every statement issued
// from here is a SELECT.
func openReadOnly(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	// One connection: this is a single sequential scan, and a diagnostic has
	// no business consuming a tenant database's connection budget.
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// wireTurnQuery selects the columns a re-reduction needs, newest first.
//
// It is written here rather than added to the sqlc query set on purpose. The
// generated layer serves the deriver and the API, whose reads are hot and
// shaped by the projection; this is a one-off diagnostic scan with its own
// window and filters, and adding it to the shared surface would put a
// diagnostic's shape into every build of the storage package. The SQL is
// static and every caller value is a bound parameter.
//
// Attribution corrections are deliberately NOT joined. They repair which
// session a turn belongs to; they do not touch the bytes, the reduction, or
// the capture meta, which is everything the comparison reads. Joining them
// would add cost to answer a question this command does not ask.
//
// source = 'wire' is the population under test. Transcript turns are uploaded
// from a harness's own on-disk log, have no upstream bytes, and are not
// affected by the capture mode at all.
const wireTurnQuery = `
SELECT id,
       request_id,
       provider,
       harness_id,
       harness_session_id,
       COALESCE(meta ->> 'model', '') AS model,
       COALESCE(raw_response, ''::bytea) AS raw_response,
       raw_response_encoding,
       raw_response_dropped,
       response,
       meta
FROM raw_turns
WHERE source = @wire::text
  AND (@since::timestamptz IS NULL OR received_at >= @since::timestamptz)
  AND (@session::text = '' OR harness_session_id = @session::text)
ORDER BY received_at DESC, id DESC
LIMIT @lim`

// scanWireTurns streams the window through visit.
//
// Rows are streamed rather than collected because a raw_response is up to
// 8 MiB and a window is thousands of turns: materializing the window would
// cost gigabytes to answer a question that only ever looks at one row at a
// time.
func scanWireTurns(
	ctx context.Context,
	pool *pgxpool.Pool,
	since time.Time,
	limit int,
	session string,
	visit func(rawequiv.Row),
) error {
	if limit <= 0 {
		limit = 1000
	}

	args := pgx.NamedArgs{
		"lim":     limit,
		"session": session,
		"wire":    storage.RawTurnSourceWire,
	}
	if since.IsZero() {
		args["since"] = nil
	} else {
		args["since"] = since
	}

	rows, err := pool.Query(ctx, wireTurnQuery, args)
	if err != nil {
		return fmt.Errorf("query raw_turns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			row      rawequiv.Row
			response []byte
			meta     []byte
		)
		if err := rows.Scan(
			&row.ID,
			&row.RequestID,
			&row.Provider,
			&row.HarnessID,
			&row.HarnessSessionID,
			&row.Model,
			&row.RawResponse,
			&row.RawResponseEncoding,
			&row.RawResponseDropped,
			&response,
			&meta,
		); err != nil {
			return fmt.Errorf("scan raw_turn: %w", err)
		}

		row.StoredReduction = response
		if len(meta) > 0 {
			// A meta block this build cannot parse is not a reason to fail
			// the scan: the fields the comparison needs are the long-standing
			// ones, and an unknown extension elsewhere in the block is
			// exactly what the raw layer is designed to tolerate.
			_ = json.Unmarshal(meta, &row.Meta)
		}
		visit(row)
	}
	return rows.Err()
}
