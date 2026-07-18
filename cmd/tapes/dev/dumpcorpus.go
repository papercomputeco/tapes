package devcmder

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// dumpCorpusPageSize is the raw-layer scan page size. The raw layer is
// scanned in id order via a keyset cursor, so this only bounds per-round
// memory, not total rows.
const dumpCorpusPageSize = 500

type dumpCorpusCommander struct {
	flags config.FlagSet

	postgresDSN string
	session     string
	all         bool
	orgID       string
	outPath     string
	pageSize    int
}

var dumpCorpusFlags = config.FlagSet{
	config.FlagPostgres: {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
}

const dumpCorpusLongDesc string = `Export raw_turns from a database into corpus files.

A corpus file is a gzipped-JSONL dump of one session's raw_turns rows —
the exact input the deriver replays (` + "`tapes dev trace-fixtures`" + `, the
corpus regression tests, ` + "`tapes seed`" + `). This is the inverse: it turns
real captured sessions in a Postgres raw layer back into replayable
corpus files, so diverse real-world sessions can drive the re-derive
test tiers, not just the three curated fixtures.

The raw layer is scanned once in insertion order and fanned out to one
file per session, streaming — payloads are never buffered.

Modes (exactly one of):

  --session <harness-session-id>   dump that one session; --out is the
                                   destination FILE.
  --all                            dump every session; --out is a
                                   DIRECTORY, one corpus-<id>.jsonl.gz
                                   file per session.

--org scopes either mode to a single org UUID.

Rows without a harness session id (captures that carried no session
envelope) cannot be attributed to a session and are skipped; the count
is reported.

Examples:
  # One session to a named file, against a port-forwarded clearing DB:
  tapes dev dump-corpus --postgres "$DSN" \
    --session 0440f43d-5d24-4a5a-8385-da5d0a1f60f6 \
    --out ./corpus-0440f43d.jsonl.gz

  # Every session in one org into a directory:
  tapes dev dump-corpus --postgres "$DSN" \
    --org 00000000-0000-0000-0000-000000000000 \
    --all --out ./corpus-local/`

func newDumpCorpusCmd() *cobra.Command {
	cmder := &dumpCorpusCommander{flags: dumpCorpusFlags}

	cmd := &cobra.Command{
		Use:   "dump-corpus",
		Short: "Export raw_turns from a database into corpus files",
		Long:  dumpCorpusLongDesc,
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
	cmd.Flags().StringVar(&cmder.session, "session", "", "Harness session id to dump (--out is the destination file)")
	cmd.Flags().BoolVar(&cmder.all, "all", false, "Dump every session (--out is a directory)")
	cmd.Flags().StringVar(&cmder.orgID, "org", "", "Only dump sessions belonging to this org UUID (default: all orgs)")
	cmd.Flags().StringVar(&cmder.outPath, "out", "", "Destination file (--session) or directory (--all)")
	cmd.Flags().IntVar(&cmder.pageSize, "page-size", dumpCorpusPageSize, "Raw-layer scan page size")

	return cmd
}

func (c *dumpCorpusCommander) run(cmd *cobra.Command) error {
	if err := c.validate(); err != nil {
		return err
	}

	ctx := cmd.Context()
	driver, err := postgres.NewDriver(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	sink, err := c.newSink()
	if err != nil {
		return err
	}
	defer func() { _ = sink.closeAll() }() // safety net; run() closes and checks explicitly

	scanned, skipped, err := c.scan(ctx, driver, sink)
	if err != nil {
		return err
	}
	if err := sink.closeAll(); err != nil {
		return err
	}

	if sink.sessionCount() == 0 {
		if c.session != "" {
			return fmt.Errorf("no raw turns found for session %q (scanned %d rows)", c.session, scanned)
		}
		return fmt.Errorf("no attributable sessions found (scanned %d rows, %d unattributed)", scanned, skipped)
	}

	sink.report(cmd, scanned, skipped)
	return nil
}

func (c *dumpCorpusCommander) validate() error {
	if c.postgresDSN == "" {
		return errors.New("dump-corpus requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}
	if c.session == "" && !c.all {
		return errors.New("dump-corpus requires exactly one of --session or --all")
	}
	if c.session != "" && c.all {
		return errors.New("--session and --all are mutually exclusive")
	}
	if c.outPath == "" {
		return errors.New("dump-corpus requires --out")
	}
	if c.pageSize <= 0 || c.pageSize > math.MaxInt32 {
		return errors.New("--page-size must be between 1 and 2147483647")
	}
	return nil
}

// scan walks the raw layer once, routing each attributable row to its
// session sink. Returns the total rows scanned and the count skipped for
// having no session id.
func (c *dumpCorpusCommander) scan(ctx context.Context, driver *postgres.Driver, sink *corpusSink) (scanned, skipped int, err error) {
	var afterID int64
	for {
		rows, err := driver.ListRawTurns(ctx, afterID, int32(c.pageSize)) //nolint:gosec // bounded to int32 range by validate()
		if err != nil {
			return scanned, skipped, fmt.Errorf("list raw turns after id %d: %w", afterID, err)
		}
		if len(rows) == 0 {
			break
		}
		for i := range rows {
			row := &rows[i]
			afterID = row.ID
			scanned++
			if !c.wants(row) {
				if row.HarnessSessionID == "" && c.matchesOrg(row) {
					skipped++
				}
				continue
			}
			if err := sink.write(row); err != nil {
				return scanned, skipped, err
			}
		}
	}
	return scanned, skipped, nil
}

// wants reports whether a row belongs in the dump: it must carry a
// session id, pass the org filter, and (in --session mode) match the
// requested session.
func (c *dumpCorpusCommander) wants(row *storage.RawTurnRecord) bool {
	if row.HarnessSessionID == "" || !c.matchesOrg(row) {
		return false
	}
	if c.session != "" {
		return row.HarnessSessionID == c.session
	}
	return true
}

func (c *dumpCorpusCommander) matchesOrg(row *storage.RawTurnRecord) bool {
	return c.orgID == "" || row.OrgID == c.orgID
}

// newSink builds the output sink for the selected mode: a single fixed
// file for --session, or a per-session directory fan-out for --all.
func (c *dumpCorpusCommander) newSink() (*corpusSink, error) {
	if c.session != "" {
		if dir := filepath.Dir(c.outPath); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create output dir %s: %w", dir, err)
			}
		}
		return &corpusSink{fixedPath: c.outPath, writers: map[string]*sessionWriter{}}, nil
	}
	if err := os.MkdirAll(c.outPath, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir %s: %w", c.outPath, err)
	}
	return &corpusSink{dir: c.outPath, writers: map[string]*sessionWriter{}}, nil
}

// corpusSink fans raw rows out to one open corpus writer per session.
// In --session mode fixedPath pins the single output file; in --all mode
// dir holds one corpus-<id>.jsonl.gz per session.
type corpusSink struct {
	fixedPath string
	dir       string
	writers   map[string]*sessionWriter
}

type sessionWriter struct {
	path string
	file *os.File
	cw   *derive.CorpusWriter
	rows int
}

func (s *corpusSink) write(row *storage.RawTurnRecord) error {
	w, ok := s.writers[row.HarnessSessionID]
	if !ok {
		path := s.fixedPath
		if path == "" {
			path = filepath.Join(s.dir, fmt.Sprintf("corpus-%s.jsonl.gz", row.HarnessSessionID))
		}
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create %s: %w", path, err)
		}
		w = &sessionWriter{path: path, file: f, cw: derive.NewCorpusWriter(f)}
		s.writers[row.HarnessSessionID] = w
	}
	if err := w.cw.Write(row); err != nil {
		return err
	}
	w.rows++
	return nil
}

// closeAll flushes and closes every open writer. It is idempotent — the
// deferred safety-net call after an explicit close is a no-op.
func (s *corpusSink) closeAll() error {
	var firstErr error
	for key, w := range s.writers {
		if w.cw != nil {
			if err := w.cw.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			w.cw = nil
		}
		if w.file != nil {
			if err := w.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			w.file = nil
		}
		_ = key
	}
	return firstErr
}

func (s *corpusSink) sessionCount() int { return len(s.writers) }

func (s *corpusSink) report(cmd *cobra.Command, scanned, skipped int) {
	keys := make([]string, 0, len(s.writers))
	for k := range s.writers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := s.writers[k]
		cmd.Printf("wrote %s (%d rows)\n", w.path, w.rows)
	}
	cmd.Printf("dumped %d session(s) from %d scanned rows (%d unattributed skipped)\n",
		len(s.writers), scanned, skipped)
}
