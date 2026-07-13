package devcmder

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

type rederiveCommander struct {
	flags config.FlagSet

	postgresDSN string
	project     string
}

var rederiveFlags = config.FlagSet{
	config.FlagPostgres: {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
}

const rederiveLongDesc string = `Rebuild the derived projection from the raw-turn layer.

Runs the same whole-project re-derive as POST /v1/admin/derive/run —
drop the sessions/traces/spans/links projection and rebuild it as a pure
function of raw_turns, pruning anything no longer present — but as a
direct database call rather than an HTTP request. Prefer this over the
admin endpoint for large corpora: a full re-derive can run for minutes,
longer than an HTTP client or port-forward will hold an idle connection,
whereas this streams a report to stdout when it finishes.

The sessions identity row is ingest-written and skipped here (a session
whose identity never landed is not resurrected); everything else — the
projection plus the rollup columns (status, tasks, kind_counts, per-span
verdict, capture source) — is rebuilt. Idempotent and re-runnable: the
lever that makes classifier/projection iteration cheap.

Reports are keyed per org. Example:
  tapes dev rederive --postgres "postgres://user:pass@127.0.0.1:15432/tapes"`

func newRederiveCmd() *cobra.Command {
	cmder := &rederiveCommander{flags: rederiveFlags}

	cmd := &cobra.Command{
		Use:   "rederive",
		Short: "Rebuild the derived projection from raw_turns (direct DB call)",
		Long:  rederiveLongDesc,
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
	cmd.Flags().StringVar(&cmder.project, "project", "", "Restrict the re-derive to one project (default: all)")
	return cmd
}

func (c *rederiveCommander) run(cmd *cobra.Command) error {
	if c.postgresDSN == "" {
		return errors.New("rederive requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}

	ctx := cmd.Context()
	driver, err := postgres.NewDriver(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	reports, err := driver.RederiveFromRaw(ctx, c.project)
	if err != nil {
		return fmt.Errorf("re-derive: %w", err)
	}

	out, err := json.MarshalIndent(map[string]any{"orgs": reports}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal reports: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}
