package backfillcmder

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

type previewsCommander struct {
	flags config.FlagSet

	postgresDSN string
	session     string
	batch       int
	pause       time.Duration
	dryRun      bool
}

var previewsFlags = config.FlagSet{
	config.FlagPostgres: {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
}

const previewsLongDesc string = `Fill stored span previews on rows derived before previews existed.

The deriver writes input_preview / output_preview beside every span
payload it stores. Rows derived before those columns existed carry NULL
and are served as pending. This job fills them in from the payload
already on the row — the same 512-rune projection the deriver applies —
without re-deriving anything: the payload, content_hash and derive_seq
are read or left alone, never rewritten, so no change-feed consumer sees
a backfilled row as changed.

It runs directly against the database, in keyset-paged batches with one
bounded transaction per batch and a pause between batches, so it can run
beside a live deployment. Idempotent and resumable: a row that has
previews no longer matches, so re-running an interrupted job picks up
where it stopped and a completed job is a no-op.

Example:
  tapes backfill previews \
    --postgres "postgres://user:pass@127.0.0.1:15432/tapes" \
    --batch 500 --pause 200ms`

func newPreviewsCmd() *cobra.Command {
	cmder := &previewsCommander{flags: previewsFlags}

	cmd := &cobra.Command{
		Use:   "previews",
		Short: "Fill stored span previews on rows derived before previews existed",
		Long:  previewsLongDesc,
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
	cmd.Flags().StringVar(&cmder.session, "session", "", "restrict the backfill to one session id (default: all)")
	cmd.Flags().IntVar(&cmder.batch, "batch", backfill.DefaultPreviewBatch, "spans per batch; each batch is one read and one write transaction")
	cmd.Flags().DurationVar(&cmder.pause, "pause", backfill.DefaultPreviewPause, "pause between batches")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "count the spans that would be backfilled without writing")

	return cmd
}

func (c *previewsCommander) run(cmd *cobra.Command) error {
	if c.postgresDSN == "" {
		return errors.New("backfill previews requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}
	if c.batch <= 0 {
		return errors.New("--batch must be positive")
	}

	ctx := cmd.Context()
	driver, err := postgres.NewDriver(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	result, err := backfill.Previews(ctx, backfill.PreviewOptions{
		Store:     driver,
		SessionID: c.session,
		BatchSize: c.batch,
		Pause:     c.pause,
		DryRun:    c.dryRun,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(cmd.ErrOrStderr(), format+"\n", args...)
		},
	})
	if result != nil {
		verb := "backfilled"
		if c.dryRun {
			verb = "would backfill"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s %d span(s) in %d batch(es)\n", verb, result.Backfilled, result.Batches)
	}
	return err
}
