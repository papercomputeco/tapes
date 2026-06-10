// Package backfillcmder exposes offline backfills into a running tapes
// deployment. Today that is the paperd wire-trace replay, which fills
// the immutable raw-turn layer (and any not-yet-captured nodes) for
// sessions recorded before the raw layer existed.
package backfillcmder

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/backfill"
)

const backfillLongDesc string = `Backfill captured data into a running tapes deployment.

Subcommands replay existing capture artifacts through the normal ingest
path, so every write is idempotent: raw turns dedup on their capture id
and nodes dedup by content hash.`

const wireTraceLongDesc string = `Replay paperd wire-trace capture bundles through tapes-ingest.

Reads turn-* bundles (request.json + response.sse + meta.json) from a
paperd wire-trace directory, reconstructs the ingest envelope that
tapes-extproc would have dispatched live — verbatim request bytes,
response reduced with the shared capture reducer, session block from
the captured X-Tapes-* headers — and POSTs each to {ingest-url}/v1/ingest.

Idempotent: re-running is a no-op for turns already backfilled, and
turns that already landed via live capture only gain their raw row.

Example (against a clearing):
  kubectl port-forward -n tenant-develop svc/local-gw-ingest 18890:8090 &
  tapes backfill wire-trace \
    --dir "$GROVE_ROOT/.grove/clearings/develop/wire-trace" \
    --ingest-url http://127.0.0.1:18890`

// NewBackfillCmd creates the `tapes backfill` command tree.
func NewBackfillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Backfill captured data into tapes",
		Long:  backfillLongDesc,
	}
	cmd.AddCommand(newWireTraceCmd())
	return cmd
}

type wireTraceCommander struct {
	dir       string
	ingestURL string
	dryRun    bool
	verbose   bool
}

func newWireTraceCmd() *cobra.Command {
	cmder := &wireTraceCommander{}

	cmd := &cobra.Command{
		Use:   "wire-trace",
		Short: "Replay paperd wire-trace bundles through ingest",
		Long:  wireTraceLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts := backfill.WireTraceOptions{
				CapturesDir: cmder.dir,
				IngestURL:   cmder.ingestURL,
				DryRun:      cmder.dryRun,
				Verbose:     cmder.verbose,
				Logf: func(format string, args ...any) {
					fmt.Fprintf(cmd.ErrOrStderr(), format+"\n", args...)
				},
			}
			result, err := backfill.BackfillWireTrace(cmd.Context(), opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"scanned %d: posted %d, raw-only %d, skipped %d, failed %d\n",
				result.Scanned, result.Posted, result.RawOnly, result.Skipped, result.Failed)
			for _, f := range result.Failures {
				fmt.Fprintf(cmd.OutOrStdout(), "  failure: %s\n", f)
			}
			if result.Failed > 0 {
				return fmt.Errorf("%d turn(s) failed to backfill", result.Failed)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&cmder.dir, "dir", "", "paperd wire-trace directory holding turn-* bundles (required)")
	cmd.Flags().StringVar(&cmder.ingestURL, "ingest-url", "http://127.0.0.1:8090", "base URL of the tapes-ingest server")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "parse and reduce every bundle but skip the POST")
	cmd.Flags().BoolVarP(&cmder.verbose, "verbose", "v", false, "log each turn's outcome")
	_ = cmd.MarkFlagRequired("dir")

	return cmd
}
