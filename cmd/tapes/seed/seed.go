package seedcmder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/seed"
)

const seedLongDesc string = `Seed demo data through the tapes API.

Seeding replays bundled capture corpora through the normal ingest
write path (raw turns + sessions) and derives the seeded sessions, so
demo data is indistinguishable from live capture. Re-running is a
no-op: the raw layer deduplicates replayed turns.

Examples:
  tapes seed
  tapes seed --api-target http://localhost:8081
  tapes seed --demo`

const seedShortDesc string = "Seed demo sessions"

type seedCommander struct {
	apiTarget string
	demo      bool
	overwrite bool
}

func NewSeedCmd() *cobra.Command {
	cmder := &seedCommander{}

	cmd := &cobra.Command{
		Use:   "seed",
		Short: seedShortDesc,
		Long:  seedLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd.Context())
		},
	}

	cmd.Flags().StringVarP(&cmder.apiTarget, "api-target", "a", "http://localhost:8081", "URL of a running tapes API server")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "m", false, "Seed demo data")
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "f", false, "Deprecated: seeding is idempotent, overwrite is unnecessary")

	return cmd
}

func (c *seedCommander) run(ctx context.Context) error {
	apiTarget, location, err := c.resolveAPITarget()
	if err != nil {
		return err
	}

	var report *seed.Result
	if err := cliui.Step(os.Stdout, "Seeding demo data", func() error {
		var seedErr error
		report, seedErr = seedDemoViaAPI(ctx, apiTarget)
		return seedErr
	}); err != nil {
		return err
	}

	detail := fmt.Sprintf("(%d raw turns: %d inserted, %d deduped)",
		report.RawTurns, report.RawTurnsInserted, report.RawTurnsDeduped)
	fmt.Printf("\n  %s Seeded %s sessions %s into %s\n\n",
		cliui.SuccessMark,
		cliui.NameStyle.Render(strconv.Itoa(report.Sessions)),
		cliui.DimStyle.Render(detail),
		cliui.DimStyle.Render(location),
	)
	return nil
}

// seedDemoViaAPI invokes POST /v1/admin/seed/demo on a running tapes
// API server and returns the seeding report.
func seedDemoViaAPI(ctx context.Context, apiTarget string) (*seed.Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(apiTarget, "/")+"/v1/admin/seed/demo", bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, fmt.Errorf("create seed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("seed demo via api: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read seed response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("seed api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var report seed.Result
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("decode seed response: %w", err)
	}
	return &report, nil
}

func (c *seedCommander) resolveAPITarget() (string, string, error) {
	if c.overwrite {
		return "", "", errors.New("--overwrite is no longer supported")
	}

	target := normalizeAPITarget(c.apiTarget)
	return target, target, nil
}

func normalizeAPITarget(target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return "http://localhost:8081"
	}
	if !strings.Contains(target, "://") {
		return "http://" + target
	}
	return target
}
