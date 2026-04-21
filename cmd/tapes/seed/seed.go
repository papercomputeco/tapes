package seedcmder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/deck"
)

const seedLongDesc string = `Seed demo data through the tapes API.

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
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "f", false, "Deprecated: use a fresh PostgreSQL database instead")

	return cmd
}

func (c *seedCommander) run(ctx context.Context) error {
	apiTarget, location, err := c.resolveAPITarget()
	if err != nil {
		return err
	}

	var sessionCount, messageCount int
	if err := cliui.Step(os.Stdout, "Seeding demo data", func() error {
		var seedErr error
		sessionCount, messageCount, seedErr = deck.SeedDemoViaAPI(ctx, apiTarget, false)
		return seedErr
	}); err != nil {
		return err
	}

	fmt.Printf("\n  %s Seeded %s sessions %s into %s\n\n",
		cliui.SuccessMark,
		cliui.NameStyle.Render(strconv.Itoa(sessionCount)),
		cliui.DimStyle.Render(fmt.Sprintf("(%d messages)", messageCount)),
		cliui.DimStyle.Render(location),
	)
	return nil
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
