package seedcmder

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/deck"
)

const seedLongDesc string = `Seed demo data into a SQLite database.

Examples:
  tapes seed
  tapes seed --demo
  tapes seed --sqlite ./tapes.db
  tapes seed --overwrite`

const seedShortDesc string = "Seed demo sessions"

type seedCommander struct {
	sqlitePath string
	demo       bool
	overwrite  bool
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

	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "m", false, "Seed demo data")
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "f", false, "Overwrite database before seeding")

	return cmd
}

func (c *seedCommander) run(ctx context.Context) error {
	sqlitePath := c.resolveSQLitePath()

	var sessionCount, messageCount int
	if err := cliui.Step(os.Stdout, "Seeding demo data", func() error {
		var seedErr error
		sessionCount, messageCount, seedErr = deck.SeedDemo(ctx, sqlitePath, c.overwrite)
		return seedErr
	}); err != nil {
		return err
	}

	fmt.Printf("\n  %s Seeded %s sessions %s into %s\n\n",
		cliui.SuccessMark,
		cliui.NameStyle.Render(strconv.Itoa(sessionCount)),
		cliui.DimStyle.Render(fmt.Sprintf("(%d messages)", messageCount)),
		cliui.DimStyle.Render(sqlitePath),
	)
	return nil
}

func (c *seedCommander) resolveSQLitePath() string {
	if strings.TrimSpace(c.sqlitePath) != "" {
		return c.sqlitePath
	}

	if c.demo {
		return deck.DemoSQLitePath
	}

	path, err := sqlitepath.ResolveSQLitePath("")
	if err == nil {
		return path
	}

	return "tapes.db"
}
