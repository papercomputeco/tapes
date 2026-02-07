package seedcmder

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
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
			return cmder.run(cmd.Context(), cmd)
		},
	}

	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "D", false, "Seed demo data")
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "O", false, "Overwrite database before seeding")

	return cmd
}

func (c *seedCommander) run(ctx context.Context, cmd *cobra.Command) error {
	sqlitePath := c.resolveSQLitePath()
	sessionCount, messageCount, err := deck.SeedDemo(ctx, sqlitePath, c.overwrite)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Seeded %d demo sessions (%d messages) into %s\n", sessionCount, messageCount, sqlitePath)
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
