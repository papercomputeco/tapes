package servecmder

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
)

func (commander *ServeCommander) configureCassettes(cmd *cobra.Command) {
	if len(commander.stack.CassetteSources) == 0 {
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "configured %d cassette OpenAPI source(s)\n", len(commander.stack.CassetteSources))
	commander.stack.Started = func(ctx context.Context, server *api.Server) {
		server.StartCassetteSpecRefresh(ctx, commander.refresh)
	}
}
