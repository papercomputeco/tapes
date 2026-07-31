package servecmder

import (
	"context"

	"github.com/papercomputeco/tapes/api"
)

func (commander *ServeCommander) configureCassettes() {
	if len(commander.stack.CassetteSources) == 0 {
		return
	}

	commander.stack.Logger.Info("configured cassette OpenAPI sources",
		"count", len(commander.stack.CassetteSources),
	)
	commander.stack.Started = func(ctx context.Context, server *api.Server) {
		server.StartCassetteSpecRefresh(ctx, commander.refresh)
	}
}
