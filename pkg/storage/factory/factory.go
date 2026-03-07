//go:build !turso

// Package factory provides a shared storage driver constructor
// used by all command packages to avoid duplicating driver selection logic.
package factory

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// Params holds the configuration needed to create a storage driver.
type Params struct {
	PostgresDSN       string
	SQLitePath        string
	TursoDSN          string
	TursoAuthToken    string
	TursoLocalPath    string
	TursoSyncInterval string
	Logger            *slog.Logger
}

// NewDriver creates a storage driver based on the provided configuration.
// It checks backends in priority order: PostgreSQL, SQLite, then in-memory fallback.
func NewDriver(ctx context.Context, p Params) (storage.Driver, error) {
	if p.PostgresDSN != "" {
		driver, err := postgres.NewDriver(ctx, p.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL storer: %w", err)
		}
		if p.Logger != nil {
			p.Logger.Info("using PostgreSQL storage")
		}
		return driver, nil
	}

	if p.SQLitePath != "" {
		driver, err := sqlite.NewDriver(ctx, p.SQLitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		if p.Logger != nil {
			p.Logger.Info("using SQLite storage", "path", p.SQLitePath)
		}
		return driver, nil
	}

	if p.Logger != nil {
		p.Logger.Info("using in-memory storage")
	}
	return inmemory.NewDriver(), nil
}
