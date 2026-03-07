package apicmder

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/factory"
)

func (c *apiCommander) newStorageDriver() (storage.Driver, error) {
	return factory.NewDriver(context.Background(), factory.Params{
		PostgresDSN:       c.postgresDSN,
		SQLitePath:        c.sqlitePath,
		TursoDSN:          c.tursoDSN,
		TursoAuthToken:    c.tursoAuthToken,
		TursoLocalPath:    c.tursoLocalPath,
		TursoSyncInterval: c.tursoSyncInterval,
		Logger:            c.logger,
	})
}
