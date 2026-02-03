package vectorutils

import (
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/chroma"
	"github.com/papercomputeco/tapes/pkg/vector/sqlitevec"
)

type NewVectorDriverOpts struct {
	ProviderType string
	Target       string
	Dimensions   uint
	Logger       *zap.Logger
}

func NewVectorDriver(o *NewVectorDriverOpts) (vector.Driver, error) {
	switch o.ProviderType {
	case "chroma":
		return newChromaDriver(o)
	case "sqlite":
		return newSqliteVecDriver(o)
	default:
		return nil, fmt.Errorf("unsupported vector store provider: %s", o.ProviderType)
	}
}

func newChromaDriver(o *NewVectorDriverOpts) (vector.Driver, error) {
	if o.Target == "" {
		return nil, errors.New("chroma target URL must be provided")
	}

	return chroma.NewDriver(chroma.Config{
		URL: o.Target,
	}, o.Logger)
}

func newSqliteVecDriver(o *NewVectorDriverOpts) (vector.Driver, error) {
	return sqlitevec.NewDriver(sqlitevec.Config{
		DBPath:     o.Target,
		Dimensions: o.Dimensions,
	}, o.Logger)
}
