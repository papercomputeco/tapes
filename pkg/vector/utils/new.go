package vectorutils

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/chroma"
)

type NewVectorDriverOpts struct {
	ProviderType string
	TargetURL    string
	Logger       *zap.Logger
}

func NewVectorDriver(o *NewVectorDriverOpts) (vector.VectorDriver, error) {
	switch o.ProviderType {
	case "chroma":
		return chroma.NewChromaDriver(chroma.Config{
			URL: o.TargetURL,
		}, o.Logger)
	default:
		return nil, fmt.Errorf("unsupported vector store provider: %s", o.ProviderType)
	}
}
