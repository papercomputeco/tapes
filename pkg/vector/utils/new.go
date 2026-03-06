package vectorutils

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/chroma"
	"github.com/papercomputeco/tapes/pkg/vector/qdrant"
	"github.com/papercomputeco/tapes/pkg/vector/sqlitevec"
)

type NewVectorDriverOpts struct {
	ProviderType string
	Target       string
	Dimensions   uint
	Logger       *slog.Logger
}

func NewVectorDriver(o *NewVectorDriverOpts) (vector.Driver, error) {
	switch o.ProviderType {
	case "chroma":
		return newChromaDriver(o)
	case "sqlite":
		return newSqliteVecDriver(o)
	case "qdrant":
		return newQdrantDriver(o)
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

func newQdrantDriver(o *NewVectorDriverOpts) (vector.Driver, error) {
	if o.Target == "" {
		return nil, errors.New("qdrant target URL must be provided")
	}

	target := o.Target
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}

	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("failed to parse qdrant target URL: %w", err)
	}

	host := u.Hostname()
	port := 6334
	if u.Port() != "" {
		if p, err := strconv.Atoi(u.Port()); err == nil {
			port = p
		}
	}

	apiKey := ""
	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			apiKey = pass
		} else {
			apiKey = u.User.Username()
		}
	}

	useTLS := u.Scheme == "https"

	return qdrant.NewDriver(qdrant.Config{
		Host:       host,
		Port:       port,
		APIKey:     apiKey,
		UseTLS:     useTLS,
		Dimensions: uint64(o.Dimensions),
	}, o.Logger)
}
