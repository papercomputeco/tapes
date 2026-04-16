//go:build !darwin

package menucmder

import (
	"log/slog"

	"github.com/papercomputeco/tapes/pkg/start"
)

// Spawn is a no-op on non-darwin platforms.
func Spawn(_ *start.Manager, _ string, _ bool, _ *slog.Logger) {}
