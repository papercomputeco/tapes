//go:build !darwin

package menucmder

import "log/slog"

// Spawn is a no-op on non-darwin platforms.
func Spawn(_ string, _ bool, _ *slog.Logger) {}
