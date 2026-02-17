package logger

import (
	"context"
	"log/slog"
)

// NewNoop returns a *slog.Logger that discards all output. Useful for tests.
func NewNoop() *slog.Logger {
	return slog.New(noop{})
}

// noop is an slog.Handler that silently drops all records.
type noop struct{}

func (noop) Enabled(_ context.Context, _ slog.Level) bool  { return false }
func (noop) Handle(_ context.Context, _ slog.Record) error { return nil }
func (n noop) WithAttrs(_ []slog.Attr) slog.Handler        { return n }
func (n noop) WithGroup(_ string) slog.Handler             { return n }
