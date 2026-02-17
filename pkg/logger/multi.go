package logger

import (
	"context"
	"log/slog"
)

// multiHandler fans out log records to multiple slog.Handler instances.
// Used by the start command to write pretty output to stdout and JSON to a
// log file simultaneously.
type multiHandler struct {
	handlers []slog.Handler
}

// Multi creates a *slog.Logger that dispatches every record to all provided
// loggers' underlying handlers. Each logger's handler is extracted via
// (*slog.Logger).Handler().
func Multi(loggers ...*slog.Logger) *slog.Logger {
	handlers := make([]slog.Handler, len(loggers))
	for i, l := range loggers {
		handlers[i] = l.Handler()
	}
	return slog.New(&multiHandler{handlers: handlers})
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	children := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		children[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: children}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	children := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		children[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: children}
}
