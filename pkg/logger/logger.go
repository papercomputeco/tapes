// Package logger provides opinionated logging capabilities for the tapes system.
//
// It builds on Go's log/slog, with pluggable handlers for pretty CLI output
// (via charmbracelet/log) and structured JSON for services. All public
// constructors return *slog.Logger directly — no custom interface.
package logger

import (
	"io"
	"log/slog"
	"os"
)

// New creates a *slog.Logger configured by the given options.
//
// Defaults: Info level, writes to os.Stdout, slog.TextHandler.
func New(opts ...Option) *slog.Logger {
	cfg := &config{
		level:   slog.LevelInfo,
		writers: []io.Writer{os.Stdout},
	}

	for _, opt := range opts {
		opt(cfg)
	}

	handler := cfg.buildHandler()
	return slog.New(handler)
}
