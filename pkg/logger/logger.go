// Package logger provides opinionated logging capabilities for the tapes system.
//
// It builds on Go's log/slog, with terminal-aware console output and structured
// JSON for services. All public constructors return *slog.Logger directly — no
// custom interface.
package logger

import (
	"io"
	"log/slog"
	"os"
)

// New creates a *slog.Logger configured by the given options.
//
// Defaults: Info level, auto format and color, writes to os.Stderr.
func New(opts ...Option) *slog.Logger {
	cfg := &config{
		level:   slog.LevelInfo,
		writers: []io.Writer{os.Stderr},
		format:  FormatAuto,
		color:   ColorAuto,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	handler := cfg.buildHandler()
	return slog.New(handler)
}
