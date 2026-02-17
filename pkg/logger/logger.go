// Package logger provides opinionated logging capabilities for the tapes system.
//
// It builds on Go's log/slog, with pluggable handlers for pretty CLI output
// (via charmbracelet/log) and structured JSON for services. All public
// constructors return *slog.Logger directly — no custom interface.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"

	charmlog "github.com/charmbracelet/log"
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

// Nop returns a *slog.Logger that discards all output. Useful for tests.
func Nop() *slog.Logger {
	return slog.New(discardHandler{})
}

// discardHandler is an slog.Handler that silently drops all records.
type discardHandler struct{}

func (discardHandler) Enabled(_ context.Context, _ slog.Level) bool  { return false }
func (discardHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (d discardHandler) WithAttrs(_ []slog.Attr) slog.Handler        { return d }
func (d discardHandler) WithGroup(_ string) slog.Handler             { return d }

// config holds the resolved logger configuration.
type config struct {
	level   slog.Level
	writers []io.Writer
	pretty  bool
	json    bool
	source  bool
}

// buildHandler returns the appropriate slog.Handler for the configuration.
func (c *config) buildHandler() slog.Handler {
	w := c.writer()

	if c.pretty {
		return c.newCharmHandler(w)
	}

	if c.json {
		return slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level:     c.level,
			AddSource: c.source,
		})
	}

	return slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:     c.level,
		AddSource: c.source,
	})
}

// writer returns the resolved io.Writer. If multiple writers were configured,
// they are combined via io.MultiWriter.
func (c *config) writer() io.Writer {
	if len(c.writers) == 1 {
		return c.writers[0]
	}
	return io.MultiWriter(c.writers...)
}

// newCharmHandler creates a charmbracelet/log handler configured as an slog.Handler.
func (c *config) newCharmHandler(w io.Writer) slog.Handler {
	var charmLevel charmlog.Level
	switch {
	case c.level <= slog.LevelDebug:
		charmLevel = charmlog.DebugLevel
	case c.level <= slog.LevelInfo:
		charmLevel = charmlog.InfoLevel
	case c.level <= slog.LevelWarn:
		charmLevel = charmlog.WarnLevel
	default:
		charmLevel = charmlog.ErrorLevel
	}

	l := charmlog.NewWithOptions(w, charmlog.Options{
		Level:           charmLevel,
		ReportTimestamp: true,
		ReportCaller:    c.source,
	})

	// *charmlog.Logger implements slog.Handler directly.
	return l
}
