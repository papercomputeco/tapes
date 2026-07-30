package logger

import (
	"io"
	"log/slog"
)

// Option configures a Logger created with New.
type Option func(*config)

// WithLevel sets the minimum enabled log level.
func WithLevel(level slog.Leveler) Option {
	return func(c *config) {
		c.level = level
	}
}

// WithFormat sets the log record format.
func WithFormat(format Format) Option {
	return func(c *config) {
		c.format = format
	}
}

// WithColor sets the console color policy.
func WithColor(color ColorMode) Option {
	return func(c *config) {
		c.color = color
	}
}

// WithDebug sets the log level to Debug when true, Info otherwise.
//
// Deprecated: use WithLevel.
func WithDebug(debug bool) Option {
	if debug {
		return WithLevel(slog.LevelDebug)
	}
	return WithLevel(slog.LevelInfo)
}

// WithPretty selects tint console output when true and native text when false.
//
// Deprecated: use WithFormat.
func WithPretty(pretty bool) Option {
	if pretty {
		return WithFormat(FormatConsole)
	}
	return WithFormat(FormatText)
}

// WithJSON selects JSON output when true and native text when false.
//
// Deprecated: use WithFormat.
func WithJSON(json bool) Option {
	if json {
		return WithFormat(FormatJSON)
	}
	return WithFormat(FormatText)
}

// WithWriter overrides the output writer. Defaults to os.Stderr.
func WithWriter(w io.Writer) Option {
	return func(c *config) {
		c.writers = []io.Writer{w}
	}
}

// WithWriters sets multiple output writers with per-destination handlers.
func WithWriters(w ...io.Writer) Option {
	return func(c *config) {
		c.writers = w
	}
}

// WithSource includes source file:line in log output.
func WithSource(source bool) Option {
	return func(c *config) {
		c.source = source
	}
}
