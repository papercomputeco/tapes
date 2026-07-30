package logger

import (
	"io"
	"log/slog"
)

// Option configures a Logger created with New.
type Option func(*config)

// WithDebug sets the log level to Debug when true, Info otherwise.
func WithDebug(debug bool) Option {
	return func(c *config) {
		c.debug = debug
		if debug {
			c.level = slog.LevelDebug
		} else {
			c.level = slog.LevelInfo
		}
	}
}

// WithPretty is retained for source compatibility. Output formatting is now
// selected by WithDebug: debug logs use tint and normal logs use slog text.
//
// Deprecated: use WithDebug to enable colorized debug output.
func WithPretty(_ bool) Option {
	return func(_ *config) {}
}

// WithJSON enables slog's JSON handler for structured service logs.
func WithJSON(json bool) Option {
	return func(c *config) {
		c.json = json
	}
}

// WithWriter overrides the output writer. Defaults to os.Stdout.
func WithWriter(w io.Writer) Option {
	return func(c *config) {
		c.writers = []io.Writer{w}
	}
}

// WithWriters sets multiple output writers (combined via io.MultiWriter).
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
