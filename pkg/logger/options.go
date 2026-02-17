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
		if debug {
			c.level = slog.LevelDebug
		} else {
			c.level = slog.LevelInfo
		}
	}
}

// WithPretty enables the charmbracelet/log handler for colorized,
// human-friendly CLI output.
func WithPretty(pretty bool) Option {
	return func(c *config) {
		c.pretty = pretty
	}
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
