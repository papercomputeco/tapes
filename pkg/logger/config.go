package logger

import (
	"io"
	"log/slog"
	"time"

	"github.com/lmittmann/tint"
)

// config holds the resolved logger configuration.
type config struct {
	level   slog.Level
	writers []io.Writer
	debug   bool
	json    bool
	source  bool
}

// buildHandler returns the appropriate slog.Handler for the configuration.
func (c *config) buildHandler() slog.Handler {
	w := c.writer()

	if c.json {
		return slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level:     c.level,
			AddSource: c.source,
		})
	}

	if c.debug {
		return tint.NewTextHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			AddSource:  c.source,
			TimeFormat: time.StampMilli,
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
