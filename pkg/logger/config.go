package logger

import (
	"io"
	"log/slog"

	charmlog "github.com/charmbracelet/log"
)

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
