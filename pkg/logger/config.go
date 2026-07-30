package logger

import (
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"golang.org/x/term"
)

// config holds the resolved logger configuration.
type config struct {
	level   slog.Leveler
	writers []io.Writer
	format  Format
	color   ColorMode
	source  bool
}

// buildHandler returns handlers configured independently for each writer so
// auto format and color decisions reflect the actual destination.
func (c *config) buildHandler() slog.Handler {
	handlers := make([]slog.Handler, len(c.writers))
	for index, writer := range c.writers {
		handlers[index] = c.buildHandlerForWriter(writer)
	}
	if len(handlers) == 1 {
		return handlers[0]
	}
	return &multiHandler{handlers: handlers}
}

func (c *config) buildHandlerForWriter(w io.Writer) slog.Handler {
	format := c.format
	if format == FormatAuto {
		if c.color == ColorAlways || isTerminal(w) {
			format = FormatConsole
		} else {
			format = FormatText
		}
	}

	switch format {
	case FormatAuto, FormatText:
		return slog.NewTextHandler(w, &slog.HandlerOptions{
			Level:     c.level,
			AddSource: c.source,
		})
	case FormatJSON:
		return slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level:     c.level,
			AddSource: c.source,
		})
	case FormatConsole:
		return tint.NewTextHandler(w, &tint.Options{
			Level:      c.level,
			AddSource:  c.source,
			TimeFormat: time.StampMilli,
			NoColor:    !ColorEnabled(w, c.color),
		})
	}

	return slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:     c.level,
		AddSource: c.source,
	})
}

// ColorEnabled reports whether color should be emitted to w under mode.
func ColorEnabled(w io.Writer, mode ColorMode) bool {
	switch mode {
	case ColorAlways:
		return true
	case ColorNever:
		return false
	case ColorAuto:
		if _, disabled := os.LookupEnv("NO_COLOR"); disabled {
			return false
		}
		return isTerminal(w)
	}

	return false
}

type fileDescriptorWriter interface {
	Fd() uintptr
}

func isTerminal(w io.Writer) bool {
	fdWriter, ok := w.(fileDescriptorWriter)
	return ok && term.IsTerminal(int(fdWriter.Fd()))
}
