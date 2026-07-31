package logger

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// Format controls how log records are rendered.
type Format string

const (
	FormatAuto    Format = "auto"
	FormatConsole Format = "console"
	FormatText    Format = "text"
	FormatJSON    Format = "json"
)

// ColorMode controls whether console logs contain ANSI color sequences.
type ColorMode string

const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

// Settings are the independently resolved logging concerns shared by a command.
type Settings struct {
	Level  slog.Level
	Format Format
	Color  ColorMode
}

// DefaultSettings returns the default logging settings.
func DefaultSettings() Settings {
	return Settings{
		Level:  slog.LevelInfo,
		Format: FormatAuto,
		Color:  ColorAuto,
	}
}

// ParseSettings validates and resolves user-facing logging values.
func ParseSettings(level, format, color string) (Settings, error) {
	parsedLevel, err := ParseLevel(level)
	if err != nil {
		return Settings{}, err
	}
	parsedFormat, err := ParseFormat(format)
	if err != nil {
		return Settings{}, err
	}
	parsedColor, err := ParseColorMode(color)
	if err != nil {
		return Settings{}, err
	}

	return Settings{Level: parsedLevel, Format: parsedFormat, Color: parsedColor}, nil
}

// ParseLevel parses one of the supported minimum log levels.
func ParseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q (expected debug, info, warn, or error)", value)
	}
}

// ParseFormat parses a supported log output format.
func ParseFormat(value string) (Format, error) {
	switch Format(strings.ToLower(strings.TrimSpace(value))) {
	case FormatAuto:
		return FormatAuto, nil
	case FormatConsole:
		return FormatConsole, nil
	case FormatText:
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("invalid log format %q (expected auto, console, text, or json)", value)
	}
}

// ParseColorMode parses a supported console color policy.
func ParseColorMode(value string) (ColorMode, error) {
	switch ColorMode(strings.ToLower(strings.TrimSpace(value))) {
	case ColorAuto:
		return ColorAuto, nil
	case ColorAlways:
		return ColorAlways, nil
	case ColorNever:
		return ColorNever, nil
	default:
		return "", fmt.Errorf("invalid log color %q (expected auto, always, or never)", value)
	}
}

// New creates a logger from these settings plus any sink-specific options.
func (s Settings) New(opts ...Option) *slog.Logger {
	base := []Option{
		WithLevel(s.Level),
		WithFormat(s.Format),
		WithColor(s.Color),
	}
	return New(append(base, opts...)...)
}

// WithDefaultFormat replaces auto with a sink-specific default format.
func (s Settings) WithDefaultFormat(format Format) Settings {
	if s.Format == FormatAuto {
		s.Format = format
	}
	return s
}

type settingsContextKey struct{}

// WithSettings stores resolved logging settings in a context.
func WithSettings(ctx context.Context, settings Settings) context.Context {
	return context.WithValue(ctx, settingsContextKey{}, settings)
}

// SettingsFromContext returns resolved settings or the package defaults.
func SettingsFromContext(ctx context.Context) Settings {
	if ctx != nil {
		if settings, ok := ctx.Value(settingsContextKey{}).(Settings); ok {
			return settings
		}
	}
	return DefaultSettings()
}

// FromContext creates a logger using settings stored in ctx.
func FromContext(ctx context.Context, opts ...Option) *slog.Logger {
	return SettingsFromContext(ctx).New(opts...)
}
