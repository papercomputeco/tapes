package config

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/logger"
)

func validateLoggingConfig(logging LoggingConfig) error {
	defaults := NewDefaultConfig().Logging
	if logging.Level == "" {
		logging.Level = defaults.Level
	}
	if logging.Format == "" {
		logging.Format = defaults.Format
	}
	if logging.Color == "" {
		logging.Color = defaults.Color
	}

	if _, err := logger.ParseSettings(logging.Level, logging.Format, logging.Color); err != nil {
		return fmt.Errorf("invalid logging config: %w", err)
	}
	return nil
}
