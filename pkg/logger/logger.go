// Package logger provides opinionated logging capabilities for the tapes system
package logger

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(debug bool) *zap.Logger {
	return NewLoggerWithWriters(debug, os.Stdout)
}

func NewLoggerWithWriters(debug bool, writers ...io.Writer) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	// Set log level
	level := zap.InfoLevel
	if debug {
		level = zap.DebugLevel
	}

	if len(writers) == 0 {
		writers = []io.Writer{os.Stdout}
	}

	syncers := make([]zapcore.WriteSyncer, 0, len(writers))
	for _, writer := range writers {
		syncers = append(syncers, zapcore.AddSync(writer))
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(syncers...),
		level,
	)

	return zap.New(core, zap.AddCaller())
}
