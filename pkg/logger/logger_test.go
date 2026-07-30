package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
)

var _ = Describe("Logger", func() {
	Describe("New", func() {
		It("creates a default text logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf))
			l.Info("hello", "key", "value")

			output := buf.String()
			Expect(output).To(ContainSubstring("hello"))
			Expect(output).To(ContainSubstring("key"))
			Expect(output).To(ContainSubstring("value"))
		})

		It("uses tint at debug level", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithDebug(true))
			l.Debug("debug msg")

			Expect(buf.String()).To(And(
				ContainSubstring("debug msg"),
				ContainSubstring("\x1b["),
				MatchRegexp(`\d{1,2}:\d{2}(AM|PM)`),
			))
		})

		It("filters debug when not enabled", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithDebug(false))
			l.Debug("hidden")

			Expect(buf.String()).To(BeEmpty())
		})

		It("creates a JSON logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true))
			l.Info("structured", "count", 42)

			var parsed map[string]any
			err := json.Unmarshal(buf.Bytes(), &parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed["msg"]).To(Equal("structured"))
			Expect(parsed["count"]).To(BeNumerically("==", 42))
		})

		It("uses native text output when debug is disabled", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf))
			l.Info("text output")

			Expect(buf.String()).To(And(
				ContainSubstring("level=INFO"),
				ContainSubstring("msg=\"text output\""),
				Not(ContainSubstring("\x1b[")),
			))
		})

		It("keeps JSON output in debug mode", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true), logger.WithDebug(true))
			l.Debug("structured debug")

			var parsed map[string]any
			err := json.Unmarshal(buf.Bytes(), &parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed["level"]).To(Equal("DEBUG"))
			Expect(parsed["msg"]).To(Equal("structured debug"))
		})

		It("includes source in debug output", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithDebug(true), logger.WithSource(true))
			l.Debug("with source")

			Expect(buf.String()).To(ContainSubstring("logger_test.go"))
		})

		It("supports multiple writers", func() {
			var buf1, buf2 bytes.Buffer
			l := logger.New(logger.WithWriters(&buf1, &buf2))
			l.Info("multi")

			Expect(buf1.String()).To(ContainSubstring("multi"))
			Expect(buf2.String()).To(ContainSubstring("multi"))
		})

		It("returns *slog.Logger", func() {
			l := logger.New()
			// Verify it's a real *slog.Logger by calling Handler()
			Expect(l.Handler()).NotTo(BeNil())
		})
	})

	Describe("Nop", func() {
		It("does not panic on any method", func() {
			l := logger.NewNoop()
			Expect(func() {
				l.Debug("msg")
				l.Info("msg")
				l.Warn("msg")
				l.Error("msg")
				l.With("key", "value").Info("msg")
				l.WithGroup("group").Info("msg")
			}).NotTo(Panic())
		})

		It("returns *slog.Logger", func() {
			l := logger.NewNoop()
			Expect(l.Handler()).NotTo(BeNil())
		})

		It("discards all output", func() {
			l := logger.NewNoop()
			// Nop handler should report Enabled=false for all levels
			Expect(l.Handler().Enabled(context.Background(), slog.LevelInfo)).To(BeFalse())
		})
	})

	Describe("Multi", func() {
		It("dispatches to all loggers", func() {
			var buf1, buf2 bytes.Buffer
			l1 := logger.New(logger.WithWriter(&buf1))
			l2 := logger.New(logger.WithWriter(&buf2))
			multi := logger.Multi(l1, l2)

			multi.Info("broadcast", "key", "val")

			Expect(buf1.String()).To(ContainSubstring("broadcast"))
			Expect(buf2.String()).To(ContainSubstring("broadcast"))
		})

		It("supports With on multi logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true))
			multi := logger.Multi(l)

			child := multi.With("component", "test")
			child.Info("hello")

			lines := strings.TrimSpace(buf.String())
			var parsed map[string]any
			err := json.Unmarshal([]byte(lines), &parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed["component"]).To(Equal("test"))
		})

		It("supports WithGroup on multi logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true))
			multi := logger.Multi(l)

			child := multi.WithGroup("request")
			child.Info("processed", "method", "GET")

			lines := strings.TrimSpace(buf.String())
			var parsed map[string]any
			err := json.Unmarshal([]byte(lines), &parsed)
			Expect(err).NotTo(HaveOccurred())

			group, ok := parsed["request"].(map[string]any)
			Expect(ok).To(BeTrue(), "expected 'request' group in JSON output")
			Expect(group["method"]).To(Equal("GET"))
		})

		It("returns *slog.Logger", func() {
			multi := logger.Multi(logger.NewNoop())
			Expect(multi.Handler()).NotTo(BeNil())
		})
	})

	Describe("With", func() {
		It("binds fields to child logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true))
			child := l.With("service", "proxy")
			child.Info("started")

			lines := strings.TrimSpace(buf.String())
			var parsed map[string]any
			err := json.Unmarshal([]byte(lines), &parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(parsed["service"]).To(Equal("proxy"))
			Expect(parsed["msg"]).To(Equal("started"))
		})
	})

	Describe("WithGroup", func() {
		It("nests keys under group", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithJSON(true))
			child := l.WithGroup("request")
			child.Info("processed", "method", "GET")

			lines := strings.TrimSpace(buf.String())
			var parsed map[string]any
			err := json.Unmarshal([]byte(lines), &parsed)
			Expect(err).NotTo(HaveOccurred())

			// slog groups nest attributes under the group name
			group, ok := parsed["request"].(map[string]any)
			Expect(ok).To(BeTrue(), "expected 'request' group in JSON output")
			Expect(group["method"]).To(Equal("GET"))
		})
	})
})
