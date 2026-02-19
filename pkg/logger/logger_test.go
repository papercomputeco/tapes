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

		It("respects debug level", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithDebug(true))
			l.Debug("debug msg")

			Expect(buf.String()).To(ContainSubstring("debug msg"))
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

		It("creates a pretty logger", func() {
			var buf bytes.Buffer
			l := logger.New(logger.WithWriter(&buf), logger.WithPretty(true))
			l.Info("pretty output")

			Expect(buf.String()).To(ContainSubstring("pretty output"))
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
			l := logger.Nop()
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
			l := logger.Nop()
			Expect(l.Handler()).NotTo(BeNil())
		})

		It("discards all output", func() {
			l := logger.Nop()
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
			multi := logger.Multi(logger.Nop())
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
