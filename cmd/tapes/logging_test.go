package tapescmder

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
)

var _ = Describe("update notice", func() {
	It("omits ANSI when color is disabled", func() {
		var output bytes.Buffer
		printUpdateNotice(&output, "v1.2.3", false)

		Expect(output.String()).To(And(
			ContainSubstring("v1.2.3"),
			Not(ContainSubstring("\x1b[")),
		))
	})

	It("includes ANSI when color is enabled", func() {
		var output bytes.Buffer
		printUpdateNotice(&output, "v1.2.3", true)

		Expect(output.String()).To(ContainSubstring("\x1b["))
	})
})

var _ = Describe("logging flags", func() {
	It("registers the logging controls with defaults", func() {
		cmd := NewTapesCmd()

		Expect(cmd.PersistentFlags().Lookup("log-level").DefValue).To(Equal("info"))
		Expect(cmd.PersistentFlags().Lookup("log-format").DefValue).To(Equal("auto"))
		Expect(cmd.PersistentFlags().Lookup("log-color").DefValue).To(Equal("auto"))
		Expect(cmd.PersistentFlags().Lookup("debug").Deprecated).NotTo(BeEmpty())
	})

	It("maps the deprecated debug shorthand to debug level", func() {
		cmd := NewTapesCmd()
		Expect(cmd.PersistentFlags().Set("debug", "true")).To(Succeed())

		settings, err := resolveLoggingSettings(cmd, "info", "auto", "auto")

		Expect(err).NotTo(HaveOccurred())
		Expect(settings.Level).To(Equal(slog.LevelDebug))
	})

	It("prefers an explicit log level over the debug shorthand", func() {
		cmd := NewTapesCmd()
		Expect(cmd.PersistentFlags().Set("debug", "true")).To(Succeed())
		Expect(cmd.PersistentFlags().Set("log-level", "error")).To(Succeed())

		settings, err := resolveLoggingSettings(cmd, "error", "json", "never")

		Expect(err).NotTo(HaveOccurred())
		Expect(settings).To(Equal(logger.Settings{
			Level:  slog.LevelError,
			Format: logger.FormatJSON,
			Color:  logger.ColorNever,
		}))
	})

	It("rejects invalid flag values", func() {
		cmd := NewTapesCmd()

		_, err := resolveLoggingSettings(cmd, "trace", "auto", "auto")
		Expect(err).To(MatchError(ContainSubstring("invalid log level")))
	})

	It("lets a valid CLI value override an invalid config value", func() {
		originalLogger := slog.Default()
		DeferCleanup(slog.SetDefault, originalLogger)

		configDir := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(`[logging]
level = "verbose"
`), 0o600)).To(Succeed())

		cmd := NewTapesCmd()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{
			"--config-dir", configDir,
			"--disable-telemetry",
			"--disable-update-check",
			"--log-level", "error",
			"version",
		})

		Expect(cmd.Execute()).To(Succeed())
	})
})
