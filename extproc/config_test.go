package extproc

import (
	"bytes"
	"log/slog"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// setenv sets an env var for the current spec and leaves it unset afterwards;
// ConfigFromEnv reads the process environment directly.
func setenv(key, value string) {
	GinkgoHelper()
	Expect(os.Setenv(key, value)).To(Succeed())
	DeferCleanup(os.Unsetenv, key)
}

var _ = Describe("ConfigFromEnv", func() {
	BeforeEach(func() {
		// Defaults are only observable from an unset environment.
		Expect(os.Unsetenv("TAPES_GRPC_MAX_RECV_BYTES")).To(Succeed())
		Expect(os.Unsetenv("TAPES_MAX_INFLIGHT_DISPATCHES")).To(Succeed())
	})

	It("reads TAPES_GRPC_MAX_RECV_BYTES and TAPES_MAX_INFLIGHT_DISPATCHES with 64 MiB / 100 defaults", func() {
		cfg := ConfigFromEnv()
		Expect(cfg.GRPCMaxRecvBytes).To(Equal(64 << 20))
		Expect(cfg.MaxInflight).To(Equal(100))

		setenv("TAPES_GRPC_MAX_RECV_BYTES", "16777216")
		setenv("TAPES_MAX_INFLIGHT_DISPATCHES", "7")
		cfg = ConfigFromEnv()
		Expect(cfg.GRPCMaxRecvBytes).To(Equal(16 << 20))
		Expect(cfg.MaxInflight).To(Equal(7))
	})

	It("falls back to defaults with a logged error on unparseable or non-positive size values", func() {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(previous)

		for _, bad := range []string{"banana", "-1", "0"} {
			setenv("TAPES_GRPC_MAX_RECV_BYTES", bad)
			setenv("TAPES_MAX_INFLIGHT_DISPATCHES", bad)
			cfg := ConfigFromEnv()
			Expect(cfg.GRPCMaxRecvBytes).To(Equal(64<<20), "value %q", bad)
			Expect(cfg.MaxInflight).To(Equal(100), "value %q", bad)
		}

		line := logs.String()
		Expect(line).To(ContainSubstring(`"level":"ERROR"`))
		Expect(line).To(ContainSubstring(`"msg":"invalid integer env value, falling back to default"`))
		Expect(line).To(ContainSubstring(`"value":"banana"`))
	})
})
