package extproc

import (
	"io"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Extproc metrics", func() {
	It("serves a scrapeable /metrics body with every drop reason pre-rendered", func() {
		m := NewMetrics()

		// Stand the handler up behind an httptest.Server so we hit the real
		// HTTP path rather than calling the Handler method directly.
		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		// Dropped is pre-rendered per DropReason; captured only surfaces
		// once a counter is incremented (separate test below).
		Expect(txt).To(ContainSubstring("tapes_extproc_turns_dropped_total"))
		Expect(txt).To(ContainSubstring("tapes_extproc_inflight_dispatches"))

		// Every closed-enum drop reason should appear in the pre-rendered
		// label rows so dashboards don't see gaps on a cold process.
		for _, r := range AllDropReasons() {
			Expect(txt).To(ContainSubstring(`reason="`+string(r)+`"`),
				"missing drop reason label: %s", r)
		}

		// Pre-rendering covers the providers we currently route through.
		for _, prov := range []string{"anthropic", "openai", "ollama", "unknown"} {
			Expect(txt).To(ContainSubstring(`provider="`+prov+`"`),
				"missing provider label: %s", prov)
		}
	})

	It("ObserveTurnSize increments tapes_extproc_turns_large_total only above the threshold", func() {
		m := NewMetrics()
		m.SetLargeTurnThreshold(1024) // 1 KiB for the test

		m.ObserveTurnSize("anthropic", 500)  // below → no increment
		m.ObserveTurnSize("anthropic", 2048) // above → increments
		m.ObserveTurnSize("anthropic", 4096) // above → increments again

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()
		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_large_total{provider="anthropic"} 2`))
		// And specifically NOT the dropped counter — a large turn is still
		// captured, not a drop.
		Expect(txt).NotTo(ContainSubstring(`tapes_extproc_turns_dropped_total{provider="anthropic"} 1`))
	})

	It("exposes a configurable large-turn threshold, defaulting to 4 MiB", func() {
		m := NewMetrics()
		Expect(m.LargeTurnThreshold()).To(Equal(DefaultLargeTurnThreshold))
		Expect(DefaultLargeTurnThreshold).To(Equal(4 * 1024 * 1024))

		m.SetLargeTurnThreshold(8 * 1024 * 1024)
		Expect(m.LargeTurnThreshold()).To(Equal(8 * 1024 * 1024))

		// Zero / negative inputs are rejected so the threshold can't be
		// accidentally disabled.
		m.SetLargeTurnThreshold(0)
		Expect(m.LargeTurnThreshold()).To(Equal(8 * 1024 * 1024))
	})

	It("labels captured turns on ObserveAccepted", func() {
		m := NewMetrics()
		m.ObserveAccepted("anthropic")
		m.ObserveAccepted("anthropic")
		m.ObserveAccepted("openai")

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)
		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_captured_total{provider="anthropic"} 2`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_captured_total{provider="openai"} 1`))
	})

	It("records salvaged truncated response decodes separately from normal decode success", func() {
		m := NewMetrics()
		m.ObserveResponseDecoded("gzip", "ok")
		m.ObserveResponseDecodeSalvaged("anthropic", "gzip", true)
		m.ObserveResponseDecodeSalvaged("anthropic", "gzip", false)

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		Expect(txt).To(ContainSubstring(`tapes_extproc_response_decoded_total{encoding="gzip",result="ok"} 1`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_response_decode_salvaged_total{encoding="gzip",message_stop_seen="true",provider="anthropic"} 1`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_response_decode_salvaged_total{encoding="gzip",message_stop_seen="false",provider="anthropic"} 1`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_response_decode_salvaged_total{encoding="x-gzip",message_stop_seen="true",provider="anthropic"} 0`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_response_decode_salvaged_total{encoding="x-gzip",message_stop_seen="false",provider="anthropic"} 0`))
	})

	It("normalizes terminal outcome labels into bounded sets", func() {
		m := NewMetrics()
		m.ObserveTerminal("anthropic", "dropped", string(DropUpstreamStatus), OutcomeContext{
			Endpoint:            "messages",
			Stream:              "true",
			ModelFamily:         "claude-opus-4",
			UpstreamStatusClass: "5xx",
			UpstreamStatus:      503,
		})
		m.ObserveTerminal("custom-provider", "surprise", "made_up", OutcomeContext{
			Endpoint:       "/raw/path/123",
			Stream:         "maybe",
			ModelFamily:    "claude-very-specific-20260101",
			UpstreamStatus: 799,
		})

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_terminal_total`))
		Expect(txt).To(ContainSubstring(`endpoint="messages"`))
		Expect(txt).To(ContainSubstring(`model_family="claude-opus-4"`))
		Expect(txt).To(ContainSubstring(`reason="upstream_status"`))
		Expect(txt).To(ContainSubstring(`stream="true"`))
		Expect(txt).To(ContainSubstring(`upstream_status_class="5xx"`))

		Expect(txt).To(ContainSubstring(`provider="other"`))
		Expect(txt).To(ContainSubstring(`outcome="unknown"`))
		Expect(txt).To(ContainSubstring(`reason="other"`))
		Expect(txt).To(ContainSubstring(`endpoint="unknown"`))
		Expect(txt).To(ContainSubstring(`stream="unknown"`))
		Expect(txt).To(ContainSubstring(`model_family="other"`))
	})

	It("AllDropReasons has no duplicates and no whitespace", func() {
		seen := map[string]bool{}
		for _, r := range AllDropReasons() {
			s := string(r)
			Expect(seen[s]).To(BeFalse(), "duplicate %q", s)
			seen[s] = true
			Expect(strings.TrimSpace(s)).To(Equal(s))
		}
	})
})
