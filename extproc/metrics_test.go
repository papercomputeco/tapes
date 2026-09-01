package extproc

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/papercomputeco/tapes/pkg/utils"
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

	It("shares one 256B-to-64MiB 14-bucket scheme across all three body-size histograms", func() {
		// bodySizeBuckets must be exactly the 14-bucket exponential range
		// from 256 B to 64 MiB.
		Expect(bodySizeBuckets).To(Equal(prometheus.ExponentialBucketsRange(256, 64*1024*1024, 14)))
		Expect(bodySizeBuckets).To(HaveLen(14))
		Expect(bodySizeBuckets[0]).To(BeNumerically("~", 256, 1e-6))
		Expect(bodySizeBuckets[13]).To(BeNumerically("~", 64*1024*1024, 1))

		// Cross-check the rendered exposition: all three body-size
		// histograms must scrape identical le= boundary sets.
		m := NewMetrics()
		m.ObserveBodyBytes("anthropic", "request", 1024)
		m.ObserveBodyBytesByOutcome("anthropic", "request", "accepted", "accepted", 1024)
		m.ObserveRequestContentLength("anthropic", 1024)

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		bodyLes := metricsBucketBoundaries(txt, "tapes_extproc_body_bytes")
		outcomeLes := metricsBucketBoundaries(txt, "tapes_extproc_body_bytes_by_outcome")
		contentLengthLes := metricsBucketBoundaries(txt, "tapes_extproc_request_content_length_bytes")

		// 14 finite boundaries plus the implicit +Inf bucket.
		Expect(bodyLes).To(HaveLen(15))
		Expect(outcomeLes).To(Equal(bodyLes))
		Expect(contentLengthLes).To(Equal(bodyLes))

		Expect(bodyLes[len(bodyLes)-1]).To(Equal("+Inf"))
		first, err := strconv.ParseFloat(bodyLes[0], 64)
		Expect(err).NotTo(HaveOccurred())
		Expect(first).To(BeNumerically("~", 256, 1e-6))
		last, err := strconv.ParseFloat(bodyLes[len(bodyLes)-2], 64)
		Expect(err).NotTo(HaveOccurred())
		Expect(last).To(BeNumerically("~", 64*1024*1024, 1))
	})

	It("routes content-length metric providers through the normalizeProvider allowlist", func() {
		m := NewMetrics()
		m.ObserveRequestContentLength("custom-weird-provider", 512)
		m.ObserveRequestContentLengthUnknown("custom-weird-provider")

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		// Exact sample lines: off-allowlist providers normalize to "other",
		// and the full-line match proves provider is the only label.
		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_bytes_count{provider="other"} 1`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_unknown_total{provider="other"} 1`))

		// The raw provider string must never leak into a label row.
		Expect(txt).NotTo(ContainSubstring(`provider="custom-weird-provider"`))
	})

	It("ObserveRequestContentLengthUnknown increments the counter and never touches the histogram", func() {
		m := NewMetrics()
		m.ObserveRequestContentLengthUnknown("anthropic")
		m.ObserveRequestContentLengthUnknown("anthropic")

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_unknown_total{provider="anthropic"} 2`))

		// No histogram series may render at all: absent series means _count
		// is 0 and the le="256" bucket is 0 (no Observe(0) stand-in).
		Expect(txt).NotTo(ContainSubstring(`tapes_extproc_request_content_length_bytes_bucket`))
		Expect(txt).NotTo(ContainSubstring(`tapes_extproc_request_content_length_bytes_count`))
		Expect(txt).NotTo(ContainSubstring(`tapes_extproc_request_content_length_bytes_sum`))
	})

	It("declares utils.Version/utils.Sha for the ldflags contract and renders build_info as 1", func() {
		// Referencing the package vars directly means compiling this file
		// proves the utils.Version/utils.Sha ldflags symbols still exist.
		// Values are not pinned: release builds stamp them via -X.
		Expect(utils.Version).NotTo(BeEmpty())
		Expect(utils.Sha).NotTo(BeEmpty())

		// Lock the other side of the contract: the Dagger build must still
		// stamp exactly these symbol names, and the extproc image build must
		// still route through it.
		_, file, _, ok := runtime.Caller(0)
		Expect(ok).To(BeTrue(), "runtime.Caller failed")
		build, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".dagger", "build.go"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(build)).To(ContainSubstring(`-X 'github.com/papercomputeco/tapes/pkg/utils.Version=`))
		Expect(string(build)).To(ContainSubstring(`-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=`))
		img, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".dagger", "extproc.go"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(img)).To(ContainSubstring(`releaseLDFlags(`))

		m := NewMetrics()
		m.SetBuildInfo("v1.2.3", "abc1234")

		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := srv.Client().Get(srv.URL + "/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		txt := string(body)

		// Exact line: value 1 (build_info join convention) and only the
		// version/commit labels.
		Expect(txt).To(ContainSubstring(`tapes_extproc_build_info{commit="abc1234",version="v1.2.3"} 1`))
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

// metricsBucketBoundaries extracts the ordered, de-duplicated le= boundary
// values rendered for a histogram's _bucket samples in a scraped exposition body.
func metricsBucketBoundaries(txt, metric string) []string {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(metric) + `_bucket\{[^}]*le="([^"]+)"[^}]*\} `)
	var les []string
	seen := map[string]bool{}
	for _, match := range re.FindAllStringSubmatch(txt, -1) {
		if !seen[match[1]] {
			seen[match[1]] = true
			les = append(les, match[1])
		}
	}
	return les
}

var _ = Describe("model family labelling", func() {
	// Both halves must agree: modelFamily derives the label, and
	// normalizeModelFamily gates it against the bounded Prometheus allowlist.
	// A family produced by one but missing from the other silently collapses
	// to "other", so these pin the pair together.
	It("splits Fable 5.1 from Fable 5 and keeps both through the allowlist", func() {
		for raw, want := range map[string]string{
			"claude-fable-5-1":          "claude-fable-5-1",
			"claude-fable-5-1-20260101": "claude-fable-5-1",
			"claude-fable-5":            "claude-fable-5",
			"claude-fable-5-20260101":   "claude-fable-5",
		} {
			Expect(modelFamily(raw)).To(Equal(want), "modelFamily(%q)", raw)
			Expect(normalizeModelFamily(modelFamily(raw))).To(Equal(want),
				"allowlist dropped %q to other", raw)
		}
	})

	It("gives every currently-priced flagship its own family", func() {
		// Regression guard for the whole class: claude-opus-4 does not
		// prefix-match claude-opus-5, so a new major silently lands in
		// "other" and disappears from cost-by-family panels. Opus 5 sat
		// that way from its July 2026 release until this test.
		for raw, want := range map[string]string{
			"claude-opus-5":             "claude-opus-5",
			"claude-opus-5[1m]":         "claude-opus-5",
			"claude-opus-4-8":           "claude-opus-4",
			"claude-sonnet-5":           "claude-sonnet-5",
			"claude-haiku-4-6":          "claude-haiku-4-6",
			"claude-haiku-4-5":          "claude-haiku-4-5",
			"claude-haiku-4-5-20251001": "claude-haiku-4-5",
		} {
			Expect(modelFamily(raw)).To(Equal(want), "modelFamily(%q)", raw)
			Expect(normalizeModelFamily(modelFamily(raw))).To(Equal(want),
				"allowlist dropped %q to other", raw)
		}
	})

	It("keeps an unknown model out of the allowlist", func() {
		Expect(modelFamily("some-unreleased-model")).To(Equal("other"))
		Expect(modelFamily("")).To(Equal("unknown"))
	})
})
