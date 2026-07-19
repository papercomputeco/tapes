package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// statsStubDriver wraps a real storage.Driver and implements the
// storage.SpanStatsReader capability interface with a canned aggregate,
// recording the window it received so specs can assert org/window
// threading. The node-layer CountSessions path (and StemCount) is retired:
// /v1/stats now always reads the span projection, so this is the only
// surface the handler exercises. It follows the sessionsStubDriver pattern.
type statsStubDriver struct {
	storage.Driver

	stats     storage.SpanStats
	statsErr  error
	calls     int
	lastOrg   string
	lastSince *time.Time
	lastUntil *time.Time
}

func (d *statsStubDriver) AggregateSpanStats(_ context.Context, orgID string, since, until *time.Time) (storage.SpanStats, error) {
	d.calls++
	d.lastOrg = orgID
	d.lastSince = since
	d.lastUntil = until
	return d.stats, d.statsErr
}

var _ = Describe("v1 session handlers", func() {
	Describe("GET /v1/stats", func() {
		newStatsServer := func(driver storage.Driver) *Server {
			server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			return server
		}

		It("returns the span-layer aggregate verbatim", func() {
			drv := &statsStubDriver{
				Driver: inmemory.NewDriver(),
				stats: storage.SpanStats{
					SessionCount:    2,
					TurnCount:       4,
					CompletedCount:  1,
					InputTokens:     1_500_000,
					OutputTokens:    750_000,
					TotalCostUSD:    37.5,
					TotalDurationNS: 3 * int64(time.Second),
					ToolCalls:       2,
				},
			}
			body := decodeStats(newStatsServer(drv), "/v1/stats")

			Expect(body.SessionCount).To(Equal(2))
			Expect(body.TurnCount).To(Equal(4))
			Expect(body.CompletedCount).To(Equal(1))
			Expect(body.InputTokens).To(Equal(int64(1_500_000)))
			Expect(body.OutputTokens).To(Equal(int64(750_000)))
			Expect(body.TotalCost).To(BeNumerically("~", 37.5, 0.0001))
			Expect(body.TotalDurationMs).To(Equal(int64(3 * time.Second / time.Millisecond)))
			Expect(body.ToolCalls).To(Equal(2))
			Expect(drv.calls).To(Equal(1))
		})

		It("threads the since/until window through to the reader", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(drv)

			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00Z&until=2026-04-02T00:00:00Z")

			Expect(drv.calls).To(Equal(1))
			Expect(drv.lastSince).NotTo(BeNil())
			Expect(drv.lastUntil).NotTo(BeNil())
			Expect(drv.lastSince.UTC()).To(Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
			Expect(drv.lastUntil.UTC()).To(Equal(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)))
		})

		It("serves a repeated window from cache without re-aggregating", func() {
			drv := &statsStubDriver{
				Driver: inmemory.NewDriver(),
				stats:  storage.SpanStats{SessionCount: 7, TotalCostUSD: 12.5},
			}
			server := newStatsServer(drv)

			first := decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00Z")
			second := decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00Z")

			Expect(drv.calls).To(Equal(1))
			Expect(second).To(Equal(first))
		})

		It("collapses millisecond-unique since values into one snapped window", func() {
			// Dashboard clients anchor since on their own clock, so two
			// requests for the same logical window differ by milliseconds.
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(drv)

			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00.123Z")
			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:00:59.987Z")

			Expect(drv.calls).To(Equal(1))
			Expect(drv.lastSince).NotTo(BeNil())
			Expect(drv.lastSince.UTC()).To(Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
		})

		It("caches distinct windows separately", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(drv)

			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00Z")
			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:01:00Z")

			Expect(drv.calls).To(Equal(2))
		})

		It("does not cache aggregate failures", func() {
			drv := &statsStubDriver{
				Driver:   inmemory.NewDriver(),
				statsErr: errors.New("boom"),
			}
			server := newStatsServer(drv)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stats?since=2026-04-01T00:00:00Z", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(fiber.StatusInternalServerError))

			drv.statsErr = nil
			_ = decodeStats(server, "/v1/stats?since=2026-04-01T00:00:00Z")
			Expect(drv.calls).To(Equal(2))
		})

		It("returns zeros across every field for an empty aggregate", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			body := decodeStats(newStatsServer(drv), "/v1/stats")

			Expect(body.SessionCount).To(Equal(0))
			Expect(body.TurnCount).To(Equal(0))
			Expect(body.CompletedCount).To(Equal(0))
			Expect(body.TotalCost).To(Equal(0.0))
			Expect(body.InputTokens).To(Equal(int64(0)))
			Expect(body.OutputTokens).To(Equal(int64(0)))
			Expect(body.TotalDurationMs).To(Equal(int64(0)))
			Expect(body.ToolCalls).To(Equal(0))
		})

		It("returns 500 when the driver lacks the span-stats capability", func() {
			// A bare in-memory driver does not implement SpanStatsReader,
			// and the legacy node-layer fallback is retired.
			server := newStatsServer(inmemory.NewDriver())

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stats", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(fiber.StatusInternalServerError))
		})
	})
})

func decodeStats(server *Server, path string) StatsResponse {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
	defer resp.Body.Close()
	var body StatsResponse
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(raw, &body)).To(Succeed())
	return body
}
