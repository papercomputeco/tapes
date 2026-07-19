package api

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("statsCache", func() {
	var (
		cache *statsCache
		now   time.Time
	)

	BeforeEach(func() {
		cache = newStatsCache()
		now = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		cache.now = func() time.Time { return now }
	})

	It("returns a stored entry within the TTL", func() {
		cache.set("k", StatsResponse{SessionCount: 3})

		now = now.Add(statsCacheTTL - time.Second)
		got, ok := cache.get("k")
		Expect(ok).To(BeTrue())
		Expect(got.SessionCount).To(Equal(3))
	})

	It("expires entries after the TTL", func() {
		cache.set("k", StatsResponse{SessionCount: 3})

		now = now.Add(statsCacheTTL + time.Second)
		_, ok := cache.get("k")
		Expect(ok).To(BeFalse())
	})

	It("misses on unknown keys", func() {
		_, ok := cache.get("nope")
		Expect(ok).To(BeFalse())
	})

	It("stays bounded under a pathological key sweep", func() {
		for i := 0; i < statsCacheMaxEntries*2; i++ {
			cache.set(time.Duration(i).String(), StatsResponse{})
		}
		Expect(len(cache.entries)).To(BeNumerically("<=", statsCacheMaxEntries))
	})
})

var _ = Describe("snapStatsWindow", func() {
	ts := func(t time.Time) *time.Time { return &t }

	It("passes nil bounds through", func() {
		since, until := snapStatsWindow(nil, nil)
		Expect(since).To(BeNil())
		Expect(until).To(BeNil())
	})

	It("floors since to the minute", func() {
		since, _ := snapStatsWindow(ts(time.Date(2026, 4, 1, 12, 30, 45, 123e6, time.UTC)), nil)
		Expect(since.UTC()).To(Equal(time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC)))
	})

	It("ceils a fractional until to the next minute so the requested window stays contained", func() {
		// The console's custom ranges end at T23:59:59.999Z; flooring
		// would silently drop the final minute of requested data.
		_, until := snapStatsWindow(nil, ts(time.Date(2026, 4, 1, 23, 59, 59, 999e6, time.UTC)))
		Expect(until.UTC()).To(Equal(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)))
	})

	It("leaves minute-aligned bounds unchanged", func() {
		since, until := snapStatsWindow(
			ts(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)),
			ts(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)),
		)
		Expect(since.UTC()).To(Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
		Expect(until.UTC()).To(Equal(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)))
	})
})

var _ = Describe("statsCacheKey", func() {
	It("distinguishes open and bounded windows", func() {
		since := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
		until := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)
		open := statsCacheKey("org", nil, nil)
		sinceOnly := statsCacheKey("org", &since, nil)
		bounded := statsCacheKey("org", &since, &until)
		Expect(open).NotTo(Equal(sinceOnly))
		Expect(sinceOnly).NotTo(Equal(bounded))
	})

	It("scopes by org", func() {
		Expect(statsCacheKey("a", nil, nil)).NotTo(Equal(statsCacheKey("b", nil, nil)))
	})
})
