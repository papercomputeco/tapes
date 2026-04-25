package deck_test

import (
	"context"
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/deck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// This integration test stands up the full deck → HTTP → in-process API chain
// against a freshly seeded demo database. It validates that HTTPQuery returns
// sane deck results end to end.
var _ = Describe("HTTPQuery integration", func() {
	var (
		ctx     context.Context
		query   *deck.HTTPQuery
		stopAPI func()
	)

	BeforeEach(func() {
		ctx = context.Background()

		driver := inmemory.NewDriver()
		sessionCount, messageCount, err := deck.SeedDemoToDriver(ctx, driver)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionCount).To(BeNumerically(">", 0))
		Expect(messageCount).To(BeNumerically(">", 0))

		pricing := deck.DefaultPricing()

		server, err := api.NewServer(api.Config{
			ListenAddr: ":0",
			Pricing:    pricing,
		}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		lc := net.ListenConfig{}
		listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())

		serverErr := make(chan error, 1)
		go func() {
			serverErr <- server.RunWithListener(listener)
		}()

		target := "http://" + listener.Addr().String()
		query = deck.NewHTTPQuery(target, pricing)

		stopAPI = func() {
			_ = server.Shutdown()
			_ = driver.Close()
		}

		DeferCleanup(func() {
			stopAPI()
		})
	})

	Describe("Overview", func() {
		It("returns sessions with no filters", func() {
			overview, err := query.Overview(ctx, deck.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).NotTo(BeEmpty())
			Expect(overview.TotalCost).To(BeNumerically(">", 0))
			Expect(overview.TotalTokens).To(BeNumerically(">", 0))
			Expect(overview.CostByModel).NotTo(BeEmpty())
		})

		It("filters by status", func() {
			overview, err := query.Overview(ctx, deck.Filters{Status: deck.StatusCompleted})
			Expect(err).NotTo(HaveOccurred())
			for _, s := range overview.Sessions {
				Expect(s.Status).To(Equal(deck.StatusCompleted))
			}
		})

		It("computes a sane success rate", func() {
			overview, err := query.Overview(ctx, deck.Filters{})
			Expect(err).NotTo(HaveOccurred())
			total := len(overview.Sessions)
			Expect(total).To(BeNumerically(">", 0))
			Expect(overview.SuccessRate).To(BeNumerically(">=", 0))
			Expect(overview.SuccessRate).To(BeNumerically("<=", 1))
			Expect(overview.Completed + overview.Failed + overview.Abandoned).To(Equal(total))
		})
	})

	Describe("SessionDetail", func() {
		It("returns detail for a real session ID", func() {
			overview, err := query.Overview(ctx, deck.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).NotTo(BeEmpty())

			sessionID := overview.Sessions[0].ID
			detail, err := query.SessionDetail(ctx, sessionID)
			Expect(err).NotTo(HaveOccurred())
			Expect(detail.Summary.ID).NotTo(BeEmpty())
			Expect(detail.Messages).NotTo(BeEmpty())
		})
	})
})
