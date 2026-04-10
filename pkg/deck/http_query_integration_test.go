package deck_test

import (
	"context"
	"net"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/deck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// This integration test stands up the full deck → HTTP → in-process API →
// SQLite chain against a freshly seeded demo database. It validates that
// HTTPQuery produces the same shape of results as the legacy SQLite-backed
// Query, end to end.
var _ = Describe("HTTPQuery integration", func() {
	var (
		ctx     context.Context
		query   *deck.HTTPQuery
		stopAPI func()
	)

	BeforeEach(func() {
		ctx = context.Background()

		baseDir, err := os.MkdirTemp("", "tapes-httpquery-integration-*")
		Expect(err).NotTo(HaveOccurred())

		dbPath := filepath.Join(baseDir, "tapes.db")
		sessionCount, messageCount, err := deck.SeedDemo(ctx, dbPath, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionCount).To(BeNumerically(">", 0))
		Expect(messageCount).To(BeNumerically(">", 0))

		// Stand up an in-process API server bound to a random localhost port.
		driver, err := sqlite.NewDriver(ctx, dbPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Migrate(ctx)).To(Succeed())

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
			_ = os.RemoveAll(baseDir)
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

			// HTTPQuery's Overview returns groups, whose IDs are synthetic.
			// To exercise the per-session detail path we need a real leaf
			// hash, which we get from the underlying members. The cache,
			// populated by Overview above, has the raw per-session candidates.
			// Picking the first session in the overview is therefore a group
			// ID; SessionDetail handles either form.
			sessionID := overview.Sessions[0].ID
			detail, err := query.SessionDetail(ctx, sessionID)
			Expect(err).NotTo(HaveOccurred())
			Expect(detail.Summary.ID).NotTo(BeEmpty())
			Expect(detail.Messages).NotTo(BeEmpty())
			Expect(detail.ToolFrequency).NotTo(BeNil())
		})

		It("returns an error for an unknown session", func() {
			_, err := query.SessionDetail(ctx, "definitely-not-a-real-hash-12345")
			Expect(err).To(HaveOccurred())
		})
	})
})
