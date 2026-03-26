package deck

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query integration", func() {
	var (
		query   *Query
		closeFn func() error
		ctx     context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		baseDir, err := os.MkdirTemp("", "tapes-query-integration-*")
		Expect(err).NotTo(HaveOccurred())

		dbPath := filepath.Join(baseDir, "tapes.db")
		sessions, messages, err := SeedDemo(ctx, dbPath, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessions).To(BeNumerically(">", 0))
		Expect(messages).To(BeNumerically(">", 0))

		pricing := DefaultPricing()
		query, closeFn, err = NewQuery(ctx, dbPath, pricing)
		Expect(err).NotTo(HaveOccurred())

		DeferCleanup(func() {
			_ = closeFn()
			_ = os.RemoveAll(baseDir)
		})
	})

	Describe("Overview", func() {
		It("returns sessions with no filters", func() {
			overview, err := query.Overview(ctx, Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).NotTo(BeEmpty())
			Expect(overview.TotalCost).To(BeNumerically(">", 0))
			Expect(overview.TotalTokens).To(BeNumerically(">", 0))
			Expect(overview.CostByModel).NotTo(BeEmpty())
		})

		It("filters by model", func() {
			all, err := query.Overview(ctx, Filters{})
			Expect(err).NotTo(HaveOccurred())

			filtered, err := query.Overview(ctx, Filters{Model: "claude-sonnet-4.5"})
			Expect(err).NotTo(HaveOccurred())

			Expect(len(filtered.Sessions)).To(BeNumerically("<=", len(all.Sessions)))
			for _, s := range filtered.Sessions {
				Expect(normalizeModel(s.Model)).To(Equal("claude-sonnet-4.5"))
			}
		})

		It("filters by status", func() {
			overview, err := query.Overview(ctx, Filters{Status: StatusCompleted})
			Expect(err).NotTo(HaveOccurred())

			for _, s := range overview.Sessions {
				Expect(s.Status).To(Equal(StatusCompleted))
			}
		})

		It("computes success rate correctly", func() {
			overview, err := query.Overview(ctx, Filters{})
			Expect(err).NotTo(HaveOccurred())

			total := len(overview.Sessions)
			Expect(total).To(BeNumerically(">", 0))
			Expect(overview.SuccessRate).To(BeNumerically(">=", 0))
			Expect(overview.SuccessRate).To(BeNumerically("<=", 1))
			Expect(overview.Completed + overview.Failed + overview.Abandoned).To(Equal(total))
		})
	})

	Describe("SessionDetail", func() {
		It("returns detail for a valid session ID", func() {
			overview, err := query.Overview(ctx, Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).NotTo(BeEmpty())

			sessionID := overview.Sessions[0].ID
			detail, err := query.SessionDetail(ctx, sessionID)
			Expect(err).NotTo(HaveOccurred())
			Expect(detail.Summary.ID).NotTo(BeEmpty())
			Expect(detail.Messages).NotTo(BeEmpty())
			Expect(detail.ToolFrequency).NotTo(BeNil())
		})

		It("returns grouped messages", func() {
			overview, err := query.Overview(ctx, Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview.Sessions).NotTo(BeEmpty())

			sessionID := overview.Sessions[0].ID
			detail, err := query.SessionDetail(ctx, sessionID)
			Expect(err).NotTo(HaveOccurred())
			Expect(detail.GroupedMessages).NotTo(BeEmpty())
		})

		It("returns an error for a non-existent session", func() {
			_, err := query.SessionDetail(ctx, "non-existent-id-12345")
			Expect(err).To(HaveOccurred())
		})
	})
})
