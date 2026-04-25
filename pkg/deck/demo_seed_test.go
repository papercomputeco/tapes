package deck

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("SeedDemoToDriver", func() {
	It("allows seeding an empty database", func() {
		ctx := context.Background()
		driver := inmemory.NewDriver()

		sessions, messages, err := SeedDemoToDriver(ctx, driver)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessions).To(BeNumerically(">", 0))
		Expect(messages).To(BeNumerically(">", 0))
	})

	It("returns an error when the database already has data", func() {
		ctx := context.Background()
		driver := inmemory.NewDriver()

		_, _, err := SeedDemoToDriver(ctx, driver)
		Expect(err).NotTo(HaveOccurred())

		_, _, err = SeedDemoToDriver(ctx, driver)
		Expect(err).To(MatchError(ContainSubstring("already has data")))
	})
})
