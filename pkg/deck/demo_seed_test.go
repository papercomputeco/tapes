package deck

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

var _ = Describe("SeedDemo", func() {
	It("allows seeding when sqlite file exists but is empty", func() {
		ctx := context.Background()
		baseDir, err := os.MkdirTemp("", "tapes-seed-empty-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = os.RemoveAll(baseDir)
		})

		dbPath := filepath.Join(baseDir, "tapes.db")
		Expect(os.WriteFile(dbPath, []byte{}, 0o644)).To(Succeed())

		sessions, messages, err := SeedDemo(ctx, dbPath, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessions).To(BeNumerically(">", 0))
		Expect(messages).To(BeNumerically(">", 0))
	})

	It("returns an error when sqlite has existing data", func() {
		ctx := context.Background()
		baseDir, err := os.MkdirTemp("", "tapes-seed-data-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = os.RemoveAll(baseDir)
		})

		dbPath := filepath.Join(baseDir, "tapes.db")
		driver, err := sqlite.NewDriver(ctx, dbPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Client.Node.Create().
			SetID("seeded-node").
			Exec(ctx)).To(Succeed())
		Expect(driver.Close()).To(Succeed())

		_, _, err = SeedDemo(ctx, dbPath, false)
		Expect(err).To(MatchError(ContainSubstring("already has data")))
	})
})
