package seedcmder

import (
	"bytes"
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

var _ = Describe("seed command", func() {
	var (
		origCwd     string
		origTapesDB string
		origTapesSQ string
	)

	BeforeEach(func() {
		origTapesDB = os.Getenv("TAPES_DB")
		origTapesSQ = os.Getenv("TAPES_SQLITE")
		var err error
		origCwd, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.Setenv("TAPES_DB", origTapesDB)).To(Succeed())
		Expect(os.Setenv("TAPES_SQLITE", origTapesSQ)).To(Succeed())
		Expect(os.Chdir(origCwd)).To(Succeed())
	})

	It("errors when default sqlite database already has data", func() {
		ctx := context.Background()
		baseDir, err := os.MkdirTemp("", "tapes-seed-default-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = os.RemoveAll(baseDir)
		})

		Expect(os.Setenv("TAPES_DB", "")).To(Succeed())
		Expect(os.Setenv("TAPES_SQLITE", "")).To(Succeed())
		Expect(os.Chdir(baseDir)).To(Succeed())

		dbPath := filepath.Join(baseDir, "tapes.db")
		driver, err := sqlite.NewDriver(ctx, dbPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Client.Node.Create().
			SetID("seeded-node").
			Exec(ctx)).To(Succeed())
		Expect(driver.Close()).To(Succeed())

		cmd := NewSeedCmd()
		buf := &bytes.Buffer{}
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{})

		err = cmd.ExecuteContext(ctx)
		Expect(err).To(MatchError(ContainSubstring("already has data")))
	})
})
