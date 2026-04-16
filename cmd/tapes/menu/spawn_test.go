//go:build darwin

package menucmder

import (
	"io"
	"log/slog"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Spawn", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-spawn-menu-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("does not panic on repeated invocations", func() {
		// Spawn always exec's; the spawned binary is responsible for
		// deduplicating via flock. Calling it twice in a row must not
		// crash even though the second exec may immediately fail to
		// acquire the lock.
		Expect(func() {
			Spawn(tmpDir, false, discardLogger())
			Spawn(tmpDir, true, discardLogger())
		}).NotTo(Panic())
	})
})

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
