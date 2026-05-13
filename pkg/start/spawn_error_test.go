package start_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("SpawnError", func() {
	var tempDir string

	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "tapes-spawn-error-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tempDir)).To(Succeed())
	})

	It("LoadSpawnError returns nil when no sentinel exists", func() {
		m, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())
		got, err := m.LoadSpawnError()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil())
	})

	It("Record + Load round-trips the reason, DSN, and detail", func() {
		m, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		Expect(m.RecordSpawnError(start.SpawnError{
			Reason: start.ReasonPostgresUnreachable,
			DSN:    "postgres://x:y@localhost:5432/tapes?sslmode=disable",
			Detail: "dial tcp 127.0.0.1:5432: connect: connection refused",
		})).To(Succeed())

		got, err := m.LoadSpawnError()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.Reason).To(Equal(start.ReasonPostgresUnreachable))
		Expect(got.DSN).To(ContainSubstring("localhost:5432"))
		Expect(got.Detail).To(ContainSubstring("connection refused"))
		Expect(got.At.IsZero()).To(BeFalse())
	})

	It("ClearSpawnError removes the sentinel; missing-file is a no-op", func() {
		m, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		Expect(m.RecordSpawnError(start.SpawnError{Reason: start.ReasonOther, Detail: "boom"})).To(Succeed())
		Expect(m.ClearSpawnError()).To(Succeed())
		Expect(m.ClearSpawnError()).To(Succeed())

		got, err := m.LoadSpawnError()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil())
	})

	It("LoadSpawnError treats a corrupt sentinel as missing and clears it", func() {
		m, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(filepath.Join(tempDir, "last-spawn-error.json"), []byte("{not json"), 0o600)).To(Succeed())

		got, err := m.LoadSpawnError()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil())

		_, statErr := os.Stat(filepath.Join(tempDir, "last-spawn-error.json"))
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})
})
