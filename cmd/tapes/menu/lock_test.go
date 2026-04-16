//go:build darwin

package menucmder

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("acquireMenuLock", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-menu-lock-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns a holder when no other lock is held", func() {
		first, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(first).NotTo(BeNil())
		Expect(first.Close()).To(Succeed())
	})

	It("returns nil to a second caller while the first holds the lock", func() {
		first, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(first).NotTo(BeNil())
		DeferCleanup(func() { _ = first.Close() })

		second, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(BeNil(), "second caller must observe the lock as held")
	})

	It("releases the lock when the holder closes the file", func() {
		first, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(first).NotTo(BeNil())
		Expect(first.Close()).To(Succeed())

		second, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(second).NotTo(BeNil(), "lock must be reacquirable after close")
		Expect(second.Close()).To(Succeed())
	})

	It("creates the lock file if it does not exist", func() {
		path, err := lockFilePath(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(filepath.Dir(path)).To(Equal(tmpDir))

		_, statErr := os.Stat(path)
		Expect(os.IsNotExist(statErr)).To(BeTrue())

		holder, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).NotTo(BeNil())
		DeferCleanup(func() { _ = holder.Close() })

		_, statErr = os.Stat(path)
		Expect(statErr).NotTo(HaveOccurred())
	})

	It("acquires successfully when a stale lock file exists with no live holder", func() {
		path, err := lockFilePath(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(path, []byte{}, 0o600)).To(Succeed())

		holder, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).NotTo(BeNil(), "stale lock file must not block acquisition")
		DeferCleanup(func() { _ = holder.Close() })
	})
})
