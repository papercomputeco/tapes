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

	It("creates the pid file if it does not exist", func() {
		path, err := pidFilePath(tmpDir)
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

	It("acquires successfully when a stale pid file exists with no live holder", func() {
		// Pre-seed the pid file with a fake pid; no process holds the flock.
		path, err := pidFilePath(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(path, []byte("999999999\n"), 0o600)).To(Succeed())

		holder, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).NotTo(BeNil(), "stale pid file must not block lock acquisition")
		DeferCleanup(func() { _ = holder.Close() })
	})
})

var _ = Describe("writePID", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-menu-writepid-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("persists the pid as decimal text", func() {
		holder, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).NotTo(BeNil())
		DeferCleanup(func() { _ = holder.Close() })

		Expect(writePID(holder, 12345)).To(Succeed())

		path, err := pidFilePath(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(readPID(path)).To(Equal(12345))
	})

	It("overwrites a previously recorded pid", func() {
		holder, err := acquireMenuLock(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(holder).NotTo(BeNil())
		DeferCleanup(func() { _ = holder.Close() })

		Expect(writePID(holder, 11111)).To(Succeed())
		Expect(writePID(holder, 22222)).To(Succeed())

		path, err := pidFilePath(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(readPID(path)).To(Equal(22222))
	})
})
