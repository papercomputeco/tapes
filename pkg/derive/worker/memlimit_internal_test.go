package worker

import (
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

var _ = Describe("parseCgroupLimit", func() {
	DescribeTable("interprets a cgroup limit file's contents",
		func(in string, wantVal int64, wantOK bool) {
			v, ok := parseCgroupLimit(in)
			Expect(ok).To(Equal(wantOK))
			Expect(v).To(Equal(wantVal))
		},
		Entry("empty (unreadable) means no limit", "", int64(0), false),
		Entry("v2 'max' means no limit", "max", int64(0), false),
		Entry("a finite limit parses", "2147483648", int64(2147483648), true),
		Entry("zero is not a usable limit", "0", int64(0), false),
		Entry("negative is rejected", "-1", int64(0), false),
		Entry("non-numeric is rejected", "garbage", int64(0), false),
		// cgroup v1 writes a near-max sentinel for "unlimited".
		Entry("v1 unlimited sentinel means no limit", "9223372036854771712", int64(0), false),
	)
})

var _ = Describe("readCgroupMemoryLimit", func() {
	var dir string
	var v2, v1 string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		v2 = filepath.Join(dir, "memory.max")
		v1 = filepath.Join(dir, "memory.limit_in_bytes")
		origV2, origV1 := cgroupV2MaxPath, cgroupV1MaxPath
		cgroupV2MaxPath, cgroupV1MaxPath = v2, v1
		DeferCleanup(func() { cgroupV2MaxPath, cgroupV1MaxPath = origV2, origV1 })
	})

	It("prefers cgroup v2 when present", func() {
		Expect(os.WriteFile(v2, []byte("2147483648\n"), 0o644)).To(Succeed())
		Expect(os.WriteFile(v1, []byte("1073741824\n"), 0o644)).To(Succeed())
		v, ok := readCgroupMemoryLimit()
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal(int64(2147483648)))
	})

	It("falls back to v1 when v2 is 'max'", func() {
		Expect(os.WriteFile(v2, []byte("max\n"), 0o644)).To(Succeed())
		Expect(os.WriteFile(v1, []byte("1073741824\n"), 0o644)).To(Succeed())
		v, ok := readCgroupMemoryLimit()
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal(int64(1073741824)))
	})

	It("reports no limit when neither file is constrained", func() {
		Expect(os.WriteFile(v2, []byte("max\n"), 0o644)).To(Succeed())
		// v1 absent
		_, ok := readCgroupMemoryLimit()
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("ApplySoftMemoryLimit", func() {
	var restore int64

	BeforeEach(func() {
		// Snapshot and restore the process-global soft limit so specs
		// don't leak GC state into each other.
		restore = debug.SetMemoryLimit(math.MaxInt64)
		DeferCleanup(func() { debug.SetMemoryLimit(restore) })
	})

	It("honors an operator GOMEMLIMIT override and sets nothing itself", func() {
		GinkgoT().Setenv("GOMEMLIMIT", "1GiB")
		Expect(ApplySoftMemoryLimit(discardLogger())).To(BeZero())
	})

	It("treats GOMEMLIMIT=off as an explicit opt-out and applies no cgroup limit", func() {
		GinkgoT().Setenv("GOMEMLIMIT", "off")
		// Point at a real cgroup limit so a fall-through would apply one.
		dir := GinkgoT().TempDir()
		v2 := filepath.Join(dir, "memory.max")
		Expect(os.WriteFile(v2, []byte("2147483648\n"), 0o644)).To(Succeed())
		origV2 := cgroupV2MaxPath
		cgroupV2MaxPath = v2
		DeferCleanup(func() { cgroupV2MaxPath = origV2 })

		Expect(ApplySoftMemoryLimit(discardLogger())).To(BeZero())
	})

	It("derives the soft limit from the cgroup memory limit", func() {
		GinkgoT().Setenv("GOMEMLIMIT", "")
		dir := GinkgoT().TempDir()
		v2 := filepath.Join(dir, "memory.max")
		Expect(os.WriteFile(v2, []byte("2147483648\n"), 0o644)).To(Succeed())
		origV2 := cgroupV2MaxPath
		cgroupV2MaxPath = v2
		DeferCleanup(func() { cgroupV2MaxPath = origV2 })

		got := ApplySoftMemoryLimit(discardLogger())
		limit := int64(2147483648)
		Expect(got).To(Equal(int64(float64(limit) * memLimitFraction)))
	})

	It("is a no-op when no cgroup limit is readable", func() {
		GinkgoT().Setenv("GOMEMLIMIT", "")
		origV2, origV1 := cgroupV2MaxPath, cgroupV1MaxPath
		cgroupV2MaxPath = filepath.Join(GinkgoT().TempDir(), "absent")
		cgroupV1MaxPath = filepath.Join(GinkgoT().TempDir(), "absent")
		DeferCleanup(func() { cgroupV2MaxPath, cgroupV1MaxPath = origV2, origV1 })
		Expect(ApplySoftMemoryLimit(discardLogger())).To(BeZero())
	})
})
