package memlimit

import (
	"bytes"
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

// pointCgroupAt redirects the cgroup v2 limit file at a temp file holding
// contents, restoring the real path when the spec ends.
func pointCgroupAt(contents string) {
	v2 := filepath.Join(GinkgoT().TempDir(), "memory.max")
	Expect(os.WriteFile(v2, []byte(contents), 0o644)).To(Succeed())
	origV2 := cgroupV2MaxPath
	cgroupV2MaxPath = v2
	DeferCleanup(func() { cgroupV2MaxPath = origV2 })
}

var _ = Describe("soft memory limit", func() {
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

	Describe("reading the cgroup memory limit", func() {
		var v2, v1 string

		BeforeEach(func() {
			dir := GinkgoT().TempDir()
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

	Describe("ApplySoftMemoryLimit", func() {
		var previous int64

		BeforeEach(func() {
			// Snapshot and restore the process-global soft limit so specs
			// don't leak GC state into each other or into the rest of the
			// test binary.
			previous = debug.SetMemoryLimit(math.MaxInt64)
			DeferCleanup(func() { debug.SetMemoryLimit(previous) })
		})

		It("honors GOMEMLIMIT from the environment", func() {
			GinkgoT().Setenv("GOMEMLIMIT", "1GiB")
			// Point at a real cgroup limit so a fall-through would apply one.
			pointCgroupAt("2147483648\n")

			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			Expect(ApplySoftMemoryLimit(log)).To(BeZero())
			Expect(debug.SetMemoryLimit(-1)).To(Equal(int64(math.MaxInt64)),
				"the runtime's limit must be left alone when the operator pinned GOMEMLIMIT")
			Expect(buf.String()).To(And(
				ContainSubstring("honoring GOMEMLIMIT from environment"),
				ContainSubstring("GOMEMLIMIT=1GiB"),
			))
		})

		It("treats GOMEMLIMIT=off as an explicit opt-out and applies no cgroup limit", func() {
			GinkgoT().Setenv("GOMEMLIMIT", "off")
			pointCgroupAt("2147483648\n")

			Expect(ApplySoftMemoryLimit(discardLogger())).To(BeZero())
			Expect(debug.SetMemoryLimit(-1)).To(Equal(int64(math.MaxInt64)))
		})

		// This is the call `tapes serve api` (and the derive worker) makes
		// once at startup, before the server is built: with a cgroup limit
		// present the runtime's soft limit must end up at 90% of it.
		It("applies a cgroup-derived soft limit at api startup", func() {
			GinkgoT().Setenv("GOMEMLIMIT", "")
			cgroupLimit := int64(2147483648)
			pointCgroupAt("2147483648\n")

			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			got := ApplySoftMemoryLimit(log)

			want := int64(float64(cgroupLimit) * 0.9)
			Expect(got).To(Equal(want))
			Expect(debug.SetMemoryLimit(-1)).To(Equal(want),
				"debug.SetMemoryLimit must be left at 90% of the cgroup limit")
			Expect(buf.String()).To(And(
				ContainSubstring("soft memory limit applied"),
				ContainSubstring("cgroup_limit_bytes=2147483648"),
				ContainSubstring("soft_limit_bytes=1932735283"),
			))
		})

		It("is a no-op when no cgroup limit is readable", func() {
			GinkgoT().Setenv("GOMEMLIMIT", "")
			origV2, origV1 := cgroupV2MaxPath, cgroupV1MaxPath
			cgroupV2MaxPath = filepath.Join(GinkgoT().TempDir(), "absent")
			cgroupV1MaxPath = filepath.Join(GinkgoT().TempDir(), "absent")
			DeferCleanup(func() { cgroupV2MaxPath, cgroupV1MaxPath = origV2, origV1 })

			Expect(ApplySoftMemoryLimit(discardLogger())).To(BeZero())
			Expect(debug.SetMemoryLimit(-1)).To(Equal(int64(math.MaxInt64)))
		})
	})
})
