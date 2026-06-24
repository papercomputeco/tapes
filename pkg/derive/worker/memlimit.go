package worker

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// A full session derive allocates transiently far above its live set:
// every wire turn re-sends the whole prior conversation, so the deriver
// re-parses an O(N) request per turn and the per-turn garbage piles up
// faster than the GC reclaims it. Under the default GOGC=100 the heap is
// allowed to roughly DOUBLE its live size before a collection, so a big
// session's transient peak runs ~2x its (already large) live set and can
// exceed the container memory limit in a brief spike, getting the worker
// OOM-killed even though its steady-state live set fits the budget
// (PCC-767).
//
// The live set itself fits the budget; only the transient overshoot does
// not. A soft memory limit (GOMEMLIMIT) is the right tool: it makes the
// GC pace against a ceiling instead of against live-heap-times-GOGC, so
// the peak collapses back toward the live set, trading some extra GC CPU
// for a bounded heap. We derive the ceiling from the cgroup memory limit
// the orchestrator already sets, so it tracks the container budget
// without a second knob to keep in sync.

// memLimitFraction is the share of the cgroup memory limit used as the
// soft heap ceiling. The remainder is headroom for non-heap memory
// (goroutine stacks, the pgx driver, the Go runtime itself) so the soft
// limit bites — driving more frequent GC — before the hard cgroup limit
// OOMKills the process.
const memLimitFraction = 0.9

// cgroup limit file paths (v2 first, then v1). Overridable in tests.
var (
	cgroupV2MaxPath = "/sys/fs/cgroup/memory.max"
	cgroupV1MaxPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

// cgroupUnlimited is the sentinel cgroup v1 writes for "no limit": a
// page-aligned near-max int64. Anything at or above it (or v2's literal
// "max") means the container is unconstrained, so we set no soft limit.
const cgroupUnlimited int64 = 0x7000000000000000

// ApplySoftMemoryLimit sets a soft heap ceiling (GOMEMLIMIT) derived from
// the cgroup memory limit, so a large session's transient allocation
// overshoot is GC-paced under the container budget instead of OOMKilling
// the worker. It is a no-op — leaving the Go default in place — when the
// operator already pinned GOMEMLIMIT, when no cgroup memory limit is
// readable (local dev, unconstrained container), or on non-Linux.
//
// Returns the soft limit applied in bytes, or 0 when none was set.
func ApplySoftMemoryLimit(log *slog.Logger) int64 {
	// Respect an explicit operator override: the Go runtime already
	// honored GOMEMLIMIT at startup, so don't second-guess it. This
	// includes "off", which disables the limit — an operator who sets it
	// is opting out, and re-applying a cgroup-derived limit would silently
	// override that intent.
	if v := strings.TrimSpace(os.Getenv("GOMEMLIMIT")); v != "" {
		log.Info("soft memory limit: honoring GOMEMLIMIT from environment", "GOMEMLIMIT", v)
		return 0
	}

	limit, ok := readCgroupMemoryLimit()
	if !ok {
		log.Debug("soft memory limit: no cgroup memory limit found; leaving GC at default")
		return 0
	}

	soft := int64(float64(limit) * memLimitFraction)
	debug.SetMemoryLimit(soft)
	log.Info("soft memory limit applied",
		"cgroup_limit_bytes", limit,
		"soft_limit_bytes", soft,
		"fraction", memLimitFraction)
	return soft
}

// readCgroupMemoryLimit returns the container's memory limit in bytes,
// preferring cgroup v2. ok is false when no finite limit is configured.
func readCgroupMemoryLimit() (limit int64, ok bool) {
	if v, ok := parseCgroupLimit(readFileTrim(cgroupV2MaxPath)); ok {
		return v, true
	}
	return parseCgroupLimit(readFileTrim(cgroupV1MaxPath))
}

// parseCgroupLimit interprets one cgroup limit file's contents. The empty
// string (unreadable file), v2's "max", and v1's near-max sentinel all
// mean "no limit".
func parseCgroupLimit(s string) (int64, bool) {
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= cgroupUnlimited {
		return 0, false
	}
	return v, true
}

// readFileTrim reads a small sysfs file and trims it, returning "" on any
// error (missing path on non-Linux, permission, etc.).
func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
