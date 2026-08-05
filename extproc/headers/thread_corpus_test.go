package headers

// Oracle and authored-home gate for the shared thread fixture corpus
// (fixtures/thread/), which pins the harness-native sub-thread header ↔
// meta.thread_id contract.
//
// The canonical vocabulary — the header spellings and the per-harness
// resolution rules — lives in the tapes-harnesses crate: src/envelope.rs
// (CLAUDE_THREAD_ID_HEADERS, CODEX_THREAD_ID_HEADER, CODEX_SESSION_ID_HEADER,
// HARNESS_THREAD_ID_RULES) with the lifecycle counterpart in
// src/attribution/codex_app (session_id = root, agent_id = child thread).
// This package's ThreadID is one of four independent readers of that
// vocabulary (with proxy/header.ThreadID, pkg/backfill's
// threadIDFromHeaders, and the crate's envelope::thread_id), so the failure
// messages below name the crate: a red test here means one side renamed a
// header or changed a rule without the other.
//
// Three gates, mirroring the envelope corpus's structure:
//
//  1. the oracle — every case resolves through this package's ThreadID to
//     the declared thread_id;
//  2. the DIGEST seal — the authored home recomputes it so a corpus change
//     is a one-line reviewable diff, and vendored copies elsewhere can
//     detect staleness against the same value;
//  3. coverage — the corpus still exercises each rule, stated as properties
//     so the assertions survive case renames.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
)

// canonicalHome is where the thread vocabulary is authored; every failure
// message points at it so the fix lands in the right repository.
const canonicalHome = "tapes-harnesses src/envelope.rs (HARNESS_THREAD_ID_RULES) / src/attribution/codex_app"

func threadCorpusDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "thread")
}

type threadCase struct {
	file        string
	Name        string            `json:"name"`
	Harness     string            `json:"harness"`
	Description string            `json:"description"`
	Headers     map[string]string `json:"headers"`
	ThreadID    string            `json:"thread_id"`
	Grounding   string            `json:"grounding"`
	Notes       string            `json:"notes"`
}

func loadThreadCases(t *testing.T) []threadCase {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(threadCorpusDir(), "cases", "*.json"))
	if err != nil {
		t.Fatalf("glob thread corpus cases: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no thread corpus cases under %s", threadCorpusDir())
	}
	sort.Strings(matches)

	out := make([]threadCase, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		var c threadCase
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("%s: %v", filepath.Base(m), err)
		}
		c.file = filepath.Base(m)
		out = append(out, c)
	}
	return out
}

// TestThreadCorpusOracle proves this package's ThreadID matches the corpus.
func TestThreadCorpusOracle(t *testing.T) {
	for _, c := range loadThreadCases(t) {
		t.Run(c.Name, func(t *testing.T) {
			if c.Name+".json" != c.file {
				t.Errorf("%s: name %q must match the filename", c.file, c.Name)
			}
			if c.Grounding == "" {
				t.Errorf("%s: grounding is required", c.file)
			}
			pairs := make([][2]string, 0, len(c.Headers))
			for _, k := range slices.Sorted(maps.Keys(c.Headers)) {
				if k != strings.ToLower(k) {
					t.Errorf("%s: header %q must be lower-cased", c.file, k)
				}
				pairs = append(pairs, [2]string{k, c.Headers[k]})
			}
			if got := ThreadID(mkHeaders(pairs...)); got != c.ThreadID {
				t.Errorf("ThreadID(%s): got %q want %q\n"+
					"  This case is the shared thread-identity contract. If the resolution\n"+
					"  rule or a header spelling changed here, the same change belongs in the\n"+
					"  canonical home first: %s.",
					c.Name, got, c.ThreadID, canonicalHome)
			}
		})
	}
}

// TestThreadCorpusDigest recomputes the seal over the case bytes, exactly as
// fixtures/thread/README.md specifies it. This is the authored home, so on a
// legitimate corpus change it prints the new value to copy into DIGEST;
// vendored copies elsewhere recompute the same seal to catch staleness.
func TestThreadCorpusDigest(t *testing.T) {
	casesDir := filepath.Join(threadCorpusDir(), "cases")
	matches, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("glob %s: %v (%d files)", casesDir, err, len(matches))
	}
	sort.Strings(matches)

	outer := sha256.New()
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		sum := sha256.Sum256(b)
		fmt.Fprintf(outer, "%s  %s\n", filepath.Base(m), hex.EncodeToString(sum[:]))
	}
	got := "sha256:" + hex.EncodeToString(outer.Sum(nil))

	sealed, err := os.ReadFile(filepath.Join(threadCorpusDir(), "DIGEST"))
	if err != nil {
		t.Fatalf("read DIGEST: %v", err)
	}
	if want := strings.TrimSpace(string(sealed)); got != want {
		t.Errorf("thread corpus digest mismatch:\n  sealed:     %s\n  recomputed: %s\n"+
			"If the corpus change is intentional, update fixtures/thread/DIGEST to the\n"+
			"recomputed value and re-sync every vendored copy from the same commit.",
			want, got)
	}
}

// TestThreadCorpusCoverage fails if the corpus stops exercising a rule of the
// shared contract. Stated as properties rather than case names so the
// assertions survive renames; deleting the only case that covers a rule is
// otherwise invisible, and that is exactly how independent readers drift.
//
// The properties reference this package's header constants on purpose: if a
// constant's spelling drifts from the corpus (i.e. from the canonical crate
// vocabulary), the affected rules read as uncovered and this test goes red
// naming the other home.
func TestThreadCorpusCoverage(t *testing.T) {
	cases := loadThreadCases(t)

	claudeHeader := harnessThreadIDHeaders[0]

	rules := []struct {
		what  string
		holds func(threadCase) bool
	}{
		{
			what: "the Claude presence rule: a non-empty " + claudeHeader + " resolves verbatim",
			holds: func(c threadCase) bool {
				v := c.Headers[claudeHeader]
				return v != "" && c.ThreadID == v
			},
		},
		{
			what: "an empty " + claudeHeader + " counting as absent",
			holds: func(c threadCase) bool {
				v, present := c.Headers[claudeHeader]
				return present && v == "" && c.ThreadID == ""
			},
		},
		{
			what: "the Codex divergent pair (" + CodexThreadID + " != " + CodexSessionID + ") resolving to the thread id",
			holds: func(c threadCase) bool {
				tid, sid := c.Headers[CodexThreadID], c.Headers[CodexSessionID]
				return tid != "" && sid != "" && tid != sid && c.ThreadID == tid &&
					c.Headers[claudeHeader] == ""
			},
		},
		{
			what: "the Codex root guard: an equal pair resolves to the main thread",
			holds: func(c threadCase) bool {
				tid, sid := c.Headers[CodexThreadID], c.Headers[CodexSessionID]
				return tid != "" && tid == sid && c.ThreadID == ""
			},
		},
		{
			what: "a lone " + CodexThreadID + " not being a recognised Codex shape",
			holds: func(c threadCase) bool {
				_, hasSession := c.Headers[CodexSessionID]
				return c.Headers[CodexThreadID] != "" && !hasSession && c.ThreadID == ""
			},
		},
		{
			what: "a lone " + CodexSessionID + " being a main-thread call",
			holds: func(c threadCase) bool {
				_, hasThread := c.Headers[CodexThreadID]
				return c.Headers[CodexSessionID] != "" && !hasThread && c.ThreadID == ""
			},
		},
		{
			what: "precedence: the Claude list winning over a divergent Codex pair",
			holds: func(c threadCase) bool {
				tid, sid := c.Headers[CodexThreadID], c.Headers[CodexSessionID]
				agent := c.Headers[claudeHeader]
				return agent != "" && tid != "" && sid != "" && tid != sid && c.ThreadID == agent
			},
		},
	}

	for _, r := range rules {
		if !slices.ContainsFunc(cases, r.holds) {
			t.Errorf("no thread corpus case covers: %s\n"+
				"  Either a case was deleted, or a header spelling here drifted from the\n"+
				"  canonical vocabulary in %s.\n"+
				"  Add the case (or realign the spelling) rather than dropping the rule.",
				r.what, canonicalHome)
		}
	}
}
