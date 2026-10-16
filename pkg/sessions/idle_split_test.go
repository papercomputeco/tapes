package sessions_test

import (
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// assignAll places turns in the given arrival order, recording each split
// the way the store does, and returns each turn's session key by capture
// instant.
func assignAll(conversation string, gap time.Duration, arrivals []time.Time) map[time.Time]string {
	var splits []sessions.SyntheticSplit
	keys := map[time.Time]string{}
	for _, at := range arrivals {
		split, created := sessions.AssignSyntheticSplit(conversation, splits, at, gap)
		if created {
			splits = append(splits, split)
		} else {
			for i := range splits {
				if splits[i].HarnessSessionID == split.HarnessSessionID {
					splits[i] = split
				}
			}
		}
		keys[at] = split.HarnessSessionID
	}
	return keys
}

// groups turns keys into sets of capture instants, so two
// assignments can be compared by grouping regardless of the ids they chose.
func groups(keys map[time.Time]string) map[string][]time.Time {
	out := map[string][]time.Time{}
	for at, key := range keys {
		out[key] = append(out[key], at)
	}
	return out
}

var _ = Describe("AssignSyntheticSplit", func() {
	const conversation = "0123456789abcdef"
	gap := 30 * time.Minute
	t0 := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	at := func(minutes int) time.Time { return t0.Add(time.Duration(minutes) * time.Minute) }

	It("keys the conversation's first split by the bare conversation key", func() {
		split, created := sessions.AssignSyntheticSplit(conversation, nil, t0, gap)
		Expect(created).To(BeTrue())
		Expect(split.HarnessSessionID).To(Equal(conversation))
	})

	It("keeps turns within the gap in one session", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(10), at(39), at(69)})
		Expect(groups(keys)).To(HaveLen(1))
		Expect(keys[at(69)]).To(Equal(conversation))
	})

	It("opens a new session after the gap, keyed by its start", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(10), at(41), at(50)})
		Expect(keys[at(0)]).To(Equal(conversation))
		Expect(keys[at(10)]).To(Equal(conversation))
		Expect(keys[at(41)]).To(Equal(conversation + "-20261013T094100Z"))
		Expect(keys[at(50)]).To(Equal(keys[at(41)]))
	})

	It("measures the gap from the previous turn, not the session start", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(25), at(50), at(75), at(100)})
		Expect(groups(keys)).To(HaveLen(1))
	})

	It("splits at exactly the gap boundary only when the gap is exceeded", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(30)})
		Expect(groups(keys)).To(HaveLen(1))
		keys = assignAll(conversation, gap, []time.Time{at(0), t0.Add(gap + time.Second)})
		Expect(groups(keys)).To(HaveLen(2))
	})

	It("is idempotent: re-assigning a placed turn returns its split unchanged", func() {
		first, _ := sessions.AssignSyntheticSplit(conversation, nil, at(0), gap)
		later, created := sessions.AssignSyntheticSplit(conversation, []sessions.SyntheticSplit{first}, at(45), gap)
		Expect(created).To(BeTrue())
		splits := []sessions.SyntheticSplit{first, later}
		for _, turn := range []time.Time{at(0), at(45)} {
			again, created := sessions.AssignSyntheticSplit(conversation, splits, turn, gap)
			Expect(created).To(BeFalse())
			Expect(again).To(BeElementOf(splits))
		}
	})

	It("lands a late turn in the split its capture time falls into", func() {
		inOrder := assignAll(conversation, gap, []time.Time{at(0), at(20), at(90), at(100)})
		// at(20) arrives last, after the conversation moved on.
		late := assignAll(conversation, gap, []time.Time{at(0), at(90), at(100), at(20)})
		Expect(late).To(Equal(inOrder))
	})

	It("lands a late turn just before a burst in that burst", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(90), at(100), at(70)})
		Expect(keys[at(70)]).To(Equal(keys[at(90)]))
		Expect(keys[at(70)]).NotTo(Equal(keys[at(0)]))
	})

	It("groups any arrival order of separated bursts identically", func() {
		turns := []time.Time{at(0), at(5), at(12), at(80), at(85), at(200), at(201), at(229)}
		want := groups(assignAll(conversation, gap, turns))
		Expect(want).To(HaveLen(3))
		rng := rand.New(rand.NewPCG(1, 2))
		for range 50 {
			shuffled := append([]time.Time(nil), turns...)
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			got := groups(assignAll(conversation, gap, shuffled))
			Expect(got).To(HaveLen(len(want)))
			for _, members := range got {
				Expect(want).To(ContainElement(ConsistOf(members)))
			}
		}
	})

	It("joins the earlier split when a late turn bridges two", func() {
		keys := assignAll(conversation, gap, []time.Time{at(0), at(50), at(25)})
		Expect(keys[at(25)]).To(Equal(keys[at(0)]))
		Expect(keys[at(50)]).NotTo(Equal(keys[at(0)]))
	})

	It("never splits when the gap is disabled", func() {
		keys := assignAll(conversation, 0, []time.Time{at(0), at(600), at(6000)})
		Expect(groups(keys)).To(HaveLen(1))
		Expect(groups(keys)).To(HaveKeyWithValue(conversation, ConsistOf(at(0), at(600), at(6000))))
	})
})

var _ = Describe("ValidateIdleGap", func() {
	It("accepts a disabled gap and any gap of at least a second", func() {
		Expect(sessions.ValidateIdleGap(0)).To(Succeed())
		Expect(sessions.ValidateIdleGap(-time.Minute)).To(Succeed())
		Expect(sessions.ValidateIdleGap(sessions.MinIdleGap)).To(Succeed())
		Expect(sessions.ValidateIdleGap(sessions.DefaultIdleGap)).To(Succeed())
	})

	It("rejects a positive gap below a second", func() {
		err := sessions.ValidateIdleGap(500 * time.Millisecond)
		Expect(err).To(MatchError(ContainSubstring("below the minimum of 1s")))
	})

	// Split ids name their start at second resolution. At the minimum gap,
	// any arrival order of turns spaced just over the gap apart — the
	// densest that still opens a split per turn — must give every split
	// its own id, or two splits would merge under one key.
	It("gives every split a distinct id at the minimum gap", func() {
		const conversation = "be436c07b169c409"
		base := time.Date(2026, 10, 13, 17, 0, 0, 999_000_000, time.UTC)
		const n = 40
		turns := make([]time.Time, 0, n)
		for i := range n {
			turns = append(turns, base.Add(time.Duration(i)*(sessions.MinIdleGap+time.Millisecond)))
		}
		rng := rand.New(rand.NewPCG(7, 11))
		for range 25 {
			shuffled := append([]time.Time(nil), turns...)
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			Expect(groups(assignAll(conversation, sessions.MinIdleGap, shuffled))).To(HaveLen(len(turns)))
		}
	})
})
