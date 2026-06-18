package spanembed

import (
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Internal test: splitParts is unexported. Specs register into the suite run by
// spanembed_suite_test.go, so this file deliberately does not call RunSpecs and
// imports ginkgo qualified (a dot-import would clash with this package's Report
// type).
var _ = ginkgo.Describe("splitParts", func() {
	ginkgo.It("returns nil for text too short to split", func() {
		Expect(splitParts("", 0)).To(BeNil())
		Expect(splitParts("a", 999999)).To(BeNil())
	})

	ginkgo.It("halves when the token count is unknown or near the budget", func() {
		parts := splitParts(strings.Repeat("a", 100), 0)
		Expect(parts).To(HaveLen(2))
		Expect(strings.Join(parts, "")).To(Equal(strings.Repeat("a", 100)))
	})

	ginkgo.It("splits into ceil(tokens/budget) pieces when the count is large", func() {
		// ~25k tokens against an 8k budget -> 4 pieces.
		parts := splitParts(strings.Repeat("x", 4000), 25000)
		Expect(parts).To(HaveLen(4))
		Expect(strings.Join(parts, "")).To(Equal(strings.Repeat("x", 4000)))
	})

	ginkgo.It("estimates tokens from length when the provider reports none (real OpenAI case)", func() {
		// 200k chars at ~4 chars/token -> ~50k tokens -> ceil(50000/8000) = 7
		// pieces in one shot, instead of falling back to repeated halving.
		text := strings.Repeat("x", 200000)
		parts := splitParts(text, 0)
		Expect(parts).To(HaveLen(7))
		Expect(strings.Join(parts, "")).To(Equal(text))
	})

	ginkgo.It("always reassembles to the original text exactly", func() {
		text := strings.Repeat("line of words\n", 500) + "tail"
		for _, tokens := range []int{0, 9000, 17000, 40000} {
			parts := splitParts(text, tokens)
			Expect(strings.Join(parts, "")).To(Equal(text), "tokens=%d", tokens)
			for _, p := range parts {
				Expect(p).NotTo(BeEmpty())
			}
		}
	})

	ginkgo.It("prefers to cut on a newline when one is near the even-division point", func() {
		// Two equal halves separated by a single newline near the midpoint.
		text := strings.Repeat("a", 50) + "\n" + strings.Repeat("b", 50)
		parts := splitParts(text, 0)
		Expect(parts).To(HaveLen(2))
		Expect(parts[0]).To(HaveSuffix("\n"))
		Expect(parts[1]).To(Equal(strings.Repeat("b", 50)))
	})
})
