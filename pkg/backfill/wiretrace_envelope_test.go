package backfill

// Unit coverage for the header→envelope decode contract that
// sessionEnvelopeFromHeaders implements.
//
// The shared fixture corpus (envelope_fixtures_test.go) pins the cases
// that every consumer of the contract must agree on. This file pins the
// decode rules themselves at a finer grain — the `+` case in particular
// has no fixture, because a literal `+` needs no escaping and so cannot
// be told apart from its encoded form by looking at the header alone.
// It is exactly the case that separates PathUnescape from QueryUnescape.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// headersFrom builds the lookup closure sessionEnvelopeFromHeaders
// expects from a plain map.
func headersFrom(m map[string]string) func(string) string {
	return func(name string) string { return m[name] }
}

var _ = Describe("sessionEnvelopeFromHeaders decode contract", func() {
	// A harness-session-id is present in every case so the parser does
	// not short-circuit to a nil envelope.
	const sessionID = "11111111-1111-4111-8111-111111111111"

	base := func(extra map[string]string) func(string) string {
		m := map[string]string{
			"x-tapes-harness-id":         "claude",
			"x-tapes-harness-session-id": sessionID,
		}
		for k, v := range extra {
			m[k] = v
		}
		return headersFrom(m)
	}

	DescribeTable("cwd decodes to the logical path",
		func(header, expected string) {
			env := sessionEnvelopeFromHeaders(base(map[string]string{"x-tapes-cwd": header}))
			Expect(env).NotTo(BeNil())
			Expect(env.Cwd).To(Equal(expected))
		},
		Entry("cwd-unicode: non-ASCII path is recovered, not left escaped",
			"/Users/%E6%9D%BE%E6%9C%AC/code", "/Users/松本/code"),
		Entry("cwd-control-bytes-escaped: a decoded newline is refused outright",
			"/Users/matt%0Awith-injection:%20yes", ""),
		Entry("plain ASCII path passes through unchanged",
			"/Users/matt/code", "/Users/matt/code"),
		Entry("escaped space decodes to a space",
			"/Users/matt/my%20code", "/Users/matt/my code"),
		Entry("a literal + in a path stays a +, not a space",
			"/Users/matt/go+rust", "/Users/matt/go+rust"),
		Entry("a decoded DEL (0x7F) is refused",
			"/Users/matt%7Fcode", ""),
		Entry("a decoded NUL is refused",
			"/Users/matt%00code", ""),
		Entry("malformed percent-encoding falls back to the raw value",
			"/Users/matt/100%discount", "/Users/matt/100%discount"),
	)

	DescribeTable("session-name decodes on the same rules as cwd",
		func(header, expected string) {
			env := sessionEnvelopeFromHeaders(base(map[string]string{"x-tapes-session-name": header}))
			Expect(env).NotTo(BeNil())
			Expect(env.Name).To(Equal(expected))
		},
		// The regression this pins: the reader used QueryUnescape, which
		// turns a literal `+` into a space. PathUnescape leaves it alone,
		// which is what the producer's RFC 3986 escaping means.
		Entry("a literal + survives decoding (QueryUnescape would eat it)",
			"go+rust", "go+rust"),
		Entry("a + mixed with escapes still survives",
			"go+rust%20notes", "go+rust notes"),
		Entry("non-ASCII name is recovered",
			"caf%C3%A9", "café"),
		Entry("special chars decode as the corpus declares",
			"name%20with%20space%20%22quotes%22%20caf%C3%A9", `name with space "quotes" café`),
		Entry("a decoded newline is refused, same guard as cwd",
			"friendly%0Ax-injected: yes", ""),
		Entry("malformed percent-encoding falls back to the raw value",
			"50%off", "50%off"),
	)

	It("refuses control bytes independently per field", func() {
		env := sessionEnvelopeFromHeaders(base(map[string]string{
			"x-tapes-cwd":          "/Users/matt%0Aevil",
			"x-tapes-session-name": "perfectly-fine",
		}))
		Expect(env).NotTo(BeNil())
		// A poisoned cwd must not take an innocent session name with it.
		Expect(env.Cwd).To(BeEmpty())
		Expect(env.Name).To(Equal("perfectly-fine"))
	})

	It("leaves absent headers empty rather than decoding an empty string", func() {
		env := sessionEnvelopeFromHeaders(base(nil))
		Expect(env).NotTo(BeNil())
		Expect(env.Cwd).To(BeEmpty())
		Expect(env.Name).To(BeEmpty())
	})

	DescribeTable("hasControlRune identifies exactly the refused runes",
		func(s string, expected bool) {
			Expect(hasControlRune(s)).To(Equal(expected))
		},
		Entry("empty string", "", false),
		Entry("plain ASCII", "/Users/matt/code", false),
		Entry("multi-byte UTF-8 is not a control rune", "/Users/松本/code", false),
		Entry("space (0x20) is the first allowed byte", "a b", false),
		Entry("tilde (0x7E) is allowed", "a~b", false),
		Entry("NUL (0x00)", "a\x00b", true),
		Entry("newline (0x0A)", "a\nb", true),
		Entry("carriage return (0x0D)", "a\rb", true),
		Entry("tab (0x09)", "a\tb", true),
		Entry("unit separator (0x1F) is the last control", "a\x1fb", true),
		Entry("DEL (0x7F)", "a\x7fb", true),
	)
})
