package postgres

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// kValue parses {"k":"..."} and returns the decoded k string.
func kValue(b []byte) string {
	var v struct {
		K string `json:"k"`
	}
	Expect(json.Unmarshal(b, &v)).To(Succeed())
	return v.K
}

// Escape/byte sequences are built from explicit byte values so this file
// carries no literal backslash-escapes that a source transform could
// misread. 0x5c is a backslash, 0x75 is the letter u, 0x30 is the digit 0.
var (
	// six bytes: backslash u 0 0 0 0  (an escaped NUL)
	escNUL = []byte{0x5c, 0x75, 0x30, 0x30, 0x30, 0x30}
	// backslash u D 8 0 0  (a lone high surrogate)
	escLoneHigh = []byte{0x5c, 0x75, 0x44, 0x38, 0x30, 0x30}
	// backslash u D C 0 0  (a lone low surrogate)
	escLoneLow = []byte{0x5c, 0x75, 0x44, 0x43, 0x30, 0x30}
	// backslash u D 8 3 D  backslash u D E 0 0  (a valid high+low pair)
	escValidPair = []byte{0x5c, 0x75, 0x44, 0x38, 0x33, 0x44, 0x5c, 0x75, 0x44, 0x45, 0x30, 0x30}
	// bytes 5c 5c 75 30 30 30 30 (an escaped backslash, then the text u0000)
	escBackslash = []byte{0x5c, 0x5c, 0x75, 0x30, 0x30, 0x30, 0x30}
	// backslash u 0 0 e 9  (an ordinary BMP escape)
	escBMP = []byte{0x5c, 0x75, 0x30, 0x30, 0x65, 0x39}
	// the UTF-8 encoding of U+FFFD
	uFFFD = []byte{0xEF, 0xBF, 0xBD}
)

// wrapStr embeds inner inside a minimal JSON object string value.
func wrapStr(inner []byte) []byte {
	out := append([]byte(`{"k":"`), inner...)
	return append(out, []byte(`"}`)...)
}

var _ = Describe("sanitizeJSONB", func() {
	It("replaces an escaped NUL with U+FFFD", func() {
		Expect(sanitizeJSONB(wrapStr(escNUL))).To(Equal(wrapStr(uFFFD)))
	})

	It("replaces a raw NUL byte with U+FFFD", func() {
		Expect(sanitizeJSONB(wrapStr([]byte{0x00}))).To(Equal(wrapStr(uFFFD)))
	})

	It("replaces a lone high surrogate escape", func() {
		Expect(sanitizeJSONB(wrapStr(escLoneHigh))).To(Equal(wrapStr(uFFFD)))
	})

	It("replaces a lone low surrogate escape", func() {
		Expect(sanitizeJSONB(wrapStr(escLoneLow))).To(Equal(wrapStr(uFFFD)))
	})

	It("preserves a valid surrogate pair (as its decoded rune)", func() {
		// The escape may be rewritten to the raw UTF-8 rune, but the decoded
		// value must be identical — U+1F600, the grinning face.
		Expect(kValue(sanitizeJSONB(wrapStr(escValidPair)))).To(Equal(string(rune(0x1F600))))
	})

	It("does not treat an escaped backslash followed by u0000 as a NUL escape", func() {
		// The decoded value is a literal backslash then the text u0000 — no NUL.
		Expect(kValue(sanitizeJSONB(wrapStr(escBackslash)))).To(Equal(string(rune(0x5c)) + "u0000"))
	})

	It("preserves an ordinary BMP escape (as its decoded rune)", func() {
		Expect(kValue(sanitizeJSONB(wrapStr(escBMP)))).To(Equal(string(rune(0x00e9)))) // é
	})

	It("preserves numbers, booleans, null, and array structure while scrubbing", func() {
		in := append([]byte(`{"n":42,"f":3.14,"big":10000000000,"b":true,"z":null,"arr":[1,2,3],"k":"x`), escNUL...)
		in = append(in, []byte(`"}`)...)

		out := sanitizeJSONB(in)
		var got map[string]any
		Expect(json.Unmarshal(out, &got)).To(Succeed())
		Expect(got["n"]).To(BeEquivalentTo(42))
		Expect(got["f"]).To(BeEquivalentTo(3.14))
		Expect(got["big"]).To(BeEquivalentTo(1e10))
		Expect(got["b"]).To(Equal(true))
		Expect(got["z"]).To(BeNil())
		Expect(got["arr"]).To(HaveLen(3))
		Expect(got["k"]).To(Equal("x" + string(rune(0xFFFD))))
	})

	It("returns a clean payload unchanged without allocating", func() {
		in := []byte(`{"hello":"world","n":42}`)
		out := sanitizeJSONB(in)
		Expect(out).To(Equal(in))
		// Same backing array: a clean payload is a true no-op.
		Expect(&out[0]).To(BeIdenticalTo(&in[0]))
	})

	It("handles nil and empty input", func() {
		Expect(sanitizeJSONB(nil)).To(BeNil())
		Expect(sanitizeJSONB([]byte{})).To(Equal([]byte{}))
	})

	It("rewrites multiple offenders in one payload", func() {
		in := append(append(append([]byte(`{"a":"`), escNUL...), []byte(`","b":"`)...), escLoneHigh...)
		in = append(in, []byte(`"}`)...)
		want := append(append(append([]byte(`{"a":"`), uFFFD...), []byte(`","b":"`)...), uFFFD...)
		want = append(want, []byte(`"}`)...)
		Expect(sanitizeJSONB(in)).To(Equal(want))
	})
})

var _ = Describe("asContentError", func() {
	It("wraps SQLSTATE 22P05 as ErrInvalidContent", func() {
		err := asContentError(&pgconn.PgError{Code: "22P05", Message: "unsupported Unicode escape sequence"})
		Expect(errors.Is(err, storage.ErrInvalidContent)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("22P05"))
	})

	It("wraps SQLSTATE 22021 as ErrInvalidContent", func() {
		err := asContentError(&pgconn.PgError{Code: "22021", Message: "invalid byte sequence"})
		Expect(errors.Is(err, storage.ErrInvalidContent)).To(BeTrue())
	})

	It("leaves an unrelated pg error unchanged", func() {
		orig := &pgconn.PgError{Code: "23505", Message: "unique_violation"}
		Expect(asContentError(orig)).To(BeIdenticalTo(orig))
	})

	It("leaves a non-pg error unchanged", func() {
		orig := errors.New("boom")
		Expect(asContentError(orig)).To(BeIdenticalTo(orig))
		Expect(errors.Is(asContentError(orig), storage.ErrInvalidContent)).To(BeFalse())
	})

	It("returns nil for nil", func() {
		Expect(asContentError(nil)).To(BeNil())
	})
})
