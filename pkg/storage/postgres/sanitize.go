package postgres

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// Postgres SQLSTATEs raised when a JSONB column rejects the payload's own
// content rather than signalling an infrastructure fault.
const (
	// pgUnsupportedUnicodeEscape ("unsupported Unicode escape sequence") is
	// raised when a jsonb string contains an escaped NUL (the six-character
	// sequence backslash u 0 0 0 0), because NUL is not representable in a
	// jsonb string.
	pgUnsupportedUnicodeEscape = "22P05"

	// pgInvalidByteSequence ("invalid byte sequence for encoding") is raised
	// when the bytes are not valid UTF-8 in the server encoding.
	pgInvalidByteSequence = "22021"
)

var (
	// The only escapes a UTF-8 jsonb column rejects are an escaped NUL and a
	// UTF-16 surrogate; every surrogate code point (U+D800 to U+DFFF) begins
	// with a 'D' hex digit, so "backslash u D" (either case) is a cheap
	// superset trigger. Crucially this leaves benign escapes a JSON encoder
	// routinely emits (e.g. the ones for the less-than and ampersand
	// characters, or accented letters) on the allocation-free fast path
	// instead of forcing a decode/re-encode of a payload with nothing wrong.
	escNULSeq  = []byte{0x5c, 0x75, 0x30, 0x30, 0x30, 0x30} // backslash u 0 0 0 0
	escSurrUp  = []byte{0x5c, 0x75, 0x44}                   // backslash u D
	escSurrLow = []byte{0x5c, 0x75, 0x64}                   // backslash u d

	// replacementBytes is the UTF-8 encoding of U+FFFD REPLACEMENT CHARACTER.
	replacementBytes = []byte(string(rune(0xFFFD)))

	nulStr  = string(rune(0))
	fffdStr = string(rune(0xFFFD))
)

// sanitizeJSONB neutralizes the content a Postgres JSONB column rejects
// (SQLSTATE 22P05) — an escaped NUL, a raw NUL byte, and unpaired UTF-16
// surrogate escapes — replacing each with U+FFFD. A payload with none of
// these is returned unchanged and without allocation, so the raw layer's
// field-survival contract is unaffected; only an offending payload (which
// could not be stored faithfully anyway) is re-serialized via the JSON
// token stream, which copies every token — known field or not — through.
//
// The trigger is deliberately narrow: benign escapes a JSON encoder emits
// routinely (the less-than / ampersand / greater-than characters, accented
// letters as u-escapes) do NOT force the decode/re-encode — only an escaped
// NUL or a surrogate escape does. One thing this intentionally does NOT
// catch is a payload carrying raw invalid UTF-8 bytes with no triggering
// escape: that is rare, and it lands as a 22021 at the jsonb cast, which
// asContentError turns into a clean 422 rather than a corrupt-but-stored row.
//
// The heavy lifting is delegated to encoding/json/jsontext (the same
// syntactic layer merkle hashing already uses): its decoder, run with
// AllowInvalidUTF8, mangles invalid UTF-8 and unpaired surrogates to U+FFFD
// for us, so the only thing left to scrub by hand is NUL, which is valid
// UTF-8 and therefore slips past that check. Malformed JSON that the decoder
// cannot parse is returned unchanged for the jsonb cast (and asContentError)
// to reject on its own terms.
func sanitizeJSONB(in []byte) []byte {
	if len(in) == 0 {
		return in
	}

	hasNUL := bytes.IndexByte(in, 0x00) >= 0
	hasEscape := bytes.Contains(in, escNULSeq) ||
		bytes.Contains(in, escSurrUp) ||
		bytes.Contains(in, escSurrLow)
	if !hasNUL && !hasEscape {
		return in // fast path: nothing a jsonb cast rejects can be present
	}

	b := in
	if hasNUL {
		// A raw NUL is only ever illegal-inside-a-string, so replacing it at
		// the byte level both neutralizes it and keeps the token stream below
		// parseable. bytes.ReplaceAll allocates a fresh slice; in is untouched.
		b = bytes.ReplaceAll(b, []byte{0x00}, replacementBytes)
	}
	if !hasEscape {
		return b // only raw NUL was present; no escapes to inspect
	}

	opts := []jsontext.Options{jsontext.AllowInvalidUTF8(true), jsontext.AllowDuplicateNames(true)}
	dec := jsontext.NewDecoder(bytes.NewReader(b), opts...)
	var buf bytes.Buffer
	buf.Grow(len(b) + len(replacementBytes))
	enc := jsontext.NewEncoder(&buf, opts...)

	for {
		tok, err := dec.ReadToken()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return b // unparseable: leave it for the jsonb cast to reject
		}
		if tok.Kind() == '"' {
			tok = jsontext.String(scrubString(tok.String()))
		}
		if err := enc.WriteToken(tok); err != nil {
			return b
		}
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// scrubString replaces a decoded string's NUL runes with U+FFFD. Invalid
// UTF-8 is already mangled to U+FFFD by the AllowInvalidUTF8 decoder, but
// ToValidUTF8 is kept as a cheap belt-and-suspenders in case a caller ever
// feeds this a string from elsewhere.
func scrubString(s string) string {
	if !strings.ContainsRune(s, 0) && utf8.ValidString(s) {
		return s
	}
	s = strings.ReplaceAll(s, nulStr, fffdStr)
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, fffdStr)
	}
	return s
}

// asContentError wraps err with storage.ErrInvalidContent when it is a
// Postgres error whose SQLSTATE means "your content is unstorable"
// (22P05 / 22021), so boundaries can distinguish a bad payload from a
// storage outage via errors.Is. Any other error (including nil) is
// returned unchanged.
func asContentError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgUnsupportedUnicodeEscape, pgInvalidByteSequence:
			return fmt.Errorf("%w: %s (SQLSTATE %s)", storage.ErrInvalidContent, pgErr.Message, pgErr.Code)
		}
	}
	return err
}
