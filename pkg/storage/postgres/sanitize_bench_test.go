package postgres

import (
	"fmt"
	"testing"
)

// buildPayload grows a JSON object whose "content" string repeats chunk until
// the whole payload is at least target bytes. chunk is spliced into a string
// value, so escape bytes in it land inside a JSON string.
func buildPayload(chunk []byte, target int) []byte {
	buf := []byte(`{"role":"assistant","content":"`)
	filler := []byte("the quick brown fox jumps over the lazy dog ")
	for len(buf) < target {
		buf = append(buf, filler...)
		buf = append(buf, chunk...)
	}
	return append(buf, []byte(`"}`)...)
}

var (
	// clean: plain ASCII, no escapes, no NUL — fast path.
	chunkClean = []byte("")
	// benign u-escapes an encoder routinely emits: < (< ) and é (u00e9).
	// After the narrowed trigger these stay on the fast path.
	chunkBenign = []byte{0x5c, 0x75, 0x30, 0x30, 0x33, 0x63, 0x5c, 0x75, 0x30, 0x30, 0x65, 0x39}
	// poison: an escaped NUL (backslash u 0 0 0 0): slow path, rewritten.
	chunkPoison = []byte{0x5c, 0x75, 0x30, 0x30, 0x30, 0x30}
	// lone high surrogate (backslash u D 8 0 0): slow path, rewritten.
	chunkSurrogate = []byte{0x5c, 0x75, 0x44, 0x38, 0x30, 0x30}
)

func benchSanitize(b *testing.B, chunk []byte, size int) {
	payload := buildPayload(chunk, size)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = sanitizeJSONB(payload)
	}
}

func BenchmarkSanitizeJSONB(b *testing.B) {
	for _, size := range []int{4 << 10, 480 << 10} { // 4KB typical, 480KB incident-sized
		b.Run(fmt.Sprintf("clean/%dKB", size>>10), func(b *testing.B) { benchSanitize(b, chunkClean, size) })
		b.Run(fmt.Sprintf("benign_esc/%dKB", size>>10), func(b *testing.B) { benchSanitize(b, chunkBenign, size) })
		b.Run(fmt.Sprintf("poison/%dKB", size>>10), func(b *testing.B) { benchSanitize(b, chunkPoison, size) })
		b.Run(fmt.Sprintf("surrogate/%dKB", size>>10), func(b *testing.B) { benchSanitize(b, chunkSurrogate, size) })
	}
}
