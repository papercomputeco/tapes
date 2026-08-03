package capture_test

// These cases mirror the extproc decoder's own suite (tapes-extproc
// decode_test.go) case for case. That is the point of them: the two decoders
// are one contract implemented twice, and a contract implemented twice drifts
// unless both copies are pinned to the same table.

import (
	"bytes"
	"compress/gzip"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/klauspost/compress/zstd"

	"github.com/papercomputeco/tapes/pkg/capture"
)

func gzipped(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(b)
	Expect(err).NotTo(HaveOccurred())
	Expect(w.Close()).To(Succeed())
	return buf.Bytes()
}

func zstdCompressed(b []byte) []byte {
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	Expect(err).NotTo(HaveOccurred())
	_, err = w.Write(b)
	Expect(err).NotTo(HaveOccurred())
	Expect(w.Close()).To(Succeed())
	return buf.Bytes()
}

var _ = Describe("DecodeContentEncoding", func() {
	const plain = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

	decode := func(body []byte, enc string) ([]byte, capture.DecodeStats) {
		out, stats, err := capture.DecodeContentEncoding(body, enc)
		Expect(err).NotTo(HaveOccurred())
		return out, stats
	}

	It("passes an empty or identity encoding through untouched", func() {
		for _, enc := range []string{"", "identity", "  Identity  "} {
			out, stats := decode([]byte(plain), enc)
			Expect(string(out)).To(Equal(plain), "encoding %q", enc)
			Expect(stats.Truncated).To(BeFalse())
		}
	})

	It("does not copy on the identity path", func() {
		// Pinned because the identity path is the common one: a capture
		// that pays a full copy per turn to do nothing is a cost with no
		// corresponding behavior.
		body := []byte(plain)
		out, _ := decode(body, "")
		Expect(&out[0]).To(Equal(&body[0]))
	})

	It("decodes gzip under every spelling", func() {
		for _, enc := range []string{"gzip", "x-gzip", "GZIP", " gzip "} {
			out, stats := decode(gzipped([]byte(plain)), enc)
			Expect(string(out)).To(Equal(plain), "encoding %q", enc)
			Expect(stats.Truncated).To(BeFalse())
		}
	})

	It("decodes zstd", func() {
		out, stats := decode(zstdCompressed([]byte(plain)), "zstd")
		Expect(string(out)).To(Equal(plain))
		Expect(stats.Truncated).To(BeFalse())
	})

	It("treats identity as no layer at all within a list", func() {
		// "gzip, identity" is one layer of gzip, not gzip plus a second
		// encoding — identity names the absence of a transformation.
		out, _ := decode(gzipped([]byte(plain)), "gzip, identity")
		Expect(string(out)).To(Equal(plain))

		out, _ = decode([]byte(plain), "identity, identity")
		Expect(string(out)).To(Equal(plain))
	})

	It("unwraps stacked encodings outermost first", func() {
		// RFC 9110 §8.4 lists encodings in the order they were applied,
		// so "gzip, zstd" is zstd(gzip(body)) and zstd comes off first.
		// Decoding left-to-right would fail on the very first layer,
		// which is the only reason this asymmetry is testable at all.
		stacked := zstdCompressed(gzipped([]byte(plain)))
		out, stats := decode(stacked, "gzip, zstd")
		Expect(string(out)).To(Equal(plain))
		Expect(stats.Truncated).To(BeFalse())
	})

	It("salvages a truncated gzip body and reports it", func() {
		// Losing the 8-byte trailer (CRC32 + ISIZE) is what a capture cut
		// off mid-flight looks like: every payload byte arrived and only
		// the integrity footer is missing.
		full := gzipped([]byte(plain))
		out, stats := decode(full[:len(full)-8], "gzip")
		Expect(string(out)).To(Equal(plain))
		Expect(stats.Truncated).To(BeTrue())
	})

	It("salvages a truncated zstd body and reports it", func() {
		body := make([]byte, 256<<10)
		rng := rand.New(rand.NewSource(1))
		for i := range body {
			body[i] = byte(rng.Intn(26) + 'a')
		}
		full := zstdCompressed(body)
		out, stats := decode(full[:len(full)-1], "zstd")
		Expect(len(out)).To(BeNumerically(">", 0))
		Expect(stats.Truncated).To(BeTrue())
	})

	It("refuses a corrupt body rather than salvaging nothing", func() {
		// Salvage needs output to salvage. A body that decoded to zero
		// bytes is a failure, not a partial success, or every corrupt
		// capture would land as a silently empty turn.
		for _, tc := range []struct{ body, enc string }{
			{"\x28\xb5\x2f\xfd\xff", "zstd"},
			{"\x1f\x8b\x08\xff\xff\xff", "gzip"},
		} {
			_, _, err := capture.DecodeContentEncoding([]byte(tc.body), tc.enc)
			Expect(err).To(HaveOccurred(), "encoding %q", tc.enc)
		}
	})

	It("names both the header and the offending token on an unknown encoding", func() {
		_, _, err := capture.DecodeContentEncoding([]byte(plain), "gzip, brotli-experimental")
		Expect(err).To(HaveOccurred())
		// With a stacked header the token alone does not say which
		// header produced it, and the header alone does not say which
		// layer failed.
		Expect(err.Error()).To(ContainSubstring("brotli-experimental"))
		Expect(err.Error()).To(ContainSubstring("gzip, brotli-experimental"))
	})

	It("rejects a decompression bomb", func() {
		_, _, err := capture.DecodeContentEncoding(
			gzipped(make([]byte, capture.MaxDecompressedBytes+1)), "gzip")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("exceeds"))
	})

	It("accepts a body of exactly the cap", func() {
		// The cap is inclusive; the reader takes one byte past it purely
		// so that "at the limit" and "over it" are distinguishable
		// without a second read.
		out, _ := decode(gzipped(make([]byte, capture.MaxDecompressedBytes)), "gzip")
		Expect(out).To(HaveLen(capture.MaxDecompressedBytes))
	})
})
