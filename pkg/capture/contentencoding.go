package capture

// Decoding a captured body's Content-Encoding.
//
// This is the ingest-side half of a contract whose other half lives in the
// extproc capture adapter (telemetry/tapes-extproc, decodeBodyWithStats). The
// two must agree, and for a long time they did not: extproc decoded zstd,
// stacked encodings and truncated-but-salvageable streams, while ingest
// handled a single identity-or-gzip layer. The asymmetry was not a latent
// bug — it was load-bearing. extproc interlocks its raw-only lane against
// what it believes ingest can decode, so every encoding ingest could not read
// permanently fell back to dual-send. Codex traffic is zstd, so raw-only was
// unreachable for it by construction.
//
// Closing that gap is what this file is for. The rules below are extproc's
// rules, deliberately mirrored rather than reinvented, because the failure
// mode of a capture contract implemented twice is that the two copies drift
// while both stay green — the same divergence the envelope fixture corpus
// exists to kill for headers.
//
// It lives in pkg/capture rather than in ingest because content-encoding is a
// capture concern, not a projection one, and because two callers need it:
// ingest's raw-only reduction and the storage layer's read-time reduction
// recovery. Those were separate copies of a single-layer gzip decoder, which
// is how this class of drift starts.

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// MaxDecompressedBytes caps one decompressed body, per layer. It bounds a
// decompression bomb: the compressed bytes are already capped on the way in,
// but a few KiB of zstd expands to gigabytes if nothing stops it.
//
// 32 MiB matches extproc's cap exactly. It has to: a body extproc accepted and
// forwarded must not then be rejected here, or the raw lane would drop bytes
// the producer believed were safe to send.
const MaxDecompressedBytes = 32 << 20

// DecodeStats reports what had to be tolerated to produce the decoded bytes.
type DecodeStats struct {
	// Truncated marks a body recovered from a stream that ended early —
	// the decode succeeded only because partial output was accepted. The
	// caller decides what that is worth; it is surfaced rather than
	// swallowed so a salvaged reduction is never silently indistinguishable
	// from a clean one.
	Truncated bool
}

// DecodeContentEncoding returns body decoded per encoding, for handing to a
// reducer. Callers store the ENCODED bytes and decode only for reduction:
// re-compression is not byte-identical, so a column that promises "verbatim"
// has to keep what arrived.
//
// An unrecognized encoding is an error rather than a pass-through. Handing
// compressed bytes to a reducer that expects text yields a parse failure well
// away from the actual cause, and the bytes are still stored either way — so
// erroring here loses nothing and names the real problem.
//
// Truncated streams are salvaged rather than refused when they yielded any
// output at all, and the salvage is reported in DecodeStats. A capture that
// lost its tail is still most of a turn; refusing it would discard everything
// the stream did deliver in exchange for nothing. This mirrors extproc's
// response-side behavior — and it must, because extproc forwards the truncated
// compressed bytes as they arrived, so these are exactly the bytes ingest is
// handed.
func DecodeContentEncoding(body []byte, encoding string) ([]byte, DecodeStats, error) {
	ce := strings.TrimSpace(strings.ToLower(encoding))
	if ce == "" || ce == "identity" {
		return body, DecodeStats{}, nil
	}

	// Layers are peeled right-to-left: RFC 9110 §8.4 lists encodings in the
	// order they were applied, so the last one listed is the outermost and
	// comes off first.
	layers := splitContentEncoding(ce)
	current := body
	var stats DecodeStats
	for i := len(layers) - 1; i >= 0; i-- {
		decoded, layerStats, err := decodeOneLayer(current, layers[i])
		if err != nil {
			// Name the full header as well as the failing token: with
			// stacked encodings the token alone does not say which
			// header produced it.
			return nil, DecodeStats{}, fmt.Errorf("content-encoding %q: %w", encoding, err)
		}
		stats.Truncated = stats.Truncated || layerStats.Truncated
		current = decoded
	}
	return current, stats, nil
}

// splitContentEncoding parses a Content-Encoding value into the layers to
// undo, dropping whitespace and identity tokens.
//
// identity is dropped rather than handled as a layer because it means "no
// transformation was applied": "gzip, identity" is one layer of gzip, and
// "identity, identity" is zero layers, not an error.
func splitContentEncoding(ce string) []string {
	parts := strings.Split(ce, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == "identity" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// decodeOneLayer undoes a single content-coding. The caller has already
// lower-cased and trimmed the token.
func decodeOneLayer(body []byte, encoding string) ([]byte, DecodeStats, error) {
	switch encoding {
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, DecodeStats{}, fmt.Errorf("gzip reader init: %w", err)
		}
		defer zr.Close()
		return readCapped(zr, "gzip")

	case "zstd":
		// Bound the decoder itself as well as its output. The window and
		// memory limits let a hostile frame be refused before it is
		// expanded, rather than after MaxDecompressedBytes of it exists.
		zr, err := zstd.NewReader(
			bytes.NewReader(body),
			zstd.WithDecoderMaxMemory(MaxDecompressedBytes),
			zstd.WithDecoderMaxWindow(MaxDecompressedBytes),
		)
		if err != nil {
			return nil, DecodeStats{}, fmt.Errorf("zstd reader init: %w", err)
		}
		defer zr.Close()
		return readCapped(zr, "zstd")

	default:
		// deflate and br land here deliberately: no capture path emits
		// them, and a decoder for an encoding nothing produces is
		// untested code that only exists to be wrong later.
		return nil, DecodeStats{}, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

// readCapped drains r under the size cap, applying the salvage rule.
//
// The cap is checked before the error is, so an oversize stream that also
// ended early is refused rather than salvaged — otherwise the bomb guard would
// be bypassable by truncating the bomb.
func readCapped(r io.Reader, kind string) ([]byte, DecodeStats, error) {
	// Read one byte past the cap so that a body of exactly
	// MaxDecompressedBytes is accepted and one byte more is unambiguously
	// over, without a second read to disambiguate.
	decoded, err := io.ReadAll(io.LimitReader(r, MaxDecompressedBytes+1))
	if len(decoded) > MaxDecompressedBytes {
		return nil, DecodeStats{}, fmt.Errorf("%s decoded body exceeds %d bytes", kind, MaxDecompressedBytes)
	}
	if err != nil {
		// Salvage on exactly two conjuncts: the stream ended early, and
		// it produced something first. A corrupt header, a bad checksum,
		// or an early end that yielded nothing are all hard failures —
		// there is no partial turn in them to keep.
		if errors.Is(err, io.ErrUnexpectedEOF) && len(decoded) > 0 {
			return decoded, DecodeStats{Truncated: true}, nil
		}
		return nil, DecodeStats{}, fmt.Errorf("%s read: %w", kind, err)
	}
	return decoded, DecodeStats{}, nil
}
