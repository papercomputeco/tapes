package extproc

import (
	"bytes"
	"compress/gzip"
	"math/rand"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func gzipped(t *testing.T, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write([]byte(body)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func zstdCompressed(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("zstd write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}

func TestDecodeResponseBody(t *testing.T) {
	tests := []struct {
		name     string
		body     []byte
		encoding string
		wantBody string
		wantErr  bool
	}{
		{
			name:     "empty encoding is pass-through",
			body:     []byte("event: message_start\n"),
			encoding: "",
			wantBody: "event: message_start\n",
		},
		{
			name:     "identity encoding is pass-through",
			body:     []byte("hello"),
			encoding: "identity",
			wantBody: "hello",
		},
		{
			name:     "case-insensitive identity",
			body:     []byte("hello"),
			encoding: "  Identity  ",
			wantBody: "hello",
		},
		{
			name:     "gzip decodes",
			body:     gzipped(t, "event: message_start\ndata: {\"id\":\"msg_X\"}\n"),
			encoding: "gzip",
			wantBody: "event: message_start\ndata: {\"id\":\"msg_X\"}\n",
		},
		{
			name:     "x-gzip alias decodes",
			body:     gzipped(t, "hello"),
			encoding: "x-gzip",
			wantBody: "hello",
		},
		{
			name:     "case-insensitive gzip",
			body:     gzipped(t, "hello"),
			encoding: "GZIP",
			wantBody: "hello",
		},
		{
			name:     "gzip with comma-separated layers",
			body:     gzipped(t, "hello"),
			encoding: "gzip, identity",
			wantBody: "hello",
		},
		{
			name:     "zstd decodes",
			body:     zstdCompressed(t, []byte(`{"model":"gpt-5.4","stream":true}`)),
			encoding: "zstd",
			wantBody: `{"model":"gpt-5.4","stream":true}`,
		},
		{
			name:     "zstd with corrupt body errors",
			body:     []byte{0x28, 0xb5, 0x2f, 0xfd, 0xff},
			encoding: "zstd",
			wantErr:  true,
		},
		{
			name:     "unknown encoding errors with the encoding named",
			body:     []byte("x"),
			encoding: "brotli-experimental",
			wantErr:  true,
		},
		{
			name:     "gzip with corrupt body errors",
			body:     []byte{0x1f, 0x8b, 0x08, 0xff, 0xff, 0xff},
			encoding: "gzip",
			wantErr:  true,
		},
		{
			name:     "empty body + identity is zero-length pass-through",
			body:     []byte{},
			encoding: "identity",
			wantBody: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeResponseBody(tc.body, tc.encoding)
			if tc.wantErr {
				if err == nil {
					t.Errorf("want error, got nil (decoded=%q)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tc.wantBody {
				t.Errorf("body mismatch:\n  got:  %q\n  want: %q", got, tc.wantBody)
			}
		})
	}
}

func TestDecodeResponseBodySalvagesTruncatedGzip(t *testing.T) {
	body := gzipped(t, "event: message_start\ndata: {}\n\nevent: message_stop\ndata: {}\n\n")
	truncated := body[:len(body)-8]

	got, err := decodeResponseBody(truncated, "gzip")
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(string(got), "message_stop") {
		t.Fatalf("decoded body did not contain message_stop: %q", got)
	}

	got, stats, err := decodeResponseBodyWithStats(truncated, "gzip")
	if err != nil {
		t.Fatalf("decode with stats: %v", err)
	}
	if !stats.truncated {
		t.Fatal("expected truncated decode stats")
	}
	if !strings.Contains(string(got), "message_stop") {
		t.Fatalf("decoded body did not contain message_stop: %q", got)
	}
}

func TestDecodeZstdTruncationIsSalvagedOnlyForResponses(t *testing.T) {
	plain := make([]byte, 256<<10)
	if _, err := rand.New(rand.NewSource(42)).Read(plain); err != nil {
		t.Fatalf("generate body: %v", err)
	}
	compressed := zstdCompressed(t, plain)
	truncated := compressed[:len(compressed)-1]

	got, stats, err := decodeResponseBodyWithStats(truncated, "zstd")
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !stats.truncated {
		t.Fatal("expected truncated decode stats")
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decoded %d bytes, want %d", len(got), len(plain))
	}

	if _, err := decodeRequestBody(truncated, "zstd"); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("expected truncated request error, got %v", err)
	}
}

func TestDecodeResponseBodyPreservesUnderlyingBytesOnPassthrough(t *testing.T) {
	// Identity/empty encoding should return the underlying slice without
	// copying — protects against accidental allocation in the hot path
	// for the common (uncompressed) case.
	body := []byte("event: message_start\n")
	got, err := decodeResponseBody(body, "")
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if &got[0] != &body[0] {
		t.Errorf("identity decode should pass the underlying slice through, but the address changed")
	}
}

func TestDecodeRejectsOversizedGzip(t *testing.T) {
	// A decompression bomb: a tiny gzip payload that would expand past
	// maxDecompressedBytes. Compressing maxDecompressedBytes+1 zero
	// bytes produces ~32 KB of gzip, decoding to >32 MiB — the size cap
	// must reject this with an error rather than letting ReadAll allocate.
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(make([]byte, maxDecompressedBytes+1)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err := decodeResponseBody(buf.Bytes(), "gzip")
	if err == nil {
		t.Fatalf("expected size-cap error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error naming the size cap, got %v", err)
	}
}

func TestDecodeRejectsOversizedZstd(t *testing.T) {
	compressed := zstdCompressed(t, make([]byte, maxDecompressedBytes+1))

	_, err := decodeRequestBody(compressed, "zstd")
	if err == nil {
		t.Fatal("expected size-cap error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") && !strings.Contains(err.Error(), "memory") {
		t.Errorf("expected error naming the size or decoder memory cap, got %v", err)
	}
}

func TestDecodeOnRealisticGzipSize(t *testing.T) {
	// Synthesize a multi-event SSE body that mirrors the size shape
	// we see in production captures (~5 KB plaintext), gzip it, and
	// verify we get the same bytes back through decodeResponseBody.
	var sb strings.Builder
	sb.WriteString("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_X\",\"role\":\"assistant\"}}\n\n")
	for i := 0; i < 50; i++ {
		sb.WriteString("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"some text content here\"}}\n\n")
	}
	sb.WriteString("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	plain := sb.String()

	compressed := gzipped(t, plain)
	if len(compressed) >= len(plain) {
		t.Fatalf("expected gzip to shrink the payload, but %d >= %d", len(compressed), len(plain))
	}

	got, err := decodeResponseBody(compressed, "gzip")
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got) != plain {
		t.Errorf("round-trip mismatch: got %d bytes, want %d", len(got), len(plain))
	}
}
