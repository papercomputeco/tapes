package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// The equivalence check PCC-1029 calls for, without a clearing.
//
// A dual-send envelope carries both halves of the same turn: the adapter's
// reduction, and the verbatim bytes it reduced from. That makes the two
// reducers comparable offline — run the server-side reduction over the bytes
// in the envelope and it must agree with the reduction sitting next to them.
//
// Why this matters beyond "a test passes": the argument for making extproc a
// dumb adapter is that one reducer, server-side, for every capture path means
// two paths *cannot* reduce differently. That argument is only sound if the
// server-side reduction of the bytes we ship reproduces what we compute today.
// If it does not, raw-only silently changes what users see.
//
// What is deliberately NOT mocked: the envelope comes from the real processor
// state machine driven over the committed wire recordings, so the bytes under
// test are the bytes the deployed path would actually send.

// ingestDecodeContentEncoding mirrors tapes' ingest.decodeContentEncoding.
//
// It is intentionally narrower than extproc's own decoder — identity and gzip,
// one layer, no zstd and no stacked encodings — because the point is to
// reproduce what the SERVER can do with the bytes, not what we can. Using
// extproc's decoder here would hide exactly the asymmetry the raw-only
// interlock exists to handle.
func ingestDecodeContentEncoding(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer zr.Close()
		out, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("gzip decode: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported content-encoding %q", encoding)
	}
}

// dualSendEnvelope is the parsed view of one dispatched envelope. Fields are
// typed loosely so a missing key is visible rather than defaulted.
type dualSendEnvelope struct {
	Provider            string            `json:"provider"`
	Request             json.RawMessage   `json:"request"`
	Response            *llm.ChatResponse `json:"response"`
	RawResponse         []byte            `json:"raw_response"`
	RawResponseEncoding string            `json:"raw_response_encoding"`
	Meta                struct {
		ContentType string `json:"content_type"`
	} `json:"meta"`
}

// captureDualSend drives one recorded turn through the processor in dual mode
// and returns the envelope tapes-ingest received.
func captureDualSend(t *testing.T, b bundle) dualSendEnvelope {
	t.Helper()
	return captureWithMode(t, b, RawResponseDual)
}

// captureWithMode drives one recorded turn through the real processor state
// machine in the given mode and returns the envelope tapes-ingest received.
func captureWithMode(t *testing.T, b bundle, mode RawResponseMode) dualSendEnvelope {
	t.Helper()

	var (
		mu     sync.Mutex
		bodies [][]byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	proc, err := NewProcessor(Config{
		IngestURL:       srv.URL,
		MaxInflight:     4,
		RawResponseMode: mode,
	})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())

	stream := &fakeStream{
		ctx: context.Background(),
		toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{
				":method":      b.req.Method,
				":path":        b.req.Path,
				"x-request-id": b.name,
			}),
			reqBodyReq(b.req.Body, true),
			respHeaderReqWithEncoding(strconv.Itoa(b.meta.Status), b.meta.ContentType, b.meta.ContentEncoding),
			respBodyReq(b.resp, true),
		},
	}
	if err := proc.Process(stream); err != nil {
		t.Fatalf("Process: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("ingest received %d envelopes, want 1", len(bodies))
	}

	var env dualSendEnvelope
	if err := json.Unmarshal(bodies[0], &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return env
}

// TestDualSendReductionEquivalence is the gate: for every committed recording,
// the reduction ingest would compute from the bytes in the envelope must equal
// the reduction extproc put beside them.
func TestDualSendReductionEquivalence(t *testing.T) {
	ctx := context.Background()

	for _, b := range loadRecordings(t) {
		t.Run(b.name, func(t *testing.T) {
			env := captureDualSend(t, b)

			// Precondition: this is actually a dual-send.
			if len(env.RawResponse) == 0 {
				t.Fatalf("envelope carried no raw_response; dual-send did not attach bytes")
			}
			if env.Response == nil {
				t.Fatalf("envelope carried no reduction; this is not a dual-send")
			}

			// The bytes must be verbatim — still compressed, exactly as
			// the upstream framed them. A decoded or re-encoded column
			// would defeat the entire point of the raw lane.
			if !bytes.Equal(env.RawResponse, b.resp) {
				t.Fatalf("raw_response is not verbatim: got %d bytes, recording has %d",
					len(env.RawResponse), len(b.resp))
			}
			if env.RawResponseEncoding != b.meta.ContentEncoding {
				t.Fatalf("raw_response_encoding=%q, recording content_encoding=%q",
					env.RawResponseEncoding, b.meta.ContentEncoding)
			}

			// --- the server side, reproduced exactly ---
			decoded, err := ingestDecodeContentEncoding(env.RawResponse, env.RawResponseEncoding)
			if err != nil {
				t.Fatalf("ingest could not decode encoding=%q: %v — raw-only would store unreducible bytes",
					env.RawResponseEncoding, err)
			}

			reducer := capture.NewAnthropicReducer()
			serverSide, err := reducer.Reduce(ctx,
				bytes.NewReader(env.Request),
				bytes.NewReader(decoded),
				env.Meta.ContentType,
			)
			if err != nil {
				t.Fatalf("server-side reduce failed: %v", err)
			}

			// --- the legitimate differences, named ---
			//
			// Exactly two fields cannot survive moving the reduction to
			// the server, and both are time, not content. They are
			// asserted individually and then zeroed, so the equality
			// check below covers everything else strictly. If a third
			// difference ever appears it fails here rather than being
			// absorbed by a loose comparison.
			adapter := *env.Response

			// 1. Usage.TotalDurationNs — the proxy-measured wall clock.
			// extproc is the only party that knows it (it watched the
			// stream); a reduction of stored bytes cannot reproduce it.
			// Raw-only therefore drops it from Usage. The value itself
			// is not lost: it still rides on meta.elapsed_seconds.
			if adapter.Usage == nil {
				t.Fatalf("adapter reduction carried no Usage; the duration stamp should have created it")
			}
			if adapter.Usage.TotalDurationNs == 0 {
				t.Errorf("adapter reduction has TotalDurationNs=0; the wall-clock stamp did not fire")
			}
			if serverSide.Usage != nil && serverSide.Usage.TotalDurationNs != 0 {
				t.Errorf("server-side reduction produced TotalDurationNs=%d; it cannot know the wall clock",
					serverSide.Usage.TotalDurationNs)
			}
			normalizedUsage := *adapter.Usage
			normalizedUsage.TotalDurationNs = 0
			adapter.Usage = &normalizedUsage

			// 2. CreatedAt — stamped by the reducer at reduction time,
			// not read off the wire. Two reductions of identical bytes
			// at different instants differ by construction, so this
			// field says when the reduction ran, never when the turn
			// happened. Under raw-only it becomes ingest time rather
			// than capture time — a real semantic shift for anything
			// reading it as a turn timestamp.
			if adapter.CreatedAt.IsZero() {
				t.Errorf("adapter reduction has a zero CreatedAt")
			}
			if serverSide.CreatedAt.IsZero() {
				t.Errorf("server-side reduction has a zero CreatedAt")
			}
			adapter.CreatedAt = time.Time{}
			serverSideCopy := *serverSide
			serverSideCopy.CreatedAt = time.Time{}

			if diff := jsonDiff(t, &adapter, &serverSideCopy); diff != "" {
				t.Fatalf("server-side reduction differs from the adapter's for the same bytes:\n%s", diff)
			}
		})
	}
}

// TestDualSendRawOnlyParity pins the other half of the transition: the bytes a
// raw-only envelope ships are the same bytes a dual-send would ship. Mode
// selects what rides ALONGSIDE the bytes, never the bytes themselves — so
// equivalence proven under dual carries over to raw-only.
func TestDualSendRawOnlyParity(t *testing.T) {
	for _, b := range loadRecordings(t) {
		t.Run(b.name, func(t *testing.T) {
			dual := captureDualSend(t, b)

			rawOnly := captureRawOnly(t, b)
			if rawOnly.Response != nil {
				t.Fatalf("raw-only envelope carried a reduction")
			}
			if !bytes.Equal(rawOnly.RawResponse, dual.RawResponse) {
				t.Fatalf("raw-only shipped different bytes than dual: %d vs %d",
					len(rawOnly.RawResponse), len(dual.RawResponse))
			}
			if rawOnly.RawResponseEncoding != dual.RawResponseEncoding {
				t.Fatalf("raw-only encoding=%q, dual encoding=%q",
					rawOnly.RawResponseEncoding, dual.RawResponseEncoding)
			}
		})
	}
}

// captureRawOnly is captureDualSend in RawResponseRaw mode.
func captureRawOnly(t *testing.T, b bundle) dualSendEnvelope {
	t.Helper()
	return captureWithMode(t, b, RawResponseRaw)
}

func jsonDiff(t *testing.T, want, got any) string {
	t.Helper()
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	if bytes.Equal(wantJSON, gotJSON) {
		return ""
	}
	return fmt.Sprintf("adapter:\n%s\n\nserver-side:\n%s", wantJSON, gotJSON)
}
