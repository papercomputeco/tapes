package extproc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/papercomputeco/tapes/pkg/capture"
)

// Drive the deployed-equivalent Anthropic reducer and the extproc state
// machine against real wire recordings. This file is the unit-test replay
// path — the cluster-side replay server in cmd/replay-server/ exercises the
// same bundles end-to-end through Envoy + ext_proc, but the unit path is the
// fastest way to confirm the reducer accepts real bytes the same way it
// accepts the synthetic ones in reducer_adversarial_test.go.
//
// # Where the bundles come from
//
// recordingsDir is the fixture-grade L1 corpus governed by fixtures/README.md
// and indexed by fixtures/manifest.json. It used to be a vendored copy kept in
// step by scripts/sync-wire-recordings.sh, because the corpus lived in another
// repository; the adapter and the corpus share a repository now, so the copy
// and the script that policed it are both gone and this reads the governed
// bytes directly. Because the corpus is committed, these tests always run —
// they do not skip, and a regression in the reducer or the decode shim fails
// CI. It is also the same corpus pkg/backfill's scrub gate polices, so a leak
// is caught once for every consumer rather than once per copy.
//
// Do NOT drop ad-hoc captures of your own traffic in there. Those go in
// wireCapturesDir, which is gitignored and drives the lenient diagnostic test
// at the bottom of this file; real prompts and model responses must not be
// committed. The bar a recording must clear to join the corpus — clean-room,
// credential- and PII-free in headers and bodies, reviewed — is stated in
// fixtures/README.md.

// recordingsDir locates fixtures/recordings relative to this file, so the test
// does not depend on the working directory `go test` was invoked from.
func recordingsDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "fixtures", "recordings")
}

// wireCapturesDir is the gitignored drop point for your own captures.
func wireCapturesDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "testdata", "wire-captures")
}

// bundle is one turn-*/ directory: the request, the response, and the
// metadata describing how the response was framed.
type bundle struct {
	name string
	dir  string
	req  wireRequest
	meta wireMeta
	resp []byte
}

// loadRecordings walks the vendored corpus, which is nested one level deeper
// than a bare drop of turn-*/ directories: <set>/turn-*/. Returns bundles in
// a stable order so failures are reproducible.
func loadRecordings(t *testing.T) []bundle {
	t.Helper()

	sets, err := os.ReadDir(recordingsDir())
	if err != nil {
		t.Fatalf("read %s: %v", recordingsDir(), err)
	}

	var out []bundle
	for _, set := range sets {
		if !set.IsDir() {
			continue
		}
		setDir := filepath.Join(recordingsDir(), set.Name())
		turns, err := os.ReadDir(setDir)
		if err != nil {
			t.Fatalf("read %s: %v", setDir, err)
		}
		for _, turn := range turns {
			if !turn.IsDir() || !strings.HasPrefix(turn.Name(), "turn-") {
				continue
			}
			dir := filepath.Join(setDir, turn.Name())
			req, meta, resp, err := loadWireCaptureBundle(dir)
			if err != nil {
				t.Fatalf("load bundle %s: %v", dir, err)
			}
			out = append(out, bundle{
				name: filepath.Join(set.Name(), turn.Name()),
				dir:  dir,
				req:  req,
				meta: meta,
				resp: resp,
			})
		}
	}

	// An empty corpus means the vendored copy is missing or was emptied.
	// That must fail rather than vacuously pass: a skip here is exactly the
	// hole these tests were un-skipped to close.
	if len(out) == 0 {
		t.Fatalf("no bundles under %s — the fixture corpus is missing", recordingsDir())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// TestWireCaptureReducerFidelity asserts that every recorded response still
// reduces to a well-formed ChatResponse — the same check ingest's
// validateReducedResponse applies, run against real bytes.
func TestWireCaptureReducerFidelity(t *testing.T) {
	r := capture.NewAnthropicReducer()
	ctx := context.Background()

	for _, b := range loadRecordings(t) {
		t.Run(b.name, func(t *testing.T) {
			// The corpus is all 200s; a non-200 would never reach the
			// reducer in the deployed path (DropUpstreamStatus in
			// processor.go), so its presence means the corpus changed
			// shape and the test's assumptions need revisiting.
			if b.meta.Status != 200 {
				t.Fatalf("status=%d: corpus should contain only 200s (extproc drops non-200 before reducing)", b.meta.Status)
			}
			if len(b.resp) == 0 {
				t.Fatalf("response.sse is empty; an empty upstream response is not a reducer input")
			}

			decoded, err := decodeResponseBody(b.resp, b.meta.ContentEncoding)
			if err != nil {
				t.Fatalf("decode encoding=%q: %v (deployed path would DropResponseDecode)", b.meta.ContentEncoding, err)
			}

			resp, err := r.Reduce(ctx,
				bytes.NewReader([]byte("{}")),
				bytes.NewReader(decoded),
				b.meta.ContentType,
			)
			if err != nil {
				t.Fatalf("reduce: %v (content_type=%q decoded_bytes=%d)", err, b.meta.ContentType, len(decoded))
			}
			if reason, empty := reducerEmptyReason(resp); empty {
				t.Fatalf("reduced to a response ingest would reject: reason=%s content_type=%q decoded_bytes=%d stop_reason=%q done=%v",
					reason, b.meta.ContentType, len(decoded), resp.StopReason, resp.Done)
			}
			// A stream that reduces to content but never terminated is a
			// truncation the reducer papered over; the recorded turns all
			// completed (meta finalized_by=stream_complete).
			if !resp.Done {
				t.Errorf("done=false: reducer did not observe stream termination (stop_reason=%q)", resp.StopReason)
			}
			if resp.StopReason == "" {
				t.Errorf("empty stop_reason: reducer produced content without a terminal reason")
			}
		})
	}
}

// TestWireCaptureDecodeShimRequired pins the reason the content-encoding
// decode shim exists. Every recorded response is compressed, so feeding the
// raw bytes to the reducer must NOT yield usable content — if this starts
// passing, either the corpus is no longer compressed or the reducer grew its
// own decoding, and TestWireCaptureReducerFidelity would keep passing either
// way while the deployed shim silently stopped mattering.
func TestWireCaptureDecodeShimRequired(t *testing.T) {
	r := capture.NewAnthropicReducer()
	ctx := context.Background()

	for _, b := range loadRecordings(t) {
		if b.meta.ContentEncoding == "" || b.meta.ContentEncoding == "identity" {
			continue
		}
		t.Run(b.name, func(t *testing.T) {
			resp, err := r.Reduce(ctx,
				bytes.NewReader([]byte("{}")),
				bytes.NewReader(b.resp),
				b.meta.ContentType,
			)
			if err != nil {
				return // erroring on compressed bytes is a valid rejection
			}
			if _, empty := reducerEmptyReason(resp); !empty {
				t.Fatalf("encoding=%q reduced to usable content WITHOUT decoding (%d blocks) — the decode shim is no longer load-bearing; re-check this assumption",
					b.meta.ContentEncoding, len(resp.Message.Content))
			}
		})
	}
}

// TestWireCaptureProcessorFidelity drives each recording through the extproc
// state machine and asserts the turn reaches ingest. This is the end-to-end
// unit path: header phase, request body, response headers with the recorded
// encoding, response body.
func TestWireCaptureProcessorFidelity(t *testing.T) {
	var accepted atomic.Int64
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		accepted.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ingest.Close()

	proc, err := NewProcessor(Config{IngestURL: ingest.URL, MaxInflight: 8})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())

	for _, b := range loadRecordings(t) {
		t.Run(b.name, func(t *testing.T) {
			before := accepted.Load()
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

			// Dispatch is asynchronous; poll rather than sleep a fixed
			// interval so a fast machine doesn't wait and a slow one
			// doesn't flake.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && accepted.Load() == before {
				time.Sleep(5 * time.Millisecond)
			}
			if got := accepted.Load() - before; got != 1 {
				t.Fatalf("ingest accepted %d turns, want 1 (status=%d req_bytes=%d resp_bytes=%d)",
					got, b.meta.Status, len(b.req.Body), len(b.resp))
			}
		})
	}

	if txt := scrapeWireCaptureMetrics(t, proc); !strings.Contains(txt, "tapes_extproc_turns_terminal_total") {
		t.Errorf("terminal-turn metric missing after replaying the corpus")
	}
}

// TestLocalWireCapturesDiagnostic is the operator loop, not a gate. Drop your
// own turn-*/ bundles into the gitignored wireCapturesDir and run
// `go test -run LocalWireCaptures -v` to see how the reducer treats them
// pre- and post-decode. It asserts nothing: the labelled finding IS the
// value, and captures of real traffic vary in ways a gate cannot predict.
func TestLocalWireCapturesDiagnostic(t *testing.T) {
	entries, err := os.ReadDir(wireCapturesDir())
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read %s: %v", wireCapturesDir(), err)
	}
	if len(entries) == 0 {
		t.Skipf("no captures in %s — drop a turn-*/ bundle here to exercise this", wireCapturesDir())
	}

	r := capture.NewAnthropicReducer()
	ctx := context.Background()

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(wireCapturesDir(), e.Name())
		t.Run(e.Name(), func(t *testing.T) {
			_, meta, resp, err := loadWireCaptureBundle(dir)
			if err != nil {
				t.Fatalf("load bundle: %v", err)
			}
			if meta.Status != 200 {
				t.Logf("status=%d skipped (extproc would DropUpstreamStatus before reducing)", meta.Status)
				return
			}
			if len(resp) == 0 {
				t.Logf("EMPTY_RESPONSE: response.sse is zero bytes (upstream completed with no body)")
				return
			}

			runPass(t, r, ctx, "raw", resp, meta.ContentType)

			decoded, decodeErr := decodeResponseBody(resp, meta.ContentEncoding)
			if decodeErr != nil {
				t.Logf("DECODE_FAILED encoding=%q: %v (would DropResponseDecode)", meta.ContentEncoding, decodeErr)
				return
			}
			if meta.ContentEncoding != "" && meta.ContentEncoding != "identity" {
				t.Logf("decoded encoding=%q raw_bytes=%d decoded_bytes=%d ratio=%.2fx",
					meta.ContentEncoding, len(resp), len(decoded),
					float64(len(decoded))/float64(len(resp)))
			}
			runPass(t, r, ctx, "decoded", decoded, meta.ContentType)
		})
	}
}

func scrapeWireCaptureMetrics(t *testing.T, proc *Processor) string {
	t.Helper()
	srv := httptest.NewServer(proc.Metrics().Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}

type wireRequest struct {
	Method string
	Path   string
	Body   []byte
}

type wireMeta struct {
	Status          int
	ContentType     string
	ContentEncoding string
}

func loadWireCaptureBundle(dir string) (wireRequest, wireMeta, []byte, error) {
	metaBytes, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	var metaRaw struct {
		Status          int     `json:"status"`
		ContentType     *string `json:"content_type"`
		ContentEncoding *string `json:"content_encoding"`
	}
	if err := json.Unmarshal(metaBytes, &metaRaw); err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	meta := wireMeta{Status: metaRaw.Status}
	if metaRaw.ContentType != nil {
		meta.ContentType = *metaRaw.ContentType
	}
	if metaRaw.ContentEncoding != nil {
		meta.ContentEncoding = *metaRaw.ContentEncoding
	}

	reqBytes, err := os.ReadFile(filepath.Join(dir, "request.json"))
	if err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	var reqRaw struct {
		Method  string `json:"method"`
		URL     string `json:"url"`
		BodyB64 string `json:"body_b64"`
	}
	if err := json.Unmarshal(reqBytes, &reqRaw); err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	body, err := base64.StdEncoding.DecodeString(reqRaw.BodyB64)
	if err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	path := reqRaw.URL
	if u, err := url.Parse(reqRaw.URL); err == nil && u.Path != "" {
		path = u.RequestURI()
	}
	if path == "" {
		path = "/v1/messages"
	}
	if reqRaw.Method == "" {
		reqRaw.Method = http.MethodPost
	}

	respBytes, err := os.ReadFile(filepath.Join(dir, "response.sse"))
	if err != nil {
		return wireRequest{}, wireMeta{}, nil, err
	}
	return wireRequest{Method: reqRaw.Method, Path: path, Body: body}, meta, respBytes, nil
}

// runPass drives one body shape through the reducer and emits a single
// labelled log line describing what came out. Used by the local diagnostic
// so the operator sees a side-by-side view of the same capture pre-decode
// and post-decode.
func runPass(t *testing.T, r capture.Reducer, ctx context.Context, label string, body []byte, contentType string) {
	t.Helper()
	resp, err := r.Reduce(ctx,
		bytes.NewReader([]byte("{}")),
		bytes.NewReader(body),
		contentType,
	)
	if err != nil {
		t.Logf("[%s] REDUCER_ERROR content_type=%q resp_bytes=%d err=%v",
			label, contentType, len(body), err)
		return
	}
	reason, empty := reducerEmptyReason(resp)
	if empty {
		t.Logf(
			"[%s] EMPTY_CONTENT reason=%s content_type=%q resp_bytes=%d stop_reason=%q done=%v",
			label, reason, contentType, len(body), resp.StopReason, resp.Done,
		)
		return
	}
	t.Logf("[%s] HEALTHY content_blocks=%d stop_reason=%q done=%v",
		label, len(resp.Message.Content), resp.StopReason, resp.Done)
}
