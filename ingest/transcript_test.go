package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// rawStoreDriver wraps the in-memory driver with an in-process
// RawTurnStore so the transcript handler (which requires the raw layer)
// is exercisable without Postgres. Appends are recorded verbatim; dedup
// mirrors the Postgres partial unique index on (org, request_id).
type rawStoreDriver struct {
	*inmemory.Driver

	mu      sync.Mutex
	records []storage.RawTurnRecord

	// putErr, when non-nil, is returned by PutRawTurn instead of appending —
	// lets a test drive the handler's error-classification branches.
	putErr error
}

func newRawStoreDriver() *rawStoreDriver {
	return &rawStoreDriver{Driver: inmemory.NewDriver()}
}

func (d *rawStoreDriver) PutRawTurn(_ context.Context, rec storage.RawTurnRecord) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.putErr != nil {
		return false, d.putErr
	}
	if rec.RequestID != "" {
		for _, existing := range d.records {
			if existing.OrgID == rec.OrgID && existing.RequestID == rec.RequestID {
				return false, nil
			}
		}
	}
	d.records = append(d.records, rec)
	return true, nil
}

func (d *rawStoreDriver) ListRawTurns(_ context.Context, afterID int64, pageSize int32) ([]storage.RawTurnRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.RawTurnRecord, 0, len(d.records))
	for i, rec := range d.records {
		id := int64(i + 1)
		if id <= afterID {
			continue
		}
		rec.ID = id
		out = append(out, rec)
		if int32(len(out)) >= pageSize {
			break
		}
	}
	return out, nil
}

func (d *rawStoreDriver) CountRawTurns(_ context.Context) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.records)), nil
}

// lastRecord returns the most recently appended raw turn.
func (d *rawStoreDriver) lastRecord() storage.RawTurnRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	Expect(d.records).NotTo(BeEmpty())
	return d.records[len(d.records)-1]
}

func newTranscriptTestServer() (*ingest.Server, *rawStoreDriver, string) {
	logger := tapeslogger.NewNoop()
	driver := newRawStoreDriver()

	s, err := ingest.New(
		ingest.Config{
			ListenAddr: ":0",
			Project:    "test-project",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	go func() {
		_ = s.RunWithListener(ln)
	}()

	baseURL := "http://" + ln.Addr().String()
	return s, driver, baseURL
}

var _ = Describe("POST /v1/ingest/transcript", func() {
	const (
		payloadOrg = "11111111-1111-1111-1111-111111111111"
		gatewayOrg = "22222222-2222-2222-2222-222222222222"
		sessionID  = "0ea3c2cc-fe9d-41ff-aab1-4134ad00c350"
	)

	var (
		server  *ingest.Server
		driver  *rawStoreDriver
		baseURL string
		client  *http.Client
	)

	BeforeEach(func() {
		server, driver, baseURL = newTranscriptTestServer()
		client = &http.Client{Timeout: 5 * time.Second}
	})

	AfterEach(func() {
		Expect(server.Close()).To(Succeed())
	})

	transcriptBody := func(orgID, authSubject string) []byte {
		payload := ingest.TranscriptPayload{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      authSubject,
				HarnessID:        "claude",
				HarnessSessionID: sessionID,
			},
			Records: mustJSON([]map[string]string{{"type": "user", "uuid": "u-1"}}),
		}
		body, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())
		return body
	}

	post := func(body []byte, headers map[string]string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/ingest/transcript", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		Expect(err).NotTo(HaveOccurred())
		return resp
	}

	It("accepts a transcript and persists the payload org when no gateway identity arrives", func() {
		resp := post(transcriptBody(payloadOrg, "user_payload"), nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(rec.OrgID).To(Equal(payloadOrg))
		Expect(rec.HarnessSessionID).To(Equal(sessionID))
	})

	It("resolves org and subject from the gateway identity headers over the payload", func() {
		resp := post(transcriptBody(payloadOrg, "user_payload"), map[string]string{
			ingest.HeaderPaperAuthOrgID:   gatewayOrg,
			ingest.HeaderPaperAuthSubject: "user_gateway",
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.OrgID).To(Equal(gatewayOrg))

		var envelope sessions.IngestEnvelope
		Expect(json.Unmarshal(rec.SessionEnvelope, &envelope)).To(Succeed())
		Expect(envelope.OrgID).To(Equal(gatewayOrg))
		Expect(envelope.AuthSubject).To(Equal("user_gateway"))
	})

	It("resolves the gateway org even when the payload carries none", func() {
		// The production shape: paperd serializes org_id as "" because it
		// cannot know the platform org UUID; the gateway supplies it.
		resp := post(transcriptBody("", ""), map[string]string{
			ingest.HeaderPaperAuthOrgID: gatewayOrg,
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		Expect(driver.lastRecord().OrgID).To(Equal(gatewayOrg))
	})

	It("rejects a gateway org that is not a UUID", func() {
		// The override runs before envelope validation: a gateway
		// stamping a malformed org (e.g. a WorkOS org_… id passed through
		// verbatim) must reject loudly at the boundary, not silently
		// misattribute the upload.
		resp := post(transcriptBody("", ""), map[string]string{
			ingest.HeaderPaperAuthOrgID: "org_01KTCRQ6ZEXXJKVPGXES8XRZVN",
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("keeps the payload subject when only the org header arrives", func() {
		resp := post(transcriptBody(payloadOrg, "user_payload"), map[string]string{
			ingest.HeaderPaperAuthOrgID: gatewayOrg,
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		var envelope sessions.IngestEnvelope
		Expect(json.Unmarshal(driver.lastRecord().SessionEnvelope, &envelope)).To(Succeed())
		Expect(envelope.OrgID).To(Equal(gatewayOrg))
		Expect(envelope.AuthSubject).To(Equal("user_payload"))
	})

	It("dedups an unchanged re-push and segregates the dedup key by org", func() {
		// Same content, same org → deduped. Same content, different org
		// (header-resolved) → its own row: org is part of the raw-layer
		// identity, which is exactly what makes header-spoofed
		// cross-tenant collisions impossible once the gateway strips
		// inbound identity headers.
		resp1 := post(transcriptBody(payloadOrg, ""), nil)
		resp1.Body.Close()
		resp2 := post(transcriptBody(payloadOrg, ""), nil)
		defer resp2.Body.Close()

		var ack struct {
			Deduped bool `json:"deduped"`
		}
		Expect(json.NewDecoder(resp2.Body).Decode(&ack)).To(Succeed())
		Expect(ack.Deduped).To(BeTrue())

		resp3 := post(transcriptBody(payloadOrg, ""), map[string]string{
			ingest.HeaderPaperAuthOrgID: gatewayOrg,
		})
		resp3.Body.Close()
		count, err := driver.CountRawTurns(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(2)))
	})

	scrapeMetrics := func() string {
		resp, err := client.Get(baseURL + "/metrics")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		return string(body)
	}

	It("meters an accepted transcript on writes_total{provider=transcript}", func() {
		resp := post(transcriptBody(payloadOrg, ""), nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		Expect(scrapeMetrics()).To(ContainSubstring(`tapes_ingest_writes_total{provider="transcript",status="accepted"}`))
	})

	It("returns 422 (not 502) and meters reject_parse when content is unstorable", func() {
		// A content-level rejection is the client's malformed payload: the
		// handler must classify it as unprocessable, not a downstream fault.
		driver.putErr = fmt.Errorf("insert raw turn: %w", storage.ErrInvalidContent)

		resp := post(transcriptBody(payloadOrg, ""), nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
		Expect(scrapeMetrics()).To(ContainSubstring(`tapes_ingest_writes_total{provider="transcript",status="reject_parse"}`))
	})

	It("returns 502 and meters downstream_error on a genuine storage fault", func() {
		driver.putErr = errors.New("connection refused")

		resp := post(transcriptBody(payloadOrg, ""), nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusBadGateway))
		Expect(scrapeMetrics()).To(ContainSubstring(`tapes_ingest_writes_total{provider="transcript",status="downstream_error"}`))
	})

	It("returns 501 when the driver has no raw layer", func() {
		// A bare in-memory driver does not implement storage.RawTurnStore,
		// so the transcript endpoint (which requires the raw layer) is
		// unavailable. newTestServer's capture driver DOES host the raw
		// layer, so build a no-raw-layer server explicitly here.
		s, err := ingest.New(
			ingest.Config{ListenAddr: ":0", Project: "test-project"},
			inmemory.NewDriver(),
			tapeslogger.NewNoop(),
		)
		Expect(err).NotTo(HaveOccurred())
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() { _ = s.RunWithListener(ln) }()
		url := "http://" + ln.Addr().String()
		defer func() { Expect(s.Close()).To(Succeed()) }()

		req, err := http.NewRequest(http.MethodPost, url+"/v1/ingest/transcript", bytes.NewReader(transcriptBody(payloadOrg, "")))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusNotImplemented))
	})
})
