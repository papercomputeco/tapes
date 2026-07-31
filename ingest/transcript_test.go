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
	"github.com/papercomputeco/tapes/pkg/derive"
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

	It("stores a transcript under the single-tenant sentinel regardless of the payload org", func() {
		resp := post(transcriptBody(payloadOrg, "user_payload"), nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(rec.OrgID).To(BeEmpty(),
			"the caller does not get to pick an org; the deployment is the tenant")
		Expect(rec.HarnessSessionID).To(Equal(sessionID))
	})

	It("resolves the subject from the gateway header and ignores any org header", func() {
		// Identity from the edge is the subject only. The org is settled by
		// the deployment — an org header (which nothing legitimate ever
		// stamped) must not store rows the nil-scoped read side would miss.
		resp := post(transcriptBody(payloadOrg, "user_payload"), map[string]string{
			ingest.HeaderPaperAuthOrgID:   gatewayOrg,
			ingest.HeaderPaperAuthSubject: "user_gateway",
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.OrgID).To(BeEmpty())

		var envelope sessions.IngestEnvelope
		Expect(json.Unmarshal(rec.SessionEnvelope, &envelope)).To(Succeed())
		Expect(envelope.OrgID).To(BeEmpty())
		Expect(envelope.AuthSubject).To(Equal("user_gateway"))
	})

	It("keeps the payload subject when no gateway subject arrives", func() {
		resp := post(transcriptBody(payloadOrg, "user_payload"), map[string]string{
			ingest.HeaderPaperAuthOrgID: gatewayOrg,
		})
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		var envelope sessions.IngestEnvelope
		Expect(json.Unmarshal(driver.lastRecord().SessionEnvelope, &envelope)).To(Succeed())
		Expect(envelope.AuthSubject).To(Equal("user_payload"))
	})

	It("mints the Codex spawn-anchor row from the paperd uploader payload (PCC-1021)", func() {
		// The Codex subagent spawn-anchor contract (pkg/derive/codex.go):
		// paperd uploads one transcript payload per spawned child, keyed
		// to the ROOT Codex session, carrying the child thread id as
		// agent_id, the spawn_agent call_id as tool_use_id, and the
		// verbatim kind:"started" rollout line as the single record. The
		// endpoint must accept it unchanged and mint the row the derive
		// identity join reads.
		const (
			rootSession = "019f8d46-beb1-7f50-9df4-0cd39ed38d13"
			childThread = "019f8d46-e663-74e1-940c-f82e34c07618"
			spawnCallID = "call_J7B6r7ZdtqkECtSJV8YDQaL7"
		)
		startedLine := json.RawMessage(`{"timestamp":"2026-07-23T04:41:01.858Z","type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"` +
			spawnCallID + `","occurred_at_ms":1784781661858,"agent_thread_id":"` + childThread +
			`","agent_path":"/root/depth2_cli_child","kind":"started"}}`)
		payload := ingest.TranscriptPayload{
			Session: &sessions.IngestEnvelope{
				OrgID:            payloadOrg,
				HarnessID:        "codex",
				HarnessSessionID: rootSession,
				HarnessVersion:   "0.145.0-alpha.30",
				Cwd:              "/work",
			},
			AgentID:     childThread,
			AgentType:   "depth2_cli_child",
			Description: "/root/depth2_cli_child",
			ToolUseID:   spawnCallID,
			Records:     mustJSON([]json.RawMessage{startedLine}),
		}
		body, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())

		resp := post(body, nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.Source).To(Equal(storage.RawTurnSourceTranscript))
		Expect(rec.HarnessID).To(Equal("codex"))
		Expect(rec.HarnessSessionID).To(Equal(rootSession), "anchors key to the ROOT session, never the child thread")
		Expect(rec.RequestID).To(HavePrefix("transcript:" + rootSession + ":" + childThread + ":"))

		var meta struct {
			Transcript  bool   `json:"transcript"`
			AgentID     string `json:"agent_id"`
			AgentType   string `json:"agent_type"`
			Description string `json:"description"`
			ToolUseID   string `json:"tool_use_id"`
			Records     int    `json:"records"`
		}
		Expect(json.Unmarshal(rec.Meta, &meta)).To(Succeed())
		Expect(meta.Transcript).To(BeTrue())
		Expect(meta.AgentID).To(Equal(childThread))
		Expect(meta.AgentType).To(Equal("depth2_cli_child"))
		Expect(meta.Description).To(Equal("/root/depth2_cli_child"))
		Expect(meta.ToolUseID).To(Equal(spawnCallID))
		Expect(meta.Records).To(Equal(1))

		// The row round-trips through the derive-side parser: the
		// identity join reads agent_id/tool_use_id even though the
		// record is a Codex rollout line, not Claude transcript JSONL.
		file, err := derive.ParseTranscriptFile(&rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(file.AgentID).To(Equal(childThread))
		Expect(file.ToolUseID).To(Equal(spawnCallID))
		Expect(file.Session).To(Equal(derive.SessionKey{HarnessID: "codex", HarnessSessionID: rootSession}))
	})

	It("stores the interacted-anchor kind in row meta (PCC-1021 decision C)", func() {
		// paperd also uploads kind:"interacted" sub_agent_activity
		// records (send_message / followup_task re-entries; agent_id is
		// the TARGET thread, tool_use_id the triggering call). The
		// payload's kind marker must land in row meta so consumers can
		// filter without parsing records — and the derive parser must
		// surface it, which is what keeps these rows inert in
		// derivation.
		const (
			rootSession = "019f8d46-beb1-7f50-9df4-0cd39ed38d13"
			targetChild = "019f8d46-e663-74e1-940c-f82e34c07618"
			sendCallID  = "call_cqusEjhomv5zKjZ7vodiY7Og"
		)
		interactedLine := json.RawMessage(`{"timestamp":"2026-07-23T04:41:18.008Z","type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"` +
			sendCallID + `","occurred_at_ms":1784781678008,"agent_thread_id":"` + targetChild +
			`","agent_path":"/root/depth2_cli_child","kind":"interacted"}}`)
		payload := ingest.TranscriptPayload{
			Session: &sessions.IngestEnvelope{
				OrgID:            payloadOrg,
				HarnessID:        "codex",
				HarnessSessionID: rootSession,
			},
			AgentID:     targetChild,
			AgentType:   "depth2_cli_child",
			Description: "/root/depth2_cli_child",
			ToolUseID:   sendCallID,
			Kind:        "interacted",
			Records:     mustJSON([]json.RawMessage{interactedLine}),
		}
		body, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())

		resp := post(body, nil)
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		rec := driver.lastRecord()
		Expect(rec.HarnessSessionID).To(Equal(rootSession))

		var meta struct {
			Transcript bool   `json:"transcript"`
			AgentID    string `json:"agent_id"`
			ToolUseID  string `json:"tool_use_id"`
			Kind       string `json:"kind"`
		}
		Expect(json.Unmarshal(rec.Meta, &meta)).To(Succeed())
		Expect(meta.Transcript).To(BeTrue())
		Expect(meta.AgentID).To(Equal(targetChild))
		Expect(meta.ToolUseID).To(Equal(sendCallID))
		Expect(meta.Kind).To(Equal("interacted"))

		file, err := derive.ParseTranscriptFile(&rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(file.Kind).To(Equal("interacted"))
	})

	It("dedups an unchanged re-push even when the asserted org differs", func() {
		// Single tenant: org is not part of identity, so the same content is
		// the same row no matter what org a caller asserts. Before the org
		// removal this was segregated by org; now nothing a caller sends can
		// mint a second copy of the same transcript.
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
		Expect(count).To(Equal(int64(1)))
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
