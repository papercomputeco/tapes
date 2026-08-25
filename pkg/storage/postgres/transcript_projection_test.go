package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/derive"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("transcript-only session projection", func() {
	const (
		orgID     = "00000000-0000-0000-0000-000000000000"
		harnessID = "claude"
		sessionID = "historical-transcript-session"
	)

	var (
		ctx     context.Context
		driver  *postgres.Driver
		server  *ingest.Server
		baseURL string
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turn_attribution_corrections, derive_queue, raw_turns RESTART IDENTITY CASCADE")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		server, err = ingest.New(ingest.Config{ListenAddr: ":0", Project: "test-project"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() { _ = server.RunWithListener(ln) }()
		baseURL = "http://" + ln.Addr().String()
	})

	AfterEach(func() {
		if server != nil {
			Expect(server.Close()).To(Succeed())
		}
		if driver != nil {
			driver.Close()
		}
	})

	postTranscriptFile := func(agentID, toolUseID string, records []map[string]any) bool {
		body, err := json.Marshal(map[string]any{
			"session": map[string]any{
				"org_id": "", "auth_subject": "historical-user", "harness_id": harnessID,
				"harness_session_id": sessionID, "harness_version": "2.1.0", "cwd": "/historical/work",
			},
			"agent_id": agentID, "tool_use_id": toolUseID, "records": records,
		})
		Expect(err).NotTo(HaveOccurred())
		resp, err := http.Post(baseURL+"/v1/ingest/transcript", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		var ack struct {
			Deduped bool `json:"deduped"`
		}
		Expect(json.NewDecoder(resp.Body).Decode(&ack)).To(Succeed())
		return ack.Deduped
	}
	postTranscript := func(records []map[string]any) bool {
		return postTranscriptFile("", "", records)
	}

	It("creates historical identity, selects growing versions idempotently, and yields to late usable wire", func() {
		first := []map[string]any{
			{"type": "user", "uuid": "u1", "timestamp": "2025-01-02T03:04:05Z", "message": map[string]any{"role": "user", "content": "from disk"}},
			{"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2025-01-02T03:04:06Z", "message": map[string]any{"id": "m1", "role": "assistant", "model": "claude", "content": []map[string]any{{"type": "text", "text": "historical answer"}}, "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 4, "output_tokens": 2}}},
		}
		Expect(postTranscript(first)).To(BeFalse())

		var dbSessionID string
		var startedAt, lastSeenAt time.Time
		var endedAt *time.Time
		Expect(driver.DB().QueryRow(ctx, `
			SELECT id::text, started_at, last_seen_at, ended_at
			FROM sessions WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			orgID, harnessID, sessionID).Scan(&dbSessionID, &startedAt, &lastSeenAt, &endedAt)).To(Succeed())
		Expect(startedAt).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)))
		Expect(lastSeenAt).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 6, 0, time.UTC)))
		Expect(endedAt).To(BeNil())
		var queued int
		Expect(driver.DB().QueryRow(ctx, "SELECT COUNT(*) FROM derive_queue WHERE harness_session_id=$1", sessionID).Scan(&queued)).To(Succeed())
		Expect(queued).To(Equal(1))

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.TranscriptProjection).NotTo(BeNil())
		Expect(report.RawTurns).To(Equal(1))
		Expect(report.ParsedTurns).To(Equal(1), "one transcript file is parsed once, not once per assistant call")
		turns, spans, _, err := driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).To(HaveLen(1))
		Expect(turns[0].Source).To(Equal(storage.RawTurnSourceTranscript))
		firstTraceID := turns[0].TraceID
		firstSpanIDs := storedSpanIDs(spans)

		// An unusable wire row is diagnostic raw data, not a reason to hide the
		// useful transcript fallback.
		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic",
			HarnessID: harnessID, HarnessSessionID: sessionID, RequestID: "malformed-wire",
			RawRequest: json.RawMessage(`{"not":"a request"}`), Response: json.RawMessage(`{}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		turns, _, _, err = driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).To(HaveLen(1))
		Expect(turns[0].Source).To(Equal(storage.RawTurnSourceTranscript))

		grown := append(append([]map[string]any{}, first...),
			map[string]any{"type": "user", "uuid": "u2", "parentUuid": "a1", "timestamp": "2025-01-02T03:05:00Z", "message": map[string]any{"role": "user", "content": "second turn"}},
			map[string]any{"type": "assistant", "uuid": "a2", "parentUuid": "u2", "timestamp": "2025-01-02T03:05:01Z", "message": map[string]any{"id": "m2", "role": "assistant", "model": "claude", "content": []map[string]any{{"type": "text", "text": "second answer"}}, "stop_reason": "end_turn"}},
		)
		Expect(postTranscript(grown)).To(BeFalse())
		Expect(postTranscript(grown)).To(BeTrue(), "deduped uploads still exercise the atomic requeue path")
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		turns, spans, _, err = driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).To(HaveLen(2))
		Expect(turns[0].TraceID).To(Equal(firstTraceID))
		Expect(storedSpanIDs(spans)).To(ContainElements(firstSpanIDs))

		// Repeating derive is an in-place rewrite of the same deterministic set.
		idsAfterGrowth := storedSpanIDs(spans)
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		turnsAgain, spansAgain, _, err := driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turnsAgain).To(Equal(turns))
		Expect(storedSpanIDs(spansAgain)).To(Equal(idsAfterGrowth))

		// One usable wire call switches the whole session to wire projection;
		// writeSpanSet prunes every transcript-derived trace/span.
		inserted, err = driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: harnessID, HarnessSessionID: sessionID, RequestID: "late-wire",
			RawRequest: json.RawMessage(`{"model":"claude","stream":true,"max_tokens":4096,"tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"wire truth"}]}`),
			Response:   json.RawMessage(`{"model":"claude","message":{"role":"assistant","content":[{"type":"text","text":"wire answer"}]},"stop_reason":"end_turn"}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
		report, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.TranscriptProjection.SuppressedByWire).To(BeTrue())
		Expect(report.RawTurns).To(Equal(3), "latest transcript + malformed wire + usable wire")
		Expect(report.ParsedTurns).To(Equal(2), "the selected transcript file and usable wire each count once")
		turns, spans, _, err = driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).To(HaveLen(1))
		Expect(turns[0].Source).To(Equal(storage.RawTurnSourceWire))
		Expect(turns[0].UserPrompt).To(Equal("wire truth"))
		Expect(storedSpanIDs(spans)).NotTo(ContainElements(idsAfterGrowth))
	})

	It("keeps the main file separate from a legal subagent id named main", func() {
		main := []map[string]any{
			{"type": "user", "uuid": "shared-user", "timestamp": "2025-01-02T03:04:05Z", "message": map[string]any{"role": "user", "content": "delegate"}},
			{
				"type": "assistant", "uuid": "shared-answer", "parentUuid": "shared-user",
				"timestamp": "2025-01-02T03:04:06Z",
				"message": map[string]any{
					"id": "shared-message", "role": "assistant",
					"content": []map[string]any{{
						"type": "tool_use", "id": "spawn-main", "name": "Task",
						"input": map[string]any{"description": "child"},
					}},
				},
			},
		}
		child := []map[string]any{
			{"type": "user", "uuid": "shared-user", "timestamp": "2025-01-02T03:04:07Z", "isSidechain": true, "message": map[string]any{"role": "user", "content": "child work"}},
			{"type": "assistant", "uuid": "shared-answer", "parentUuid": "shared-user", "timestamp": "2025-01-02T03:04:08Z", "isSidechain": true, "message": map[string]any{"id": "shared-message", "role": "assistant", "content": "child done"}},
		}
		Expect(postTranscriptFile("", "", main)).To(BeFalse())
		Expect(postTranscriptFile("main", "spawn-main", child)).To(BeFalse())

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.TranscriptFiles).To(Equal(2))
		Expect(report.TranscriptProjection.Files).To(Equal(2))

		var dbSessionID string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT id::text FROM sessions
			WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			orgID, harnessID, sessionID).Scan(&dbSessionID)).To(Succeed())
		turns, spans, links, err := driver.ListSessionSpanModel(ctx, dbSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).To(HaveLen(1))
		Expect(turns[0].UserPrompt).To(Equal("delegate"))

		var childAgent *storage.SpanRecord
		for i := range spans {
			if spans[i].Kind == derive.SpanKindAgent && spans[i].ThreadID == "main" {
				childAgent = &spans[i]
			}
		}
		Expect(childAgent).NotTo(BeNil())
		Expect(childAgent.ParentSpanID).To(HavePrefix("txtool_"))
		Expect(links).To(ContainElement(HaveField("Kind", derive.LinkRejoin)))
	})
})

func storedSpanIDs(spans []storage.SpanRecord) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		out = append(out, span.TraceID+"/"+span.SpanID)
	}
	return out
}
