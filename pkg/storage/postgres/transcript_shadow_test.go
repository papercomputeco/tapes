package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// Round-trip guard for the derive-read transcript selection (PCC-1021).
// The raw layer is append-only, so a started spawn-anchor row is never
// physically replaced — but RederiveSession collapses transcript rows
// to the latest version per lifecycle group before reconciliation, and
// an interacted re-entry row shares the started row's agent_id (it
// targets the same child thread). These specs drive the REAL
// POST /v1/ingest/transcript handler against Postgres and then
// rederive, so the selection itself is under test — unlike the corpus
// gate, which feeds TranscriptFiles straight into reconciliation.
var _ = Describe("codex transcript anchor rows across the derive read", func() {
	const (
		harnessID   = "codex"
		rootSession = "shadow-root-session"
		childThread = "shadow-child-thread"
		spawnCallID = "call_spawn_child"
	)

	var (
		ctx     context.Context
		driver  *postgres.Driver
		orgID   string
		baseURL string
	)

	BeforeEach(func() {
		ctx = context.Background()
		// Ingest canonicalizes every write to the single-tenant sentinel
		// (the nil org UUID) regardless of what the payload asserts, so the
		// suite seeds and rederives under the sentinel too. The payload
		// below still carries a made-up org to prove the canonicalization.
		orgID = "00000000-0000-0000-0000-000000000000"
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turn_attribution_corrections, derive_queue, raw_turns RESTART IDENTITY CASCADE")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		srv, err := ingest.New(
			ingest.Config{ListenAddr: ":0", Project: "test-project"},
			driver,
			tapeslogger.NewNoop(),
		)
		Expect(err).NotTo(HaveOccurred())
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			_ = srv.RunWithListener(ln)
		}()
		baseURL = "http://" + ln.Addr().String()
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// postTranscript drives the real ingest write surface — the same
	// handler paperd's uploader hits — so the row's meta and dedup key
	// are exactly what production mints.
	postTranscript := func(agentID, toolUseID, kind string, records string) {
		body, err := json.Marshal(map[string]any{
			"session": map[string]any{
				"org_id":             "11111111-1111-1111-1111-111111111111",
				"auth_subject":       "user-test",
				"harness_id":         harnessID,
				"harness_session_id": rootSession,
			},
			"agent_id":    agentID,
			"agent_type":  "researcher",
			"tool_use_id": toolUseID,
			"kind":        kind,
			"records":     json.RawMessage(records),
		})
		Expect(err).NotTo(HaveOccurred())
		resp, err := http.Post(baseURL+"/v1/ingest/transcript", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
	}

	startedRecords := `[{"type":"event_msg","payload":{"type":"sub_agent_activity","kind":"started","agent_id":"` + childThread + `"}}]`
	interactedRecords := func(n int) string {
		return fmt.Sprintf(
			`[{"type":"event_msg","payload":{"type":"sub_agent_activity","kind":"interacted","agent_id":%q,"seq":%d}}]`,
			childThread, n)
	}

	// putWireTurns captures a minimal Codex session: a root turn whose
	// response emits the spawn_agent tool_use, and one child-thread
	// turn. The spawn's function_call_output is deliberately absent, so
	// the agent_path fallback join cannot mask a lost started anchor —
	// if the anchor row does not survive the derive read, the child
	// observably re-parents to the trace root.
	putWireTurns := func() {
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())

		// The requests carry stream:true and a tool set so ClassifyCall
		// types them as the conversation spine (KindMain); the response
		// blocks use the provider-agnostic tags (tool_use_id/tool_name)
		// the stored reduction carries, so the spawn tool span is minted.
		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "codex",
			HarnessID: harnessID, HarnessSessionID: rootSession, RequestID: "shadow-root-spawn",
			RawRequest: json.RawMessage(
				`{"model":"claude-test","stream":true,"max_tokens":4096,"tools":[{"name":"spawn_agent"}],"messages":[{"role":"user","content":"spawn the child"}]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"tool_use","tool_use_id":"` + spawnCallID + `","tool_name":"spawn_agent","tool_input":{"task_name":"shadow_child"}}]},"stop_reason":"tool_use"}`),
			Meta: json.RawMessage(`{}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		inserted, err = driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "codex",
			HarnessID: harnessID, HarnessSessionID: rootSession, RequestID: "shadow-child-turn",
			RawRequest: json.RawMessage(
				`{"model":"claude-test","stream":true,"max_tokens":4096,"tools":[{"name":"spawn_agent"}],"messages":[{"role":"user","content":"child work"}]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"child done"}]},"stop_reason":"end_turn"}`),
			Meta: json.RawMessage(fmt.Sprintf(`{"thread_id":%q}`, childThread)),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
	}

	agentSpanParent := func() string {
		var parent string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT sp.parent_span_id
			FROM spans_20260615 sp
			JOIN sessions s ON s.id = sp.session_id
			WHERE s.org_id = $1 AND s.harness_id = $2 AND s.harness_session_id = $3
			  AND sp.span_id = $4`,
			orgID, harnessID, rootSession, "agent_"+childThread).Scan(&parent)).To(Succeed())
		return parent
	}

	It("keeps the started anchor when a later interacted row targets the same child", func() {
		putWireTurns()
		postTranscript(childThread, spawnCallID, "started", startedRecords)
		postTranscript(childThread, "call_send_1", "interacted", interactedRecords(1))

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.CodexThreadsAnchored).To(Equal(1),
			"the started anchor must survive a later interacted upload for the same agent_id")
		Expect(report.Reconcile.CodexThreadsUnanchored).To(BeZero())
		Expect(report.Reconcile.CodexInteractedRows).To(Equal(1),
			"the interacted row stays visible to reconciliation as an inert, counted file")
		Expect(agentSpanParent()).To(Equal(spawnCallID),
			"the child agent span must stay nested under its spawn_agent tool span")
	})

	It("keeps the started anchor across multiple later interacted rows", func() {
		putWireTurns()
		postTranscript(childThread, spawnCallID, "started", startedRecords)
		postTranscript(childThread, "call_send_1", "interacted", interactedRecords(1))
		postTranscript(childThread, "call_send_2", "interacted", interactedRecords(2))
		postTranscript(childThread, "call_send_3", "interacted", interactedRecords(3))

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.CodexThreadsAnchored).To(Equal(1))
		Expect(report.Reconcile.CodexThreadsUnanchored).To(BeZero())
		Expect(agentSpanParent()).To(Equal(spawnCallID))
	})

	It("degrades an interacted-only child as unanchored, never anchoring it to the send call", func() {
		putWireTurns()
		postTranscript(childThread, "call_send_1", "interacted", interactedRecords(1))

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.CodexThreadsAnchored).To(BeZero())
		Expect(report.Reconcile.CodexThreadsUnanchored).To(Equal(1),
			"a thread whose only rows are interacted degrades exactly as if unanchored")
		Expect(agentSpanParent()).NotTo(Equal("call_send_1"),
			"an interacted row must never anchor its target thread to the send call")
	})

	// Version-skew shape: a row minted by an ingest build that predates
	// the meta kind field carries no meta kind, but its verbatim
	// sub_agent_activity content still says interacted. The selection
	// cannot see the content cheaply, so the recovery happens after
	// parsing — the skew row must not shadow the started anchor either.
	It("keeps the started anchor when a skew-shaped interacted row (no meta kind) lands later", func() {
		putWireTurns()
		postTranscript(childThread, spawnCallID, "started", startedRecords)
		postTranscript(childThread, "call_send_1", "", interactedRecords(1))

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.CodexThreadsAnchored).To(Equal(1),
			"a skew interacted row (kind only in record content) must not shadow the started anchor")
		Expect(report.Reconcile.CodexThreadsUnanchored).To(BeZero())
		Expect(agentSpanParent()).To(Equal(spawnCallID))
	})

	// Growth guard: the legitimate latest-version semantics must
	// survive the fix — a re-uploaded (grown) started file replaces its
	// earlier version rather than accumulating.
	It("still reads only the latest version of a grown started anchor file", func() {
		putWireTurns()
		postTranscript(childThread, spawnCallID, "started", startedRecords)
		grown := `[{"type":"event_msg","payload":{"type":"sub_agent_activity","kind":"started","agent_id":"` + childThread + `"}},{"type":"event_msg","payload":{"type":"other"}}]`
		postTranscript(childThread, spawnCallID, "started", grown)

		report, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile.TranscriptFiles).To(Equal(1),
			"two versions of the same started file must collapse to the latest")
		Expect(report.Reconcile.CodexThreadsAnchored).To(Equal(1))
		Expect(agentSpanParent()).To(Equal(spawnCallID))
	})
})
