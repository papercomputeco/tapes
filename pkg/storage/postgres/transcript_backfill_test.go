package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// Transcript-only backfill: a session whose transcripts were synced but whose
// wire capture never ran must still materialize as a session with
// source="transcript" traces. These specs drive the real ingest HTTP handler
// (which now also materializes session identity) and then rederive against
// Postgres.
var _ = Describe("transcript-only session backfill", func() {
	const (
		harnessID   = "claude"
		rootSession = "backfill-root-session"
	)

	var (
		ctx     context.Context
		driver  *postgres.Driver
		orgID   string
		baseURL string
	)

	BeforeEach(func() {
		ctx = context.Background()
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

	It("materializes session identity for a transcript with no wire capture", func() {
		ingester, ok := storage.Driver(driver).(storage.TranscriptSessionIngester)
		Expect(ok).To(BeTrue())

		envelope := &sessions.IngestEnvelope{
			AuthSubject:      "user-test",
			HarnessID:        harnessID,
			HarnessSessionID: rootSession,
			Cwd:              "/tmp/proj",
			HarnessVersion:   "2.0.0",
		}

		first, err := ingester.IngestTranscriptSession(ctx, storage.IngestTranscriptSessionRequest{
			Session: envelope,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(first.SessionID).NotTo(BeEmpty())

		// Idempotent: a re-sync (the dedup case) resolves the same row.
		second, err := ingester.IngestTranscriptSession(ctx, storage.IngestTranscriptSessionRequest{
			Session: envelope,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(second.SessionID).To(Equal(first.SessionID))

		var count int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT COUNT(*) FROM sessions WHERE harness_id = $1 AND harness_session_id = $2",
			harnessID, rootSession).Scan(&count)).To(Succeed())
		Expect(count).To(Equal(1))
	})

	It("projects a source=transcript trace when a transcript-only session rederives", func() {
		// Drive the real write surface: POST /v1/ingest/transcript for a
		// main transcript with one user→assistant exchange. The handler now
		// also materializes the session row.
		records := `[{"uuid":"u1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"write a test"}},` +
			`{"uuid":"u2","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]}}]`
		body, err := json.Marshal(map[string]any{
			"session": map[string]any{
				"auth_subject":       "user-test",
				"harness_id":         harnessID,
				"harness_session_id": rootSession,
			},
			"records": json.RawMessage(records),
		})
		Expect(err).NotTo(HaveOccurred())

		resp, err := http.Post(baseURL+"/v1/ingest/transcript", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

		report, err := driver.RederiveSessionLocked(ctx, "test-project", orgID, harnessID, rootSession)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.ParsedTurns).To(Equal(1))

		var (
			sessionID string
			source    string
			prompt    string
		)
		Expect(driver.DB().QueryRow(ctx,
			"SELECT id FROM sessions WHERE harness_id = $1 AND harness_session_id = $2",
			harnessID, rootSession).Scan(&sessionID)).To(Succeed())

		Expect(driver.DB().QueryRow(ctx,
			"SELECT source, user_prompt FROM span_turns_20260615 WHERE session_id = $1",
			sessionID).Scan(&source, &prompt)).To(Succeed())
		Expect(source).To(Equal("transcript"))
		Expect(prompt).To(ContainSubstring("write a test"))
	})
})
