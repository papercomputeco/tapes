package sqlitecore_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/sqlitecore"
)

func TestSQLiteCore(t *testing.T) { RegisterFailHandler(Fail); RunSpecs(t, "SQLite Core Suite") }

var _ = Describe("local core", func() {
	var d *sqlitecore.Driver
	var ctx context.Context
	BeforeEach(func() {
		ctx = context.Background()
		var e error
		d, e = sqlitecore.NewDriver(ctx, filepath.Join(GinkgoT().TempDir(), "core.sqlite"))
		Expect(e).NotTo(HaveOccurred())
	})
	AfterEach(func() { Expect(d.Close()).To(Succeed()) })
	It("appends raw turns, deduplicates, and queues their session", func() {
		r := storage.RawTurnRecord{OrgID: "org", Source: storage.RawTurnSourceWire, Provider: "anthropic", HarnessID: "claude", HarnessSessionID: "s", RequestID: "r", RawRequest: []byte(`{"x":1}`), Response: []byte(`{"y":2}`), RawResponse: []byte("bytes")}
		added, e := d.PutRawTurn(ctx, r)
		Expect(e).NotTo(HaveOccurred())
		Expect(added).To(BeTrue())
		added, e = d.PutRawTurn(ctx, r)
		Expect(e).NotTo(HaveOccurred())
		Expect(added).To(BeFalse())
		rows, e := d.ListRawTurns(ctx, 0, 10)
		Expect(e).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].RawResponse).To(Equal([]byte("bytes")))
		q, e := d.GetDeriveDirty(ctx, "org", "claude", "s")
		Expect(e).NotTo(HaveOccurred())
		Expect(q).NotTo(BeNil())
		Expect(q.DirtiedAt).To(BeTemporally("~", time.Now(), time.Second))
	})

	It("migrates an existing versioned database", func() {
		path := filepath.Join(GinkgoT().TempDir(), "migrate.sqlite")
		original, err := sqlitecore.NewDriver(ctx, path)
		Expect(err).NotTo(HaveOccurred())
		Expect(original.Close()).To(Succeed())

		db, err := sql.Open("sqlite", "file:"+path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`DROP TABLE deleted_sessions; PRAGMA user_version = 1`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		migrated, err := sqlitecore.NewDriver(ctx, path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(migrated.Close()).To(Succeed()) })
		_, err = migrated.SweepDeriveDirty(ctx, time.Now().Add(-time.Hour))
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects an unversioned database with a malformed known table", func() {
		path := filepath.Join(GinkgoT().TempDir(), "malformed.sqlite")
		db, err := sql.Open("sqlite", "file:"+path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`CREATE TABLE raw_turns (id INTEGER PRIMARY KEY)`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())
		_, err = sqlitecore.NewDriver(ctx, path)
		Expect(err).To(MatchError(And(ContainSubstring("initialize local SQLite schema"), ContainSubstring("org_id"))))
	})

	It("refuses a second process for the same local database", func() {
		path := filepath.Join(GinkgoT().TempDir(), "locked.sqlite")
		first, err := sqlitecore.NewDriver(ctx, path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(first.Close()).To(Succeed()) })
		_, err = sqlitecore.NewDriver(ctx, path)
		Expect(err).To(MatchError(ContainSubstring("already in use")))
	})

	It("requeues a raw session when identity ingest follows a missing-session derive", func() {
		const sessionID = "pre-ingest-race"
		_, err := d.PutRawTurn(ctx, storage.RawTurnRecord{Source: storage.RawTurnSourceWire, Provider: "anthropic", HarnessID: "claude", HarnessSessionID: sessionID, RequestID: "turn"})
		Expect(err).NotTo(HaveOccurred())
		queued, err := d.GetDeriveDirty(ctx, "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(queued).NotTo(BeNil())
		_, err = d.RederiveSession(ctx, "", "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		cleared, err := d.ClearDeriveDirty(ctx, *queued)
		Expect(err).NotTo(HaveOccurred())
		Expect(cleared).To(BeTrue())

		_, err = d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: sessionID}, Nodes: []*merkle.Node{{Hash: "root"}}})
		Expect(err).NotTo(HaveOccurred())
		queued, err = d.GetDeriveDirty(ctx, "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(queued).NotTo(BeNil())
	})

	It("does not resurrect explicitly deleted sessions during a dirty sweep", func() {
		const sessionID = "deleted-before-sweep"
		_, err := d.PutRawTurn(ctx, storage.RawTurnRecord{Source: storage.RawTurnSourceWire, Provider: "anthropic", HarnessID: "claude", HarnessSessionID: sessionID, RequestID: "turn"})
		Expect(err).NotTo(HaveOccurred())
		result, err := d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: sessionID}, Nodes: []*merkle.Node{{Hash: "root"}}})
		Expect(err).NotTo(HaveOccurred())
		deleted, err := d.DeleteSession(ctx, "", result.SessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
		queued, err := d.GetDeriveDirty(ctx, "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(queued).To(BeNil())
		swept, err := d.SweepDeriveDirty(ctx, time.Now().Add(-time.Hour))
		Expect(err).NotTo(HaveOccurred())
		Expect(swept).To(BeZero())
		queued, err = d.GetDeriveDirty(ctx, "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(queued).To(BeNil())
	})

	It("rejects cyclic session parentage", func() {
		parentB := "b"
		_, err := d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "a", ParentHarnessSessionID: &parentB}, Nodes: []*merkle.Node{{Hash: "a-root"}}})
		Expect(err).NotTo(HaveOccurred())
		parentA := "a"
		_, err = d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "b", ParentHarnessSessionID: &parentA}, Nodes: []*merkle.Node{{Hash: "b-root"}}})
		Expect(err).To(MatchError(ContainSubstring("cycle")))
	})

	It("reconciles transcript sidecars without feeding them to the wire deriver", func() {
		const sessionID = "transcript-sidecar"
		_, err := d.PutRawTurn(ctx, storage.RawTurnRecord{Source: storage.RawTurnSourceTranscript, HarnessID: "claude", HarnessSessionID: sessionID, RawRequest: []byte(`[]`), Meta: []byte(`{"transcript":true,"agent_id":"main"}`)})
		Expect(err).NotTo(HaveOccurred())
		_, err = d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: sessionID}, Nodes: []*merkle.Node{{Hash: "root"}}})
		Expect(err).NotTo(HaveOccurred())
		report, err := d.RederiveSession(ctx, "", "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Reconcile).NotTo(BeNil())
		Expect(report.Reconcile.TranscriptFiles).To(Equal(1))
	})

	It("derives a captured session into the API read model", func() {
		const sessionID = "session-1"
		_, err := d.PutRawTurn(ctx, storage.RawTurnRecord{
			Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "claude",
			HarnessID: "claude", HarnessSessionID: sessionID, RequestID: "turn-1",
			RawRequest: []byte(`{"model":"claude-test","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`),
			Response:   []byte(`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"world"}]},"stop_reason":"end_turn"}`),
			Meta:       []byte(`{"request_id":"turn-1"}`),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: sessionID}, Nodes: []*merkle.Node{{Hash: "root"}}})
		Expect(err).NotTo(HaveOccurred())
		report, err := d.RederiveSession(ctx, "", "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.RawTurns).To(Equal(1))
		session, err := d.GetSessionRecordByHarness(ctx, "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(session).NotTo(BeNil())
		turns, err := d.ListTraceSummaries(ctx, session.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(turns).NotTo(BeEmpty())
	})
})
