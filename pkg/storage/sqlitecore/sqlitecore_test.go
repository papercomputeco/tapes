package sqlitecore_test

import (
	"context"
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

	It("refuses a second process for the same local database", func() {
		path := filepath.Join(GinkgoT().TempDir(), "locked.sqlite")
		first, err := sqlitecore.NewDriver(ctx, path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(first.Close()).To(Succeed()) })
		_, err = sqlitecore.NewDriver(ctx, path)
		Expect(err).To(MatchError(ContainSubstring("already in use")))
	})

	It("converges a queued raw session that was deleted before derive", func() {
		const sessionID = "deleted-before-derive"
		_, err := d.PutRawTurn(ctx, storage.RawTurnRecord{Source: storage.RawTurnSourceWire, Provider: "anthropic", HarnessID: "claude", HarnessSessionID: sessionID, RequestID: "turn"})
		Expect(err).NotTo(HaveOccurred())
		result, err := d.IngestTurn(ctx, storage.IngestTurnRequest{Session: &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: sessionID}, Nodes: []*merkle.Node{{Hash: "root"}}})
		Expect(err).NotTo(HaveOccurred())
		deleted, err := d.DeleteSession(ctx, "", result.SessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
		_, err = d.RederiveSession(ctx, "", "", "claude", sessionID)
		Expect(err).NotTo(HaveOccurred())
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
