package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

var _ = Describe("Driver.AssignSyntheticSession", func() {
	const gap = 30 * time.Minute
	var (
		ctx          context.Context
		driver       *postgres.Driver
		orgID        string
		conversation string
		t0           time.Time
	)
	at := func(minutes int) time.Time { return t0.Add(time.Duration(minutes) * time.Minute) }

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)
		orgID = newTestOrgID()
		conversation = randomHex(8)
		t0 = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	})

	assignIn := func(conv, requestID string, captured time.Time) string {
		key, err := driver.AssignSyntheticSession(ctx, storage.SyntheticSessionRequest{
			OrgID: orgID, HarnessID: sessions.HarnessIDUnknown, Conversation: conv,
			RequestID: requestID, CapturedAt: captured, IdleGap: gap,
		})
		Expect(err).NotTo(HaveOccurred())
		return key
	}
	assign := func(captured time.Time) string { return assignIn(conversation, "", captured) }

	It("keeps a conversation within the idle gap in one session", func() {
		Expect(assign(at(0))).To(Equal(conversation))
		Expect(assign(at(20))).To(Equal(conversation))
		Expect(assign(at(45))).To(Equal(conversation))
	})

	It("starts a new, deterministically keyed session after the idle gap", func() {
		Expect(assign(at(0))).To(Equal(conversation))
		second := assign(at(90))
		Expect(second).To(Equal(conversation + "-20261013T103000Z"))
		Expect(assign(at(100))).To(Equal(second))
		Expect(assign(at(200))).To(Equal(conversation + "-20261013T122000Z"))
	})

	It("lands out-of-order turns where in-order arrival puts them", func() {
		turns := []time.Time{at(0), at(10), at(80), at(85), at(300)}
		inOrder := map[time.Time]string{}
		for _, turn := range turns {
			inOrder[turn] = assign(turn)
		}

		reordered := randomHex(8)
		got := map[time.Time]string{}
		for _, i := range []int{0, 4, 2, 1, 3} {
			got[turns[i]] = assignIn(reordered, "", turns[i])
		}
		for _, turn := range turns {
			Expect(got[turn]).To(Equal(reordered+inOrder[turn][len(conversation):]),
				"turn at %s", turn)
		}
	})

	It("is idempotent for a re-ingested turn", func() {
		first := assign(at(0))
		later := assign(at(60))
		Expect(later).NotTo(Equal(first))

		By("re-assigning the same capture instants")
		Expect(assign(at(0))).To(Equal(first))
		Expect(assign(at(60))).To(Equal(later))

		By("re-assigning a stored turn by its request id")
		_, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic",
			HarnessID: sessions.HarnessIDUnknown, HarnessSessionID: first, RequestID: "req-stored",
			RawRequest: json.RawMessage(`{"model":"m","messages":[]}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(assignIn(conversation, "req-stored", at(500))).To(Equal(first),
			"a turn already stored keeps its session, whatever instant it is re-sent with")
	})

	It("splits a conversation captured before splits were tracked at its next idle gap", func() {
		_, err := driver.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{OrgID: orgID},
			Nodes:   sessionFixture("pre-split " + conversation),
		})
		Expect(err).NotTo(HaveOccurred())
		var legacy string
		Expect(driver.DB().QueryRow(ctx,
			`SELECT harness_session_id FROM sessions WHERE org_id = $1 AND harness_id = 'unknown'`,
			mustUUID(orgID)).Scan(&legacy)).To(Succeed())

		now := time.Now()
		resumed, err := driver.AssignSyntheticSession(ctx, storage.SyntheticSessionRequest{
			OrgID: orgID, HarnessID: sessions.HarnessIDUnknown, Conversation: legacy,
			CapturedAt: now.Add(time.Minute), IdleGap: gap,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(resumed).To(Equal(legacy), "a turn within the gap continues the existing session")

		split, err := driver.AssignSyntheticSession(ctx, storage.SyntheticSessionRequest{
			OrgID: orgID, HarnessID: sessions.HarnessIDUnknown, Conversation: legacy,
			CapturedAt: now.Add(3 * time.Hour), IdleGap: gap,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(split).To(HavePrefix(legacy + "-"))
	})

	It("opens one split for concurrent turns of the same burst", func() {
		Expect(assign(at(0))).To(Equal(conversation))
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			keys = map[string]int{}
		)
		for i := range 8 {
			wg.Go(func() {
				defer GinkgoRecover()
				key := assign(at(120 + i))
				mu.Lock()
				keys[key]++
				mu.Unlock()
			})
		}
		wg.Wait()
		Expect(keys).To(HaveLen(1))
		Expect(keys).NotTo(HaveKey(conversation))
	})

	It("scopes splits to the harness", func() {
		Expect(assign(at(0))).To(Equal(conversation))
		other, err := driver.AssignSyntheticSession(ctx, storage.SyntheticSessionRequest{
			OrgID: orgID, HarnessID: "hermes", Conversation: conversation,
			CapturedAt: at(120), IdleGap: gap,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(other).To(Equal(conversation), "another harness's conversation has its own first split")
	})
})

var _ = Describe("Idle-split capture", func() {
	const gap = 30 * time.Minute
	var (
		ctx    context.Context
		driver *postgres.Driver
		t0     time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions, raw_turns, derive_queue, synthetic_session_splits CASCADE")
		Expect(err).NotTo(HaveOccurred())
		t0 = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	})

	// capture runs one turn through the capture sequence both capture
	// paths share: resolve the key, append the raw turn, upsert the
	// sessions row by the same key.
	// prompts is the conversation so far, oldest first: each turn re-sends
	// the whole history, as a bot with one growing conversation does.
	capture := func(envelope *sessions.IngestEnvelope, rootHash, requestID string, prompts []string, captured time.Time) string {
		var messages []map[string]string
		for i, p := range prompts {
			if i > 0 {
				messages = append(messages, map[string]string{"role": "assistant", "content": "re: " + prompts[i-1]})
			}
			messages = append(messages, map[string]string{"role": "user", "content": p})
		}
		messagesJSON, err := json.Marshal(messages)
		Expect(err).NotTo(HaveOccurred())
		prompt := prompts[len(prompts)-1]
		resolved, key, err2 := storage.ResolveSessionKey(ctx, driver, envelope, storage.SyntheticTurn{
			RootHash: rootHash, RequestID: requestID, CapturedAt: captured, IdleGap: gap,
		})
		Expect(err2).NotTo(HaveOccurred())
		envJSON, err := json.Marshal(resolved)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.PutRawTurn(ctx, storage.RawTurnRecord{
			Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "bot",
			HarnessID: resolved.HarnessIDOrUnknown(), HarnessSessionID: key, RequestID: requestID,
			RawRequest: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","max_tokens":64,"messages":%s}`, messagesJSON)),
			Response: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"re: %s"}]},"stop_reason":"end_turn"}`, prompt)),
			Meta:            json.RawMessage(fmt.Sprintf(`{"ts_request":%q}`, captured.Format(time.RFC3339Nano))),
			SessionEnvelope: envJSON,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: envelope, SessionKey: key, Nodes: sessionFixture(prompt),
		})
		Expect(err).NotTo(HaveOccurred())
		return key
	}

	sessionRows := func(harnessID string) map[string]string {
		rows, err := driver.DB().Query(ctx,
			`SELECT harness_session_id, id::text FROM sessions WHERE harness_id = $1`, harnessID)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var key, id string
			Expect(rows.Scan(&key, &id)).To(Succeed())
			out[key] = id
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return out
	}

	It("splits a header-less bot's growing conversation into a session per burst", func() {
		root := randomHex(32)
		bot := &sessions.IngestEnvelope{HarnessID: "hermes"}
		morning := []string{"morning one"}
		first := capture(bot, root, "req-1", morning, t0)
		history := []string{"morning one", "morning two"}
		capture(bot, root, "req-2", history, t0.Add(5*time.Minute))
		second := capture(bot, root, "req-3", append(history, "evening one"), t0.Add(8*time.Hour))
		// A retry of an earlier turn after the conversation moved on.
		Expect(capture(bot, root, "req-2", history, t0.Add(5*time.Minute))).To(Equal(first))

		Expect(first).To(Equal(sessions.SyntheticHarnessSessionID(root)))
		Expect(second).To(Equal(first + "-20261013T170000Z"))
		rows := sessionRows("hermes")
		Expect(rows).To(HaveLen(2))
		Expect(rows).To(HaveKey(first))
		Expect(rows).To(HaveKey(second))

		By("deriving each split as its own session")
		for _, key := range []string{first, second} {
			_, err := driver.RederiveSession(ctx, "", "", "hermes", key)
			Expect(err).NotTo(HaveOccurred())
		}
		llmCalls := func(sessionID string) int {
			var n int
			Expect(driver.DB().QueryRow(ctx,
				`SELECT COUNT(*) FROM spans_20260615 WHERE session_id = $1 AND kind = 'llm'`, sessionID).Scan(&n)).To(Succeed())
			return n
		}
		Expect(llmCalls(rows[first])).To(Equal(2), "both morning calls project into the first session")
		Expect(llmCalls(rows[second])).To(Equal(1), "the evening call projects into the second")
	})

	It("never splits a harness-provided session id", func() {
		claude := &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "hs-provided"}
		root := randomHex(32)
		Expect(capture(claude, root, "req-a", []string{"one"}, t0)).To(Equal("hs-provided"))
		Expect(capture(claude, root, "req-b", []string{"one", "two"}, t0.Add(48*time.Hour))).To(Equal("hs-provided"))
		Expect(sessionRows("claude")).To(HaveLen(1))

		var splits int
		Expect(driver.DB().QueryRow(ctx, `SELECT COUNT(*) FROM synthetic_session_splits`).Scan(&splits)).To(Succeed())
		Expect(splits).To(BeZero(), "a provided id never enters split bookkeeping")
	})
})
