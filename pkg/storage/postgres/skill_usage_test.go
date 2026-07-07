package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver.AggregateSkillUsage", func() {
	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turns RESTART IDENTITY")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// mintSession creates the sessions row for the harness identity the
	// way production ingest does — the span projection only writes rows
	// for resolved sessions, so raw turns alone are not enough.
	mintSession := func(orgID, harnessSessionID string) {
		ingester, ok := storage.Driver(driver).(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")
		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-skill-usage",
				HarnessID:        "claude-code",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture("skill usage seed " + harnessSessionID),
		})
		Expect(err).NotTo(HaveOccurred())
	}

	// seedSkillTurn captures one wire turn whose assistant response
	// invokes the Skill tool (plus a Bash call as non-skill noise), the
	// same reduced ChatResponse shape the extproc dispatches. Spans are
	// projected from these rows by RederiveFromRaw below — the same
	// path production takes — so the assertions cover both the
	// emitter's Skill span contract and the aggregate SQL.
	seedSkillTurn := func(orgID, harnessSessionID, requestID, toolUseID, skill string) {
		response := fmt.Sprintf(`{"model":"claude-test","message":{"role":"assistant","content":[`+
			`{"type":"text","text":"invoking the skill"},`+
			`{"type":"tool_use","tool_use_id":%q,"tool_name":"Skill","tool_input":{"skill":%q}},`+
			`{"type":"tool_use","tool_use_id":"%s_noise","tool_name":"Bash","tool_input":{"command":"ls"}}`+
			`]},"stop_reason":"tool_use"}`, toolUseID, skill, toolUseID)
		envelope := fmt.Sprintf(`{"org_id":%q,"harness_id":"claude-code","harness_session_id":%q}`, orgID, harnessSessionID)

		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID:            orgID,
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        "claude-code",
			HarnessSessionID: harnessSessionID,
			RequestID:        requestID,
			// stream + a non-empty tool set classify the call as a main
			// conversation turn (ClassifyCall) so the span projection
			// covers it — the same shape a real harness call has.
			RawRequest:       json.RawMessage(`{"model":"claude-test","max_tokens":4096,"stream":true,"tools":[{"name":"Skill","description":"Execute a skill","input_schema":{"type":"object"}},{"name":"Bash","description":"Run a command","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"please run the skill"}]}`),
			Response:         json.RawMessage(response),
			Meta:             json.RawMessage(`{"request_id":"` + requestID + `","model":"claude-test","stream":"false","upstream_status":200}`),
			SessionEnvelope:  json.RawMessage(envelope),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
	}

	It("groups Skill invocations by skill name, scoped to org and window", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()

		// Org A: two grove-refresh invocations in one session, one
		// dagger-check in another. Org B: one grove-refresh.
		mintSession(orgA, "sess-a1")
		mintSession(orgA, "sess-a2")
		mintSession(orgB, "sess-b1")
		seedSkillTurn(orgA, "sess-a1", "req-a1", "toolu_a1", "grove-refresh")
		seedSkillTurn(orgA, "sess-a1", "req-a2", "toolu_a2", "grove-refresh")
		seedSkillTurn(orgA, "sess-a2", "req-a3", "toolu_a3", "dagger-check")
		seedSkillTurn(orgB, "sess-b1", "req-b1", "toolu_b1", "grove-refresh")

		_, err := driver.RederiveFromRaw(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		usage, err := driver.AggregateSkillUsage(ctx, orgA, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(usage).To(HaveLen(2), "Bash noise spans and org B rows must not leak in")

		Expect(usage[0].Skill).To(Equal("grove-refresh"), "most-invoked skill first")
		Expect(usage[0].Invocations).To(Equal(2))
		Expect(usage[0].SessionCount).To(Equal(1))
		Expect(usage[0].LastUsedAt).NotTo(BeZero())

		Expect(usage[1].Skill).To(Equal("dagger-check"))
		Expect(usage[1].Invocations).To(Equal(1))
		Expect(usage[1].SessionCount).To(Equal(1))

		// Org isolation: org B sees only its own invocation.
		usageB, err := driver.AggregateSkillUsage(ctx, orgB, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(usageB).To(HaveLen(1))
		Expect(usageB[0].Skill).To(Equal("grove-refresh"))
		Expect(usageB[0].Invocations).To(Equal(1))

		// Window filtering: a window entirely in the future is empty, a
		// broad window returns everything.
		future := time.Now().Add(time.Hour)
		empty, err := driver.AggregateSkillUsage(ctx, orgA, &future, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(empty).To(BeEmpty())

		past := time.Now().Add(-time.Hour)
		windowed, err := driver.AggregateSkillUsage(ctx, orgA, &past, &future)
		Expect(err).NotTo(HaveOccurred())
		Expect(windowed).To(HaveLen(2))
	})

	It("is idempotent across re-derives — counts stay per unique invocation", func() {
		orgID := newTestOrgID()
		mintSession(orgID, "sess-1")
		seedSkillTurn(orgID, "sess-1", "req-1", "toolu_1", "grove-refresh")

		_, err := driver.RederiveFromRaw(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.RederiveFromRaw(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		usage, err := driver.AggregateSkillUsage(ctx, orgID, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(usage).To(HaveLen(1))
		Expect(usage[0].Invocations).To(Equal(1), "re-derive must upsert, not duplicate")
	})
})
