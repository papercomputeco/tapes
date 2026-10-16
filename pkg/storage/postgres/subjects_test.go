package postgres_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver.ListSubjects [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
		orgID  string
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)
		orgID = newTestOrgID()
	})

	seedFor := func(subject, harnessSession string) {
		_, err := driver.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID: orgID, AuthSubject: subject,
				HarnessID: "claude", HarnessSessionID: harnessSession,
			},
			Nodes: sessionFixture(harnessSession + " turn"),
		})
		Expect(err).NotTo(HaveOccurred())
	}

	It("lists each subject once with its session count", func() {
		seedFor("local:alice", "sess-alice-1")
		seedFor("local:alice", "sess-alice-2")
		seedFor("local:bob", "sess-bob")
		seedFor("", "sess-anonymous")

		subjects, err := driver.ListSubjects(ctx, orgID, "", 10)
		Expect(err).NotTo(HaveOccurred())
		counts := map[string]int64{}
		for _, s := range subjects {
			counts[s.Subject] = s.SessionCount
		}
		Expect(counts).To(Equal(map[string]int64{"local:alice": 2, "local:bob": 1}))
	})

	It("filters by a case-insensitive substring of the id", func() {
		seedFor("local:alice", "sess-alice-q")
		seedFor("local:bob", "sess-bob-q")

		subjects, err := driver.ListSubjects(ctx, orgID, "ALI", 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(subjects).To(HaveLen(1))
		Expect(subjects[0].Subject).To(Equal("local:alice"))
	})
})
