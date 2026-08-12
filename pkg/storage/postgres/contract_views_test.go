package postgres_test

// The tapes_v1 contract: cassettes are granted SELECT on tapes_v1 views and
// never on the physical tables they front, so the views must always point at
// the projection generation the deriver actually writes. These specs pin the
// convention that a migration registering a new generation in
// derived_projection_schemas also repoints the views — forgetting that would
// silently starve every deployed cassette (the grant still succeeds, the
// queries still run, the data quietly stops being current).

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("tapes_v1 contract views [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)
	})

	// baseTables resolves the ordinary tables a view's rewrite rule reads,
	// straight from the catalogs. pg_get_viewdef would need string parsing;
	// pg_depend is the planner's own answer to "what does this view front".
	baseTables := func(view string) []string {
		rows, err := driver.DB().Query(ctx, `
			SELECT DISTINCT ref.relname
			FROM pg_rewrite rw
			JOIN pg_depend dep
			  ON dep.objid = rw.oid AND dep.classid = 'pg_rewrite'::regclass
			JOIN pg_class ref
			  ON ref.oid = dep.refobjid AND dep.refclassid = 'pg_class'::regclass
			WHERE rw.ev_class = $1::regclass
			  AND ref.relkind = 'r'
		`, view)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			Expect(rows.Scan(&name)).To(Succeed())
			names = append(names, name)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return names
	}

	It("fronts the registry's newest projection generation", func() {
		var spansTable, spanTurnsTable, spanLinksTable string
		err := driver.DB().QueryRow(ctx, `
			SELECT spans_table, span_turns_table, span_links_table
			FROM derived_projection_schemas
			ORDER BY compatibility_date DESC
			LIMIT 1
		`).Scan(&spansTable, &spanTurnsTable, &spanLinksTable)
		Expect(err).NotTo(HaveOccurred())

		// One base table each, and it is the registered one. A future
		// generation migration that inserts a new registry row without
		// CREATE OR REPLACE-ing these views fails here, which is the
		// entire reason this spec exists.
		Expect(baseTables("tapes_v1.spans")).To(ConsistOf(spansTable))
		Expect(baseTables("tapes_v1.span_turns")).To(ConsistOf(spanTurnsTable))
		Expect(baseTables("tapes_v1.span_links")).To(ConsistOf(spanLinksTable))
	})

	It("fronts sessions through the same contract shape", func() {
		// sessions is not generation-versioned today, but the contract is
		// uniform: one schema, one grant shape, no physical names.
		Expect(baseTables("tapes_v1.sessions")).To(ConsistOf("sessions"))
	})

	It("serves every contract view to a plain SELECT", func() {
		// The freshly migrated database may be empty; what is being proven
		// is that the views are well-formed against the real tables — a
		// column renamed or dropped out from under an explicit view column
		// list fails the migration itself, and a view accidentally created
		// against a stale name fails here.
		for _, view := range []string{"sessions", "spans", "span_turns", "span_links"} {
			var count int64
			err := driver.DB().QueryRow(ctx,
				"SELECT count(*) FROM tapes_v1."+view).Scan(&count)
			Expect(err).NotTo(HaveOccurred(), "tapes_v1.%s must be selectable", view)
		}
	})
})
