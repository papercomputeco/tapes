package cassette_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

var _ = Describe("ParseName", func() {
	It("accepts a lowercase name with interior digits and dashes", func() {
		name, err := cassette.ParseName("hello-world2")
		Expect(err).NotTo(HaveOccurred())
		Expect(name).To(Equal(cassette.Name("hello-world2")))
	})

	DescribeTable("refuses a name core could not route or own a schema for",
		func(value, because string) {
			_, err := cassette.ParseName(value)
			Expect(err).To(HaveOccurred(), because)
		},
		Entry("empty", "", "a cassette with no name has no prefix and no schema"),
		Entry("single character", "a", "the pattern requires a trailing alphanumeric after the first"),
		Entry("leading digit", "1summary", "a name has to be a legal Postgres identifier"),
		Entry("uppercase", "Summary", "Postgres would fold the case and the route would not"),
		Entry("underscore", "hello_world", "the route segment and the schema name are the same string"),
		Entry("trailing dash", "summary-", "a trailing dash makes the prefix ambiguous"),
		Entry("pg_ prefix", "pg_catalog", "Postgres reserves the pg_ namespace"),
		Entry("too long", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "33 characters exceeds the 32-byte limit"),
	)

	// Cassettes are served beneath /v1/cassettes/<name>, so a route-shaped name
	// cannot shadow a core route: "ping" answers at /v1/cassettes/ping and
	// leaves /ping alone. Reserving these bought nothing and took the obvious
	// name away from whoever wanted it.
	DescribeTable("allows a name that merely looks like a core route",
		func(value string) {
			name, err := cassette.ParseName(value)
			Expect(err).NotTo(HaveOccurred())
			Expect(name).To(Equal(cassette.Name(value)))
		},
		Entry("version route", "v1"),
		Entry("cassette namespace", "cassettes"),
		Entry("health route", "ping"),
		Entry("aggregate document", "openapi"),
		Entry("metrics route", "metrics"),
		Entry("local prefix namespace", "api"),
	)

	// Postgres is the namespace a name genuinely cannot escape: it becomes the
	// cassette's schema directly, in a database core also lives in.
	DescribeTable("refuses a name that would collide in Postgres",
		func(value string) {
			_, err := cassette.ParseName(value)
			Expect(err).To(HaveOccurred())
		},
		Entry("the schema core's own tables live in", "public"),
		Entry("core's schema and role prefix", "tapes"),
		Entry("the Postgres-reserved prefix", "pg_temp"),
	)
})

var _ = Describe("Name derivations", func() {
	const name = cassette.Name("summary")

	It("derives a schema and a role that cannot be confused for each other", func() {
		Expect(name.SchemaName()).To(Equal("summary"))
		Expect(name.RoleName()).To(Equal("cassette_summary"))
	})
})
