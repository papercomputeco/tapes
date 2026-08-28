package cassetterunner_test

import (
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
)

// instance builds a minimal registrable cassette. Tests that care about a field
// set it afterwards, so what a spec is about stays visible in the spec.
func instance(name string) *cassetterunner.Instance {
	return &cassetterunner.Instance{
		Name:    cassette.Name(name),
		URL:     "http://127.0.0.1:9000",
		Anchors: cassette.Anchors{Health: "/ping", OpenAPI: "/openapi", Prefix: "api"},
	}
}

var _ = Describe("Instance", func() {
	It("derives its public prefix from its name and its local one from its manifest", func() {
		one := instance("summary")

		Expect(one.Prefix()).To(Equal("/v1/cassettes/summary"),
			"core publishes every cassette inside one namespace, and names it")
		Expect(one.LocalPrefix()).To(Equal("/api/summary"),
			"the cassette serves the prefix its own manifest declared")
	})

	It("mounts a cassette that declares no prefix directly under its own name", func() {
		one := instance("summary")
		one.Anchors.Prefix = ""

		Expect(one.Prefix()).To(Equal("/v1/cassettes/summary"),
			"the public surface is core's to name and does not move")
		Expect(one.LocalPrefix()).To(Equal("/summary"))
	})

	It("maps a path between the public surface and the cassette's own, both ways", func() {
		one := instance("summary")

		Expect(one.Local("/v1/cassettes/summary/reports/7")).To(Equal("/api/summary/reports/7"))
		Expect(one.Public("/api/summary/reports/7")).
			To(Equal("/v1/cassettes/summary/reports/7"))
		Expect(one.Public(one.Local("/v1/cassettes/summary/reports/7"))).
			To(Equal("/v1/cassettes/summary/reports/7"), "the two are inverses")
	})
})

var _ = Describe("Registry", func() {
	var reg *cassetterunner.Registry

	BeforeEach(func() {
		reg = cassetterunner.NewRegistry()
	})

	It("registers a cassette and finds it again", func() {
		Expect(reg.Put(instance("summary"))).To(Succeed())

		found, ok := reg.Get("summary")
		Expect(ok).To(BeTrue())
		Expect(found.Name).To(Equal(cassette.Name("summary")))
		Expect(reg.Rejections()).To(BeEmpty())
	})

	It("returns instances in name order regardless of registration order", func() {
		Expect(reg.Put(instance("summary"))).To(Succeed())
		Expect(reg.Put(instance("alpha"))).To(Succeed())

		names := make([]cassette.Name, 0, 2)
		for _, one := range reg.Instances() {
			names = append(names, one.Name)
		}
		Expect(names).To(Equal([]cassette.Name{"alpha", "summary"}))
	})

	It("replaces a cassette re-resolved under the same name without duplicating it", func() {
		Expect(reg.Put(instance("summary"))).To(Succeed())

		moved := instance("summary")
		moved.URL = "http://127.0.0.1:9001"
		Expect(reg.Put(moved)).To(Succeed())

		Expect(reg.Instances()).To(HaveLen(1),
			"source priority is settled before Put, so what arrives here is always the winner")
		found, _ := reg.Get("summary")
		Expect(found.URL).To(Equal("http://127.0.0.1:9001"))
	})

	It("refuses an endpoint it could not proxy to", func() {
		one := instance("summary")
		one.URL = ""
		Expect(reg.Put(one)).To(MatchError(ContainSubstring("is required")))

		one.URL = "http://127.0.0.1:9000?a=1"
		Expect(reg.Put(one)).To(MatchError(ContainSubstring("must not carry a query or fragment")))

		one.URL = "http://:9000"
		Expect(reg.Put(one)).To(MatchError(ContainSubstring("must include a host")))

		Expect(reg.Instances()).To(BeEmpty())
	})

	It("refuses a name Postgres would not let the cassette own", func() {
		one := instance("public")
		Expect(reg.Put(one)).To(MatchError(ContainSubstring("is reserved")))
	})

	It("records a rejection for a source that never reached registration", func() {
		reg.SetRejection("http://sidecar.invalid/openapi", errUnparsable)
		reg.SetRejection("http://ignored.invalid/openapi", nil)

		Expect(reg.Rejections()).To(ConsistOf(cassetterunner.Rejection{
			Subject: "http://sidecar.invalid/openapi",
			Reason:  errUnparsable.Error(),
		}), "a manifest that would not parse has no name to report under, so the URL is the subject")
	})

	It("replaces a subject's rejection rather than accumulating one per retry", func() {
		reg.SetRejection("http://sidecar.invalid/openapi", errUnparsable)
		reg.SetRejection("http://sidecar.invalid/openapi", errUnparsable)

		Expect(reg.Rejections()).To(HaveLen(1),
			"a source retried every 30 seconds must not grow the discovery document without bound")
	})

	It("clears a rejection once its subject resolves", func() {
		reg.SetRejection("http://sidecar.invalid/openapi", errUnparsable)
		reg.ClearRejection("http://sidecar.invalid/openapi")

		Expect(reg.Rejections()).To(BeEmpty())
	})

	It("returns an empty rejection list rather than a nil one", func() {
		Expect(reg.Rejections()).NotTo(BeNil(),
			"an empty list on the wire means nothing failed; a null makes a client guess")
	})

	Describe("Lookup", func() {
		BeforeEach(func() {
			Expect(reg.Put(instance("summary"))).To(Succeed())
		})

		It("swaps the public prefix for the one the cassette serves", func() {
			found, forwarded, ok := reg.Lookup("/v1/cassettes/summary/reports/7")
			Expect(ok).To(BeTrue())
			Expect(found.Name).To(Equal(cassette.Name("summary")))
			Expect(forwarded).To(Equal("/api/summary/reports/7"),
				"this is the one declared rewrite in the pipeline")
		})

		It("matches the prefix itself, not only paths beneath it", func() {
			_, forwarded, ok := reg.Lookup("/v1/cassettes/summary")
			Expect(ok).To(BeTrue())
			Expect(forwarded).To(Equal("/api/summary"))
		})

		It("does not claim a path that merely starts with the same characters", func() {
			_, _, ok := reg.Lookup("/v1/cassettes/summaryzzz")
			Expect(ok).To(BeFalse(), "/v1/cassettes/sum must not shadow /v1/cassettes/summary")
		})

		It("does not claim a path no cassette serves", func() {
			_, _, ok := reg.Lookup("/v1/sessions")
			Expect(ok).To(BeFalse())
		})
	})
})

// errUnparsable stands in for a manifest that failed before it had a name.
var errUnparsable = &cassette.ValidationError{
	Subject:  "cassette http://sidecar.invalid/openapi",
	Problems: []cassette.Problem{{Field: "kind", Message: "is required"}},
}

var _ = Describe("filter-claim arming state", func() {
	var reg *cassetterunner.Registry

	// claimingManifest publishes one view and claims the note param against
	// it, so specs can vary the probed fields by varying the view.
	claimingManifest := func(view string) *v1alpha1.Manifest {
		GinkgoHelper()
		parsed, err := v1alpha1.Parse(fmt.Appendf(nil, `{
		  "kind":"cassette/v1alpha1",
		  "cassette":{"name":"notes","version":"1.0.0"},
		  "depends":{"core":"v1"},
		  "api":{"health":"/ping","openapi":"/openapi"},
		  "publishes":{
		    "views":[%[1]q],
		    "filters":[{
		      "param":"note",
		      "surface":"sessions",
		      "view":%[1]q,
		      "match":{"primitive_type":"session","value_column":"value"}
		    }]
		  }
		}`, view))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())

		return parsed
	}

	putClaiming := func(manifest *v1alpha1.Manifest) cassetterunner.ActiveClaim {
		GinkgoHelper()
		one := instance("notes")
		one.Manifest = manifest
		Expect(reg.Put(one)).To(Succeed())
		claims := cassetterunner.ManifestClaims("notes", manifest)
		Expect(claims).To(HaveLen(1))

		return claims[0]
	}

	BeforeEach(func() {
		reg = cassetterunner.NewRegistry()
	})

	It("withholds an admitted claim from ClaimsFor until it is armed", func() {
		claim := putClaiming(claimingManifest("notes_v1.attachments"))

		Expect(reg.ClaimsFor("sessions")).To(BeEmpty(),
			"admission records ownership; only arming makes a claim executable")
		Expect(reg.ArmClaim(claim)).To(BeTrue(),
			"the first arm is an effective-claim-set change")
		Expect(reg.ClaimsFor("sessions")).To(HaveLen(1))
		Expect(reg.ArmClaim(claim)).To(BeFalse(),
			"re-arming an armed claim changes nothing")
	})

	It("files a claim rejection on disarm and clears it on arm", func() {
		claim := putClaiming(claimingManifest("notes_v1.attachments"))

		Expect(reg.DisarmClaim(claim, errors.New("view missing"))).To(BeFalse(),
			"a never-armed claim does not change the effective set by staying un-armed")
		Expect(reg.Rejections()).To(ConsistOf(cassetterunner.Rejection{
			Subject: `cassette notes: claim "note"`,
			Reason:  "view missing",
		}))

		Expect(reg.DisarmClaim(claim, errors.New("grant revoked"))).To(BeFalse())
		Expect(reg.Rejections()).To(HaveLen(1),
			"a claim retried every refresh must not grow the discovery document without bound")
		Expect(reg.Rejections()[0].Reason).To(Equal("grant revoked"),
			"the reason tracks the latest probe verdict")

		Expect(reg.ArmClaim(claim)).To(BeTrue())
		Expect(reg.Rejections()).To(BeEmpty(),
			"an armed claim has nothing to report")

		Expect(reg.DisarmClaim(claim, errors.New("view dropped"))).To(BeTrue(),
			"armed to un-armed is an effective-claim-set change")
		Expect(reg.Rejections()).To(HaveLen(1))
	})

	It("keeps armed state across a re-registration with unchanged claims", func() {
		claim := putClaiming(claimingManifest("notes_v1.attachments"))
		Expect(reg.ArmClaim(claim)).To(BeTrue())

		putClaiming(claimingManifest("notes_v1.attachments"))

		Expect(reg.ClaimsFor("sessions")).To(HaveLen(1),
			"a steady-state re-registration is not an arming event and must not disarm anything")
	})

	It("requires a fresh probe verdict when a claim's probed fields change", func() {
		claim := putClaiming(claimingManifest("notes_v1.attachments"))
		Expect(reg.ArmClaim(claim)).To(BeTrue())

		putClaiming(claimingManifest("notes_v1.renamed"))

		Expect(reg.ClaimsFor("sessions")).To(BeEmpty(),
			"the earlier probe proved a different view; the moved claim starts unproved")
	})
})
