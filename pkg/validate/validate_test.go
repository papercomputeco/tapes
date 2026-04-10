package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/validate"
)

// ref is a tiny helper that mirrors storage.ParentRef but takes a plain
// string for the parent so tests read more like sentences.
func ref(hash, parentHash string) storage.ParentRef {
	if parentHash == "" {
		return storage.ParentRef{Hash: hash}
	}
	return storage.ParentRef{Hash: hash, ParentHash: &parentHash}
}

var _ = Describe("CheckRefs", func() {
	It("reports a clean store with no cycles or dangling parents", func() {
		refs := []storage.ParentRef{
			ref("root", ""),
			ref("mid", "root"),
			ref("leaf", "mid"),
		}
		report := validate.CheckRefs(refs)

		Expect(report.OK()).To(BeTrue())
		Expect(report.TotalNodes).To(Equal(3))
		Expect(report.Roots).To(Equal(1))
		Expect(report.Cycles).To(BeEmpty())
		Expect(report.Dangling).To(BeEmpty())
	})

	It("detects a simple two-node cycle", func() {
		// A ↔ B with no root — Ancestry() would spin forever on either.
		refs := []storage.ParentRef{
			ref("a", "b"),
			ref("b", "a"),
		}
		report := validate.CheckRefs(refs)

		Expect(report.OK()).To(BeFalse())
		Expect(report.Cycles).To(HaveLen(1))
		// The cycle contains both nodes (order depends on map iteration).
		Expect(report.Cycles[0]).To(ContainElement("a"))
		Expect(report.Cycles[0]).To(ContainElement("b"))
		// The closing element equals the first one — A→B→A shape.
		first := report.Cycles[0][0]
		last := report.Cycles[0][len(report.Cycles[0])-1]
		Expect(first).To(Equal(last))
	})

	It("detects a self-loop", func() {
		refs := []storage.ParentRef{ref("loopy", "loopy")}
		report := validate.CheckRefs(refs)

		Expect(report.Cycles).To(HaveLen(1))
		Expect(report.Cycles[0]).To(Equal([]string{"loopy", "loopy"}))
	})

	It("only reports a cycle once even when multiple leaves feed into it", func() {
		// leaf1 and leaf2 both descend into the B→A→B loop.
		refs := []storage.ParentRef{
			ref("a", "b"),
			ref("b", "a"),
			ref("leaf1", "a"),
			ref("leaf2", "a"),
		}
		report := validate.CheckRefs(refs)

		Expect(report.Cycles).To(HaveLen(1))
	})

	It("flags a dangling parent without reporting a cycle", func() {
		refs := []storage.ParentRef{
			ref("orphan", "missing_parent"),
		}
		report := validate.CheckRefs(refs)

		Expect(report.Cycles).To(BeEmpty())
		Expect(report.Dangling).To(HaveLen(1))
		Expect(report.Dangling[0]).To(Equal(validate.Dangling{
			Hash:       "orphan",
			ParentHash: "missing_parent",
		}))
	})

	It("handles a mix: clean chain, dangling ref, and separate cycle", func() {
		refs := []storage.ParentRef{
			ref("root", ""),
			ref("clean", "root"),
			ref("dangling", "nowhere"),
			ref("cycA", "cycB"),
			ref("cycB", "cycA"),
		}
		report := validate.CheckRefs(refs)

		Expect(report.TotalNodes).To(Equal(5))
		Expect(report.Roots).To(Equal(1))
		Expect(report.Dangling).To(HaveLen(1))
		Expect(report.Cycles).To(HaveLen(1))
	})

	It("treats an empty parent_hash string as a root marker", func() {
		// Some stores may use "" instead of NULL for roots — the check
		// should still count them as roots, not as dangling refs to "".
		refs := []storage.ParentRef{{Hash: "solo", ParentHash: strPtr("")}}
		report := validate.CheckRefs(refs)

		Expect(report.Roots).To(Equal(1))
		Expect(report.Dangling).To(BeEmpty())
		Expect(report.Cycles).To(BeEmpty())
	})
})

func strPtr(s string) *string { return &s }
