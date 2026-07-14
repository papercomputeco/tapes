package postgres_test

import (
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// stringPtr returns a pointer to s. Used inline below to build the
// optional ParentHash field on merkle.Node test fixtures.
func stringPtr(s string) *string { return &s }

var _ = Describe("toMigrateDSN", func() {
	It("preserves sslmode from key-value DSNs", func() {
		dsn := postgres.ToMigrateDSNForTest("host=localhost dbname=tapes sslmode=disable")

		u, err := url.Parse(dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(u.Scheme).To(Equal("pgx5"))
		Expect(u.Query().Get("sslmode")).To(Equal("disable"))
	})
})

var _ = Describe("integer conversions", func() {
	It("handles both 32-bit and 64-bit integer values", func() {
		Expect(postgres.InterfaceInt32ForTest(int32(42))).To(Equal(int32(42)))
		Expect(postgres.InterfaceInt32ForTest(int64(42))).To(Equal(int32(42)))
		Expect(postgres.InterfaceInt64ForTest(int64(42))).To(Equal(int64(42)))
		Expect(postgres.InterfaceInt64ForTest(int32(42))).To(Equal(int64(42)))
	})
})

var _ = Describe("validateChainOrdering", func() {
	It("accepts a single-root chain whose root has no ParentHash", func() {
		nodes := []*merkle.Node{{Hash: "h0"}}
		Expect(postgres.ValidateChainOrderingForTest(nodes)).To(Succeed())
	})

	It("accepts a single-root chain whose root has an empty ParentHash", func() {
		nodes := []*merkle.Node{{Hash: "h0", ParentHash: stringPtr("")}}
		Expect(postgres.ValidateChainOrderingForTest(nodes)).To(Succeed())
	})

	It("accepts a well-ordered multi-node chain", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			{Hash: "h1", ParentHash: stringPtr("h0")},
			{Hash: "h2", ParentHash: stringPtr("h1")},
		}
		Expect(postgres.ValidateChainOrderingForTest(nodes)).To(Succeed())
	})

	It("accepts leading injected context beside the conversation root", func() {
		nodes := []*merkle.Node{
			{Hash: "injected", Kind: "injected:agent-context"},
			{Hash: "h0"},
			{Hash: "h1", ParentHash: stringPtr("h0")},
		}
		Expect(postgres.ValidateChainOrderingForTest(nodes)).To(Succeed())
	})

	It("accepts an injected side branch without advancing the spine", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			{Hash: "injected", Kind: "injected:mode-banner", ParentHash: stringPtr("h0")},
			{Hash: "h1", ParentHash: stringPtr("h0")},
		}
		Expect(postgres.ValidateChainOrderingForTest(nodes)).To(Succeed())
	})

	It("rejects a projection containing only injected nodes", func() {
		nodes := []*merkle.Node{{Hash: "injected", Kind: "injected:agent-context"}}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no conversation spine"))
	})

	It("rejects a chain whose root carries a ParentHash", func() {
		nodes := []*merkle.Node{
			{Hash: "h0", ParentHash: stringPtr("not-a-root")},
			{Hash: "h1", ParentHash: stringPtr("h0")},
		}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nodes[0]"))
	})

	It("rejects a non-root node missing its ParentHash", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			{Hash: "h1"}, // no ParentHash, should chain to h0
		}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nodes[1]"))
	})

	It("rejects a non-root node whose ParentHash is empty", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			{Hash: "h1", ParentHash: stringPtr("")},
		}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nodes[1]"))
	})

	It("rejects a real side branch from the conversation spine", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			{Hash: "h1", ParentHash: stringPtr("h0")},
			{Hash: "h2", ParentHash: stringPtr("h0")}, // not classified as injected
		}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nodes[2]"))
		Expect(err.Error()).To(ContainSubstring("does not chain"))
	})

	It("rejects a chain containing a nil node", func() {
		nodes := []*merkle.Node{
			{Hash: "h0"},
			nil,
		}
		err := postgres.ValidateChainOrderingForTest(nodes)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nodes[1] is nil"))
	})
})
