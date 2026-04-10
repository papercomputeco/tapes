package inmemory_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/storagetest"
)

var _ = storagetest.RunListSessionsSpecs("inmemory", func() storage.Driver {
	return inmemory.NewDriver()
})

var _ = storagetest.RunAncestryChainBasicSpecs("inmemory", func() storage.Driver {
	return inmemory.NewDriver()
})

var _ = storagetest.RunAncestryChainDanglingSpecs("inmemory", func() storage.Driver {
	return inmemory.NewDriver()
})

var _ = storagetest.RunAncestryChainsSpecs("inmemory", func() storage.Driver {
	return inmemory.NewDriver()
})

// Cycle guard tests. Content-addressing makes a real cycle structurally
// impossible to create via merkle.NewNode — A's hash depends on B's hash
// and vice versa — so the test hand-crafts *merkle.Node values with
// mismatched Hash/ParentHash pairs and Put()s them directly. This
// simulates the state a corrupt bulk import could leave behind and
// verifies the walk guard stops before spinning.
var _ = Describe("AncestryChain cycle guard [inmemory]", func() {
	var (
		ctx    context.Context
		driver *inmemory.Driver
	)

	BeforeEach(func() {
		ctx = context.Background()
		driver = inmemory.NewDriver()
	})

	AfterEach(func() {
		_ = driver.Close()
	})

	minimalBucket := func() merkle.Bucket {
		return merkle.Bucket{
			Type:    "message",
			Role:    "user",
			Content: []llm.ContentBlock{{Type: "text", Text: "cycle test"}},
		}
	}

	makeCyclicPair := func(hashA, hashB string) (*merkle.Node, *merkle.Node) {
		return &merkle.Node{
				Hash:       hashA,
				ParentHash: &hashB,
				Bucket:     minimalBucket(),
			}, &merkle.Node{
				Hash:       hashB,
				ParentHash: &hashA,
				Bucket:     minimalBucket(),
			}
	}

	It("stops a two-node cycle instead of looping forever", func() {
		hashA := "aaaa000000000000000000000000000000000000000000000000000000000000"
		hashB := "bbbb000000000000000000000000000000000000000000000000000000000000"
		nodeA, nodeB := makeCyclicPair(hashA, hashB)
		_, err := driver.Put(ctx, nodeA)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.Put(ctx, nodeB)
		Expect(err).NotTo(HaveOccurred())

		chain, err := driver.AncestryChain(ctx, hashA)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Nodes).To(HaveLen(2))
		Expect(chain.Incomplete).To(BeTrue())
		Expect(chain.CycleDetected).To(BeTrue())
		Expect(chain.MissingParent).To(BeEmpty())
		Expect(chain.Complete()).To(BeFalse())
	})

	It("stops a self-loop (node whose parent_hash points at itself)", func() {
		hashSelf := "cccc000000000000000000000000000000000000000000000000000000000000"
		parentPtr := hashSelf
		node := &merkle.Node{
			Hash:       hashSelf,
			ParentHash: &parentPtr,
			Bucket:     minimalBucket(),
		}
		_, err := driver.Put(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		chain, err := driver.AncestryChain(ctx, hashSelf)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Nodes).To(HaveLen(1))
		Expect(chain.CycleDetected).To(BeTrue())
		Expect(chain.Incomplete).To(BeTrue())
	})

	It("isolates a cycle in one leaf from a clean leaf in the same batch", func() {
		hashA := "aaaa111111111111111111111111111111111111111111111111111111111111"
		hashB := "bbbb111111111111111111111111111111111111111111111111111111111111"
		nodeA, nodeB := makeCyclicPair(hashA, hashB)
		_, err := driver.Put(ctx, nodeA)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.Put(ctx, nodeB)
		Expect(err).NotTo(HaveOccurred())

		cleanRoot := merkle.NewNode(minimalBucket(), nil, merkle.NodeOptions{Project: "tapes"})
		_, err = driver.Put(ctx, cleanRoot)
		Expect(err).NotTo(HaveOccurred())
		cleanLeaf := merkle.NewNode(minimalBucket(), cleanRoot, merkle.NodeOptions{Project: "tapes"})
		_, err = driver.Put(ctx, cleanLeaf)
		Expect(err).NotTo(HaveOccurred())

		chains, err := driver.AncestryChains(ctx, []string{hashA, cleanLeaf.Hash})
		Expect(err).NotTo(HaveOccurred())
		Expect(chains).To(HaveLen(2))

		cycleChain := chains[hashA]
		Expect(cycleChain.Incomplete).To(BeTrue())
		Expect(cycleChain.CycleDetected).To(BeTrue())
		Expect(cycleChain.Nodes).To(HaveLen(2))

		cleanChain := chains[cleanLeaf.Hash]
		Expect(cleanChain.Incomplete).To(BeFalse())
		Expect(cleanChain.CycleDetected).To(BeFalse())
		Expect(cleanChain.Nodes).To(HaveLen(2))
	})
})
