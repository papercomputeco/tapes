// Package storagetest provides shared Ginkgo specs that any storage.Driver
// implementation can run against. It is intended for use only from _test.go
// files of driver packages.
package storagetest

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// DriverFactory builds a fresh, migrated driver. Called once per spec via
// BeforeEach so each spec gets an empty store.
type DriverFactory func() storage.Driver

// RunListSessionsSpecs registers a Describe block exercising ListSessions and
// CountSessions against the driver returned by makeDriver. Call from a _test.go
// file inside a `var _ = ...` initializer (or from inside an existing Describe).
//
// The label is included in the Describe text so failures from multiple drivers
// can be told apart in test output.
func RunListSessionsSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("ListSessions/CountSessions ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		ginkgo.Describe("empty store", func() {
			ginkgo.It("returns an empty page with no cursor", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(page.Items).To(gomega.BeEmpty())
				gomega.Expect(page.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("counts zero", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(stats).To(gomega.Equal(storage.SessionStats{}))
			})
		})

		ginkgo.Describe("with a small set of sessions", func() {
			var (
				baseTime time.Time
				root     *merkle.Node
				leafA    *merkle.Node // newest
				leafB    *merkle.Node
				leafC    *merkle.Node // oldest
			)

			ginkgo.BeforeEach(func() {
				baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

				// root has children, so it is NOT a leaf.
				root = makeNode("root", nil, baseTime, "tapes", "claude", "claude-opus-4-6", "anthropic")
				putWith(ctx, driver, root)

				// Three distinct branches off of root → three leaves.
				leafC = makeNode("leaf-c", &root.Hash, baseTime.Add(1*time.Minute), "tapes", "claude", "claude-opus-4-6", "anthropic")
				leafB = makeNode("leaf-b", &root.Hash, baseTime.Add(2*time.Minute), "other", "opencode", "claude-sonnet-4-6", "anthropic")
				leafA = makeNode("leaf-a", &root.Hash, baseTime.Add(3*time.Minute), "tapes", "claude", "gpt-4o", "openai")

				putWith(ctx, driver, leafC)
				putWith(ctx, driver, leafB)
				putWith(ctx, driver, leafA)
			})

			ginkgo.It("returns leaves only, ordered newest first", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.Equal([]string{leafA.Hash, leafB.Hash, leafC.Hash}))
				gomega.Expect(page.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("excludes internal nodes (root has children)", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, n := range page.Items {
					gomega.Expect(n.Hash).NotTo(gomega.Equal(root.Hash))
				}
			})

			ginkgo.It("paginates with limit and cursor without dropping or duplicating items", func() {
				page1, err := driver.ListSessions(ctx, storage.ListOpts{Limit: 2})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page1.Items)).To(gomega.Equal([]string{leafA.Hash, leafB.Hash}))
				gomega.Expect(page1.NextCursor).NotTo(gomega.BeEmpty())

				page2, err := driver.ListSessions(ctx, storage.ListOpts{Limit: 2, Cursor: page1.NextCursor})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page2.Items)).To(gomega.Equal([]string{leafC.Hash}))
				gomega.Expect(page2.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("filters by project", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Project: "tapes"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash, leafC.Hash))
			})

			ginkgo.It("filters by agent", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Agent: "opencode"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafB.Hash))
			})

			ginkgo.It("filters by model", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Model: "gpt-4o"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash))
			})

			ginkgo.It("filters by provider", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Provider: "anthropic"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafB.Hash, leafC.Hash))
			})

			ginkgo.It("filters by since (inclusive)", func() {
				since := baseTime.Add(2 * time.Minute)
				page, err := driver.ListSessions(ctx, storage.ListOpts{Since: &since})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash, leafB.Hash))
			})

			ginkgo.It("filters by until (exclusive)", func() {
				until := baseTime.Add(2 * time.Minute)
				page, err := driver.ListSessions(ctx, storage.ListOpts{Until: &until})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// leafC at base+1m, leafB at base+2m (excluded), leafA at base+3m (excluded)
				// root is at base+0m but is not a leaf.
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafC.Hash))
			})

			ginkgo.It("combines filters with AND semantics", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{
					Project:  "tapes",
					Provider: "anthropic",
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafC.Hash))
			})

			ginkgo.It("CountSessions reports correct totals", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(stats.SessionCount).To(gomega.Equal(3)) // 3 leaves
				gomega.Expect(stats.TurnCount).To(gomega.Equal(4))    // root + 3 leaves
				gomega.Expect(stats.RootCount).To(gomega.Equal(1))
			})

			ginkgo.It("CountSessions respects filters", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{Project: "tapes"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Project filter applies per-node:
				// - 3 nodes have project=tapes (root, leafA, leafC)
				// - 2 of those are leaves (leafA, leafC)
				// - 1 of those is a root
				gomega.Expect(stats.SessionCount).To(gomega.Equal(2))
				gomega.Expect(stats.TurnCount).To(gomega.Equal(3))
				gomega.Expect(stats.RootCount).To(gomega.Equal(1))
			})

			ginkgo.It("rejects an invalid cursor", func() {
				_, err := driver.ListSessions(ctx, storage.ListOpts{Cursor: "not-a-real-cursor!!!"})
				gomega.Expect(err).To(gomega.HaveOccurred())
			})
		})
	})
}

// makeNode constructs a merkle.Node with deterministic content keyed off id.
// CreatedAt is set explicitly so tests are not dependent on wall-clock granularity.
func makeNode(id string, parentHash *string, createdAt time.Time, project, agent, model, provider string) *merkle.Node {
	bucket := merkle.Bucket{
		Type:      "message",
		Role:      "user",
		Content:   []llm.ContentBlock{{Type: "text", Text: id}},
		Model:     model,
		Provider:  provider,
		AgentName: agent,
	}
	var parent *merkle.Node
	if parentHash != nil {
		// Synthesise a stand-in parent so NewNode can hash with the right link.
		parent = &merkle.Node{Hash: *parentHash}
	}
	n := merkle.NewNode(bucket, parent, merkle.NodeOptions{Project: project})
	n.CreatedAt = createdAt
	return n
}

// RunAncestryChainBasicSpecs registers Describe blocks exercising the paths
// of AncestryChain that don't require injecting an orphan: complete walks,
// single-root chains, and NotFound on a missing starting hash. These run
// against every driver regardless of whether it can bypass referential
// integrity to create a dangling parent.
func RunAncestryChainBasicSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("AncestryChain basics ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		base := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

		ginkgo.It("returns a complete chain when the walk reaches a real root", func() {
			root := makeNode("root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)
			mid := makeNode("mid", &root.Hash, base.Add(time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, mid)
			leaf := makeNode("leaf", &mid.Hash, base.Add(2*time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, leaf)

			chain, err := driver.AncestryChain(ctx, leaf.Hash)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chain.Nodes).To(gomega.HaveLen(3))
			gomega.Expect(chain.Incomplete).To(gomega.BeFalse())
			gomega.Expect(chain.MissingParent).To(gomega.BeEmpty())
			gomega.Expect(chain.Complete()).To(gomega.BeTrue())
			// Node-first order.
			gomega.Expect(chain.Nodes[0].Hash).To(gomega.Equal(leaf.Hash))
			gomega.Expect(chain.Nodes[2].Hash).To(gomega.Equal(root.Hash))
		})

		ginkgo.It("returns a single-node chain for a root with no parent", func() {
			root := makeNode("root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)

			chain, err := driver.AncestryChain(ctx, root.Hash)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chain.Nodes).To(gomega.HaveLen(1))
			gomega.Expect(chain.Incomplete).To(gomega.BeFalse())
		})

		ginkgo.It("returns an error when the starting hash itself is missing", func() {
			_, err := driver.AncestryChain(ctx, "does-not-exist")
			gomega.Expect(err).To(gomega.HaveOccurred())
		})
	})
}

// RunAncestryChainDanglingSpecs exercises the dangling-parent path of
// AncestryChain. Only drivers without enforced referential integrity (i.e.
// the in-memory driver) can accept an orphan via Put — the sqlite driver
// has a foreign-key constraint that rejects them. Drivers with FK enforcement
// must inject orphans by bypassing Put and should register these specs
// manually from their own test file with a dedicated setup.
func RunAncestryChainDanglingSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("AncestryChain dangling parents ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		base := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

		ginkgo.It("marks the chain incomplete when a parent_hash is missing", func() {
			phantom := "0000000000000000000000000000000000000000000000000000000000000000"
			orphan := makeNode("orphan", &phantom, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, orphan)
			child := makeNode("child", &orphan.Hash, base.Add(time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, child)

			chain, err := driver.AncestryChain(ctx, child.Hash)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chain.Nodes).To(gomega.HaveLen(2))
			gomega.Expect(chain.Incomplete).To(gomega.BeTrue())
			gomega.Expect(chain.MissingParent).To(gomega.Equal(phantom))
			gomega.Expect(chain.Complete()).To(gomega.BeFalse())
			gomega.Expect(chain.Nodes[1].Hash).To(gomega.Equal(orphan.Hash))
		})

		ginkgo.It("marks a single orphan node incomplete without returning an error", func() {
			phantom := "1111111111111111111111111111111111111111111111111111111111111111"
			orphan := makeNode("solo-orphan", &phantom, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, orphan)

			chain, err := driver.AncestryChain(ctx, orphan.Hash)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chain.Nodes).To(gomega.HaveLen(1))
			gomega.Expect(chain.Incomplete).To(gomega.BeTrue())
			gomega.Expect(chain.MissingParent).To(gomega.Equal(phantom))
		})
	})
}

// RunAncestryChainsSpecs exercises the batched AncestryChains path against
// the driver returned by makeDriver.
func RunAncestryChainsSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("AncestryChains ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		base := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

		ginkgo.It("returns an empty map for an empty input slice", func() {
			chains, err := driver.AncestryChains(ctx, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chains).To(gomega.BeEmpty())
		})

		ginkgo.It("walks multiple leaves sharing a root in one call", func() {
			root := makeNode("shared-root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)
			leafA := makeNode("leaf-a", &root.Hash, base.Add(time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, leafA)
			leafB := makeNode("leaf-b", &root.Hash, base.Add(2*time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, leafB)

			chains, err := driver.AncestryChains(ctx, []string{leafA.Hash, leafB.Hash})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chains).To(gomega.HaveLen(2))

			chainA := chains[leafA.Hash]
			gomega.Expect(chainA).NotTo(gomega.BeNil())
			gomega.Expect(chainA.Nodes).To(gomega.HaveLen(2))
			gomega.Expect(chainA.Incomplete).To(gomega.BeFalse())
			gomega.Expect(chainA.Nodes[0].Hash).To(gomega.Equal(leafA.Hash))
			gomega.Expect(chainA.Nodes[1].Hash).To(gomega.Equal(root.Hash))

			chainB := chains[leafB.Hash]
			gomega.Expect(chainB).NotTo(gomega.BeNil())
			gomega.Expect(chainB.Nodes).To(gomega.HaveLen(2))
			gomega.Expect(chainB.Nodes[0].Hash).To(gomega.Equal(leafB.Hash))
			gomega.Expect(chainB.Nodes[1].Hash).To(gomega.Equal(root.Hash))
		})

		ginkgo.It("walks chains of different depths in the same batch", func() {
			root := makeNode("deep-root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)
			mid := makeNode("deep-mid", &root.Hash, base.Add(time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, mid)
			deepLeaf := makeNode("deep-leaf", &mid.Hash, base.Add(2*time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, deepLeaf)

			shallowRoot := makeNode("shallow-root", nil, base.Add(3*time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, shallowRoot)
			shallowLeaf := makeNode("shallow-leaf", &shallowRoot.Hash, base.Add(4*time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, shallowLeaf)

			chains, err := driver.AncestryChains(ctx, []string{deepLeaf.Hash, shallowLeaf.Hash})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chains[deepLeaf.Hash].Nodes).To(gomega.HaveLen(3))
			gomega.Expect(chains[deepLeaf.Hash].Incomplete).To(gomega.BeFalse())
			gomega.Expect(chains[shallowLeaf.Hash].Nodes).To(gomega.HaveLen(2))
			gomega.Expect(chains[shallowLeaf.Hash].Incomplete).To(gomega.BeFalse())
		})

		ginkgo.It("dedupes duplicate input hashes", func() {
			root := makeNode("dup-root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)
			leaf := makeNode("dup-leaf", &root.Hash, base.Add(time.Second), "tapes", "claude", "m", "p")
			putWith(ctx, driver, leaf)

			chains, err := driver.AncestryChains(ctx, []string{leaf.Hash, leaf.Hash, leaf.Hash})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chains).To(gomega.HaveLen(1))
			gomega.Expect(chains[leaf.Hash].Nodes).To(gomega.HaveLen(2))
		})

		ginkgo.It("omits unknown input hashes from the map", func() {
			root := makeNode("real-root", nil, base, "tapes", "claude", "m", "p")
			putWith(ctx, driver, root)

			chains, err := driver.AncestryChains(ctx, []string{root.Hash, "does-not-exist"})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(chains).To(gomega.HaveLen(1))
			gomega.Expect(chains).To(gomega.HaveKey(root.Hash))
			gomega.Expect(chains).NotTo(gomega.HaveKey("does-not-exist"))
		})
	})
}

func putWith(ctx context.Context, d storage.Driver, n *merkle.Node) {
	_, err := d.Put(ctx, n)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func hashesOf(nodes []*merkle.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Hash
	}
	return out
}
