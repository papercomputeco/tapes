package backfill_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// fakeBackfillDriver is a storage.Driver that also implements
// storage.SessionBackfiller. It serves a fixed node list (the backfill
// candidate set), answers AncestryChain with single-node chains, and
// records every BackfillSession call so specs can assert call counts and
// args without a database.
type fakeBackfillDriver struct {
	mu sync.Mutex

	nodes []*merkle.Node

	backfillCalls []storage.SessionBackfillRequest
	// linkedPerCall is the NodesLinked value returned for each successive
	// BackfillSession call. When shorter than the number of calls, the
	// default is len(req.NodeHashes).
	linkedPerCall []int
}

func (f *fakeBackfillDriver) List(_ context.Context) ([]*merkle.Node, error) {
	return f.nodes, nil
}

func (f *fakeBackfillDriver) AncestryChain(_ context.Context, hash string) (*storage.Chain, error) {
	for _, n := range f.nodes {
		if n.Hash == hash {
			return &storage.Chain{Nodes: []*merkle.Node{n}}, nil
		}
	}
	return &storage.Chain{Nodes: []*merkle.Node{{Hash: hash}}}, nil
}

func (f *fakeBackfillDriver) BackfillSession(_ context.Context, req storage.SessionBackfillRequest) (storage.SessionBackfillResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := len(f.backfillCalls)
	f.backfillCalls = append(f.backfillCalls, req)
	linked := len(req.NodeHashes)
	if idx < len(f.linkedPerCall) {
		linked = f.linkedPerCall[idx]
	}
	return storage.SessionBackfillResult{
		SessionID:   "session-" + req.Session.HarnessSessionID,
		NodesLinked: linked,
	}, nil
}

func (f *fakeBackfillDriver) calls() []storage.SessionBackfillRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.SessionBackfillRequest, len(f.backfillCalls))
	copy(out, f.backfillCalls)
	return out
}

// Remaining storage.Driver methods are unused by the backfill flow under
// test; stub them to satisfy the interface.
func (f *fakeBackfillDriver) Get(context.Context, string) (*merkle.Node, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeBackfillDriver) GetByParent(context.Context, *string) ([]*merkle.Node, error) {
	return nil, nil
}
func (f *fakeBackfillDriver) Put(context.Context, *merkle.Node) (bool, error) { return false, nil }
func (f *fakeBackfillDriver) Has(context.Context, string) (bool, error)       { return false, nil }
func (f *fakeBackfillDriver) Roots(context.Context) ([]*merkle.Node, error)   { return nil, nil }
func (f *fakeBackfillDriver) Leaves(context.Context) ([]*merkle.Node, error)  { return nil, nil }
func (f *fakeBackfillDriver) ListSessions(context.Context, storage.ListOpts) (*storage.Page[*merkle.Node], error) {
	return &storage.Page[*merkle.Node]{}, nil
}

func (f *fakeBackfillDriver) CountSessions(context.Context, storage.ListOpts) (storage.SessionStats, error) {
	return storage.SessionStats{}, nil
}

func (f *fakeBackfillDriver) Ancestry(context.Context, string) ([]*merkle.Node, error) {
	return nil, nil
}

func (f *fakeBackfillDriver) AncestryChains(context.Context, []string) (map[string]*storage.Chain, error) {
	return nil, nil
}
func (f *fakeBackfillDriver) LoadDag(context.Context, string) (*merkle.Dag, error) { return nil, nil }
func (f *fakeBackfillDriver) Open(context.Context) error                           { return nil }
func (f *fakeBackfillDriver) UpdateUsage(context.Context, string, *llm.Usage) error {
	return nil
}
func (f *fakeBackfillDriver) Depth(context.Context, string) (int, error) { return 0, nil }
func (f *fakeBackfillDriver) Close() error                               { return nil }

// assistantNodeAt builds an assistant node whose CreatedAt is set so the
// time-window match in matchAndUpdate (±5s) can pair it with a transcript
// entry at the same instant.
func assistantNodeAt(text, model string, at time.Time) *merkle.Node {
	n := merkle.NewNode(makeAssistantBucket(text, model), nil)
	n.CreatedAt = at
	return n
}

var _ = Describe("Backfiller session backfill (recordSessionMatch / backfillSessions)", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("derives the session id from the transcript SourcePath basename when the entry SessionID is empty", func() {
		now := time.Now().UTC()
		ts := now.Format("2006-01-02T15:04:05.000Z")
		model := "claude-sonnet-4-5-20250929"

		node := assistantNodeAt("Hello from Claude!", model, now)
		fake := &fakeBackfillDriver{nodes: []*merkle.Node{node}}

		tmpDir := GinkgoT().TempDir()
		// The JSONL entry deliberately omits "sessionId", so the session id
		// must fall back to the file basename sans extension: "abc123".
		jsonl := fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"%s","message":{"id":"msg_001","role":"assistant","model":"%s","content":[{"type":"text","text":"Hello from Claude!"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, ts, model)
		writeJSONL(tmpDir, "abc123.jsonl", jsonl)

		b := backfill.NewBackfillerWithDriver(fake, backfill.Options{Sessions: true})
		result, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Matched).To(Equal(1))

		calls := fake.calls()
		Expect(calls).To(HaveLen(1))
		Expect(calls[0].Session).NotTo(BeNil())
		Expect(calls[0].Session.HarnessSessionID).To(Equal("abc123"),
			"empty entry SessionID must derive id from SourcePath basename")
	})

	It("prefers the entry's explicit SessionID over the file basename", func() {
		now := time.Now().UTC()
		ts := now.Format("2006-01-02T15:04:05.000Z")
		model := "claude-sonnet-4-5-20250929"

		node := assistantNodeAt("Explicit session", model, now)
		fake := &fakeBackfillDriver{nodes: []*merkle.Node{node}}

		tmpDir := GinkgoT().TempDir()
		jsonl := fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"%s","sessionId":"explicit-sid","message":{"id":"msg_001","role":"assistant","model":"%s","content":[{"type":"text","text":"Explicit session"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, ts, model)
		writeJSONL(tmpDir, "ignored-filename.jsonl", jsonl)

		b := backfill.NewBackfillerWithDriver(fake, backfill.Options{Sessions: true})
		_, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		calls := fake.calls()
		Expect(calls).To(HaveLen(1))
		Expect(calls[0].Session.HarnessSessionID).To(Equal("explicit-sid"))
	})

	It("aggregates multiple matched entries that share a session id into a single BackfillSession call", func() {
		now := time.Now().UTC()
		model := "claude-sonnet-4-5-20250929"

		// Two assistant nodes at distinct (far-apart) times so each pairs
		// with exactly one transcript entry.
		node1 := assistantNodeAt("first answer", model, now)
		node2 := assistantNodeAt("second answer", model, now.Add(1*time.Hour))
		fake := &fakeBackfillDriver{nodes: []*merkle.Node{node1, node2}}

		ts1 := now.Format("2006-01-02T15:04:05.000Z")
		ts2 := now.Add(1 * time.Hour).Format("2006-01-02T15:04:05.000Z")

		tmpDir := GinkgoT().TempDir()
		jsonl := fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"%s","sessionId":"shared","message":{"id":"m1","role":"assistant","model":"%s","content":[{"type":"text","text":"first answer"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}
{"type":"assistant","uuid":"a2","timestamp":"%s","sessionId":"shared","message":{"id":"m2","role":"assistant","model":"%s","content":[{"type":"text","text":"second answer"}],"stop_reason":"end_turn","usage":{"input_tokens":200,"output_tokens":75,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, ts1, model, ts2, model)
		writeJSONL(tmpDir, "shared.jsonl", jsonl)

		b := backfill.NewBackfillerWithDriver(fake, backfill.Options{Sessions: true})
		result, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Matched).To(Equal(2))

		calls := fake.calls()
		Expect(calls).To(HaveLen(1), "two entries with the same session id must aggregate to one call")
		Expect(calls[0].Session.HarnessSessionID).To(Equal("shared"))
		// Per-session counters are folded across both matched entries.
		Expect(calls[0].InputTokens).To(Equal(int64(300)))
		Expect(calls[0].OutputTokens).To(Equal(int64(125)))
		Expect(calls[0].TurnCount).To(Equal(int64(2)))
	})

	It("skips an entry whose Timestamp does not parse without aborting the backfill", func() {
		now := time.Now().UTC()
		goodTs := now.Format("2006-01-02T15:04:05.000Z")
		model := "claude-sonnet-4-5-20250929"

		// Two nodes; one will pair with the good-timestamp entry, the other
		// would pair with the bad-timestamp entry if it parsed.
		good := assistantNodeAt("good entry", model, now)
		bad := assistantNodeAt("bad entry", model, now.Add(1*time.Hour))
		fake := &fakeBackfillDriver{nodes: []*merkle.Node{good, bad}}

		tmpDir := GinkgoT().TempDir()
		// First entry has a malformed timestamp; second is well-formed.
		jsonl := fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"not-a-timestamp","sessionId":"sess","message":{"id":"m1","role":"assistant","model":"%s","content":[{"type":"text","text":"bad entry"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}
{"type":"assistant","uuid":"a2","timestamp":"%s","sessionId":"sess","message":{"id":"m2","role":"assistant","model":"%s","content":[{"type":"text","text":"good entry"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, model, goodTs, model)
		writeJSONL(tmpDir, "sess.jsonl", jsonl)

		b := backfill.NewBackfillerWithDriver(fake, backfill.Options{Sessions: true})
		result, err := b.Run(ctx, tmpDir)
		// The malformed-timestamp entry must be a per-entry skip, never a
		// hard failure of the whole backfill.
		Expect(err).NotTo(HaveOccurred())

		// The good entry still matched and produced exactly one session call,
		// folding only the good entry's tokens.
		Expect(result.Matched).To(Equal(1))
		calls := fake.calls()
		Expect(calls).To(HaveLen(1))
		Expect(calls[0].Session.HarnessSessionID).To(Equal("sess"))
		Expect(calls[0].InputTokens).To(Equal(int64(10)))
		Expect(calls[0].OutputTokens).To(Equal(int64(5)))
		Expect(calls[0].TurnCount).To(Equal(int64(1)))
	})

	It("invokes BackfillSession once per aggregated session and folds NodesLinked into the Result", func() {
		now := time.Now().UTC()
		model := "claude-sonnet-4-5-20250929"

		// Two sessions, one matched node each.
		n1 := assistantNodeAt("alpha", model, now)
		n2 := assistantNodeAt("beta", model, now.Add(2*time.Hour))
		fake := &fakeBackfillDriver{
			nodes: []*merkle.Node{n1, n2},
			// First session links 3 nodes, second links 5 — values distinct
			// from len(NodeHashes) so we know the driver's return is folded.
			linkedPerCall: []int{3, 5},
		}

		ts1 := now.Format("2006-01-02T15:04:05.000Z")
		ts2 := now.Add(2 * time.Hour).Format("2006-01-02T15:04:05.000Z")

		tmpDir := GinkgoT().TempDir()
		writeJSONL(tmpDir, "s1.jsonl", fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"%s","sessionId":"s1","message":{"id":"m1","role":"assistant","model":"%s","content":[{"type":"text","text":"alpha"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, ts1, model))
		writeJSONL(tmpDir, "s2.jsonl", fmt.Sprintf(`{"type":"assistant","uuid":"a2","timestamp":"%s","sessionId":"s2","message":{"id":"m2","role":"assistant","model":"%s","content":[{"type":"text","text":"beta"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`, ts2, model))

		b := backfill.NewBackfillerWithDriver(fake, backfill.Options{Sessions: true})
		result, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		calls := fake.calls()
		Expect(calls).To(HaveLen(2), "one BackfillSession call per aggregated session")
		Expect(result.SessionsBackfilled).To(Equal(2))
		Expect(result.NodesLinked).To(Equal(8), "Result.NodesLinked must fold each call's NodesLinked (3+5)")
	})
})
