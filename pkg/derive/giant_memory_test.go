package derive_test

// Regression coverage for the derive-worker memory bound (PCC-767).
//
// A real session re-sends its whole prior conversation on every wire turn,
// so the deriver re-parses an O(N) request per turn and the per-turn
// garbage piles up faster than the GC reclaims it. Under the default
// GOGC=100 the heap is allowed to roughly double its live size before a
// collection, so a large session's TRANSIENT peak runs ~2x its live set —
// enough to cross the container limit in a brief spike and get the worker
// OOM-killed. The durable fix is a soft memory limit
// (worker.ApplySoftMemoryLimit) that GC-paces the peak back toward the
// live set.
//
// These specs (a) pin the synthetic fixture that reproduces the growth and
// (b) prove a derive of a large session completes correctly under a tight
// soft memory limit. The sampled-peak demonstration of the spike itself is
// gated behind TAPES_MEMPROBE: a wall-clock heap sample is too timing-
// sensitive to gate CI on, but it documents and locally guards the win.

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// syntheticSession builds anthropic wire rows for ONE session whose every
// turn re-sends the full prior history (the real wire shape). Turn i sends
// messages [u1,a1,…,u_i] and responds a_i; the re-sent prefix is byte-
// identical across turns, so it dedups exactly like live capture. Each
// turn contributes 2 new unique nodes (fresh user + assistant), so a
// K-turn session derives to ~2K nodes — and turn i re-parses i messages,
// the O(N^2) churn that drives the transient peak. stream+tools make every
// call classify as the conversation spine (KindMain), exercising the real
// derive path.
func syntheticSession(turns, msgBytes int) []storage.RawTurnRecord {
	const sess = "synthetic-giant-0000-0000-0000-000000000000"
	system := strings.Repeat("S", 7000) // ~7KB, like the real Claude Code system prompt
	// A tool set re-sent verbatim every turn — a big share of each
	// request's bytes and parse cost in real traffic. Not retained on
	// nodes (only ToolCount is), so it stresses the transient term only.
	tools := make([]map[string]any, 0, 96)
	for i := range 96 {
		tools = append(tools, map[string]any{
			"name":         fmt.Sprintf("tool_%02d", i),
			"description":  strings.Repeat("d", 400),
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "string", "description": strings.Repeat("p", 200)}}},
		})
	}
	streamTrue := true
	type amsg struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	}
	var history []amsg
	rows := make([]storage.RawTurnRecord, 0, turns)
	for i := 1; i <= turns; i++ {
		history = append(history, amsg{Role: "user", Content: fmt.Sprintf("U%06d:", i) + strings.Repeat("u", msgBytes)})
		rr, err := json.Marshal(map[string]any{
			"model": "claude-opus-4-8", "system": system, "max_tokens": 4096,
			"stream": streamTrue, "tools": tools, "messages": history,
		})
		if err != nil {
			panic(err)
		}
		asstText := fmt.Sprintf("A%06d:", i) + strings.Repeat("a", msgBytes)
		rj, err := json.Marshal(map[string]any{
			"done": true, "model": "claude-opus-4-8", "stop_reason": "end_turn",
			"usage":   map[string]any{"prompt_tokens": 1000, "completion_tokens": 100, "total_tokens": 1100},
			"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": asstText}}},
		})
		if err != nil {
			panic(err)
		}
		rows = append(rows, storage.RawTurnRecord{
			ID: int64(i), Provider: "anthropic", HarnessID: "claude", HarnessSessionID: sess,
			RequestID: fmt.Sprintf("req-%06d", i), RawRequest: rr, Response: rj,
			ReceivedAt: time.Unix(int64(1700000000+i), 0),
		})
		history = append(history, amsg{Role: "assistant", Content: []map[string]any{{"type": "text", "text": asstText}}})
	}
	return rows
}

// openaiSession is syntheticSession's OpenAI/Codex twin: a growing
// chat-completions session (provider "openai") that re-sends its full
// history every turn. OpenAI carries no top-level system prompt, so its
// retained pin is dominated by the scalar request pointers
// (max_tokens/temperature/stream) the provider decoder allocates in the
// raw buffer's arena — a different aliasing shape than Anthropic's. Used to
// prove the (provider-agnostic) clone de-aliases this decoder too.
func openaiSession(turns, msgBytes int) []storage.RawTurnRecord {
	const sess = "openai-giant-0000-0000-0000-000000000000"
	tools := make([]map[string]any, 0, 64)
	for i := range 64 {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": fmt.Sprintf("tool_%02d", i), "description": strings.Repeat("d", 400),
				"parameters": map[string]any{"type": "object"},
			},
		})
	}
	streamTrue := true
	type omsg struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	var history []omsg
	rows := make([]storage.RawTurnRecord, 0, turns)
	for i := 1; i <= turns; i++ {
		history = append(history, omsg{Role: "user", Content: fmt.Sprintf("U%06d:", i) + strings.Repeat("u", msgBytes)})
		rr, err := json.Marshal(map[string]any{
			"model": "gpt-5-codex", "max_tokens": 4096, "temperature": 0.2,
			"stream": streamTrue, "tools": tools, "messages": history,
		})
		if err != nil {
			panic(err)
		}
		asstText := fmt.Sprintf("A%06d:", i) + strings.Repeat("a", msgBytes)
		rj, err := json.Marshal(map[string]any{
			"model": "gpt-5-codex", "stop_reason": "stop",
			"usage":   map[string]any{"prompt_tokens": 1000, "completion_tokens": 100, "total_tokens": 1100},
			"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": asstText}}},
		})
		if err != nil {
			panic(err)
		}
		rows = append(rows, storage.RawTurnRecord{
			ID: int64(i), Provider: "openai", HarnessID: "codex", HarnessSessionID: sess,
			RequestID: fmt.Sprintf("req-%06d", i), RawRequest: rr, Response: rj,
			ReceivedAt: time.Unix(int64(1700000000+i), 0),
		})
		history = append(history, omsg{Role: "assistant", Content: asstText})
	}
	return rows
}

// streamDerive feeds rows one at a time and drops each immediately, like
// the production worker (which streams rows from the store, not all at
// once). Returns the derived set and span projection.
func streamDerive(rows []storage.RawTurnRecord) (*derive.DerivedSet, *derive.SpanSet) {
	dv, err := derive.NewDeriver("")
	Expect(err).NotTo(HaveOccurred())
	for i := range rows {
		dv.AddTurn(&rows[i])
		rows[i] = storage.RawTurnRecord{}
	}
	set := dv.Finish()
	return set, derive.EmitSpans(set)
}

var _ = Describe("synthetic giant session (PCC-767)", func() {
	It("dedups re-sent history to two new nodes per growing turn", func() {
		set, err := derive.BuildDerivedSet(syntheticSession(20, 64), "")
		Expect(err).NotTo(HaveOccurred())
		// 20 growing turns -> 40 unique nodes (one user + one assistant
		// each), every call the conversation spine.
		Expect(set.Nodes).To(HaveLen(40))
		Expect(set.Report.CallKinds).To(Equal(map[string]int{derive.KindMain: 20}))
	})

	It("derives a large session correctly under a tight soft memory limit", func() {
		// Set a soft heap ceiling well under the unbounded transient peak,
		// the way the worker does in a memory-limited container. The derive
		// must still complete and project the full set — GC pacing changes
		// timing, never the pure projection.
		restore := debug.SetMemoryLimit(220 * 1024 * 1024)
		DeferCleanup(func() { debug.SetMemoryLimit(restore) })

		const turns = 200
		set, spans := streamDerive(syntheticSession(turns, 1024))
		Expect(set.Nodes).To(HaveLen(2 * turns))
		Expect(set.Report.CallKinds).To(Equal(map[string]int{derive.KindMain: turns}))
		// One trace per genuine prompt; one spine llm span per turn.
		Expect(spans.Turns).To(HaveLen(turns))
	})

	// The durable complement to the soft limit: clone-on-retain. A retained
	// node keeps only the unique content first seen on its turn, but that
	// content is parsed zero-copy, so each retained string aliases — and thus
	// pins — its turn's whole multi-MB re-sent-history request buffer. Cloning
	// the retained strings frees every raw buffer after its turn, so the LIVE
	// floor settles at ~unique content (O(turns)) instead of ~the re-sent
	// history (O(turns^2) on the wire).
	//
	// assertBoundedFloor is a deterministic post-GC live measurement (not a
	// sampled peak): after streaming, the only roots are the derived set, so
	// HeapAlloc is the floor. Without the clone, the retained nodes pin every
	// request buffer and the floor tracks the re-sent wire; the generous
	// ceilings fail loudly if that regresses. The clone operates on the
	// provider-agnostic node/bucket/params types, so the bound must hold for
	// every provider decoder — hence the matrix below.
	// gen is a lazy row generator: DescribeTable evaluates Entry arguments
	// eagerly and retains them for the whole table, so passing the giant row
	// slices directly would keep one provider's ~180 MB of buffers alive
	// while the other's floor is measured. Building the rows inside the spec
	// keeps each measurement isolated.
	DescribeTable("bounds the live floor to unique content, not the re-sent history",
		func(gen func() []storage.RawTurnRecord, label string) {
			rows := gen()
			var resentBytes uint64
			for i := range rows {
				resentBytes += uint64(len(rows[i].RawRequest))
			}
			set, _ := streamDerive(rows)
			Expect(set.Nodes).To(HaveLen(2 * 400))
			floor := liveHeap()
			runtime.KeepAlive(set)

			GinkgoWriter.Printf("%s derive floor: live=%s re-sent wire=%s (%.1fx)\n",
				label, mb(floor), mb(resentBytes), float64(resentBytes)/float64(floor))

			// The floor is a small fraction of the re-sent history — proof the
			// raw buffers were freed rather than pinned by retained nodes.
			Expect(floor).To(BeNumerically("<", resentBytes/10))
			// Absolute regression ceiling: unique content here is a few MB;
			// pinning the ~O(turns^2) buffers would blow far past this.
			Expect(floor).To(BeNumerically("<", uint64(96*1024*1024)))
		},
		// Anthropic: pins via content strings AND the system prompt.
		Entry("anthropic", func() []storage.RawTurnRecord { return syntheticSession(400, 1024) }, "anthropic"),
		// OpenAI/Codex: a different request decoder — no top-level system, so
		// the pin is dominated by the scalar pointers (max_tokens/temperature/
		// stream) the decoder allocates in the buffer's arena. Confirms the
		// clone de-aliases across providers, not just Anthropic.
		Entry("openai/codex", func() []storage.RawTurnRecord { return openaiSession(400, 1024) }, "openai/codex"),
	)

	It("keeps the transient peak under a soft limit [gated: TAPES_MEMPROBE]", func() {
		if os.Getenv("TAPES_MEMPROBE") == "" {
			Skip("set TAPES_MEMPROBE=1 to run the sampled-peak demonstration")
		}
		const turns, msgBytes = 400, 2048

		// Unbounded: default GOGC lets the transient churn overshoot the
		// live set ~2x.
		debug.SetMemoryLimit(math.MaxInt64)
		runtime.GC()
		stop := peakSampler()
		set, _ := streamDerive(syntheticSession(turns, msgBytes))
		unbounded := stop()
		live := liveHeap()
		runtime.KeepAlive(set)

		// Bounded: a soft limit just above the live set collapses the peak.
		soft := uint64(float64(live) * 1.10)
		debug.SetMemoryLimit(int64(soft))
		DeferCleanup(func() { debug.SetMemoryLimit(math.MaxInt64) })
		runtime.GC()
		stop = peakSampler()
		set2, _ := streamDerive(syntheticSession(turns, msgBytes))
		bounded := stop()
		runtime.KeepAlive(set2)

		GinkgoWriter.Printf("giant derive: live=%s unbounded peak=%s bounded peak=%s (soft=%s)\n",
			mb(live), mb(unbounded), mb(bounded), mb(soft))
		// The soft limit must meaningfully cut the transient peak.
		Expect(bounded).To(BeNumerically("<", unbounded*8/10))
	})
})

func mb(b uint64) string { return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024)) }

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// peakSampler polls HeapAlloc and returns the max observed until stopped.
// maxHeap is written only by the sampler goroutine and read only after
// stop() observes its exit via <-finished, so the channel close
// establishes the happens-before edge — no atomics needed.
func peakSampler() (stop func() uint64) {
	var maxHeap uint64
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var m runtime.MemStats
		tk := time.NewTicker(500 * time.Microsecond)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > maxHeap {
					maxHeap = m.HeapAlloc
				}
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-finished
		return maxHeap
	}
}
