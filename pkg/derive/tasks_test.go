package derive_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
)

func toolSpan(seq int64, at time.Time, in, out llm.ContentBlock) *derive.Span {
	// A tool span's SpanID IS the tool_use id (see EmitSpans); the fold
	// keys results by span id and reads them by tool_use id, so they must
	// match.
	return &derive.Span{
		SpanID:    in.ToolUseID,
		Kind:      derive.SpanKindTool,
		StartedAt: at,
		Seq:       seq,
		Input:     []llm.ContentBlock{in},
		Output:    []llm.ContentBlock{out},
	}
}

func taskCreate(id, subject string) (llm.ContentBlock, llm.ContentBlock) {
	use := llm.ContentBlock{
		Type: "tool_use", ToolUseID: "u" + id, ToolName: "TaskCreate",
		ToolInput: map[string]any{"subject": subject, "description": "d" + id},
	}
	res := llm.ContentBlock{Type: "tool_result", ToolOutput: "Task #" + id + " created successfully: " + subject}
	return use, res
}

func taskUpdate(id, status string) (llm.ContentBlock, llm.ContentBlock) {
	use := llm.ContentBlock{
		Type: "tool_use", ToolUseID: "up" + id, ToolName: "TaskUpdate",
		ToolInput: map[string]any{"taskId": id, "status": status},
	}
	return use, llm.ContentBlock{Type: "tool_result", ToolOutput: "ok"}
}

var _ = Describe("FoldSessionTasks", func() {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	It("folds create then update in event order, tracking status and update count", func() {
		c1u, c1r := taskCreate("1", "first")
		u1u, u1r := taskUpdate("1", "in_progress")
		spans := []*derive.Span{
			toolSpan(0, base, c1u, c1r),
			toolSpan(1, base.Add(time.Second), u1u, u1r),
		}
		tasks := derive.FoldSessionTasks(spans)
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].ID).To(Equal("1"))
		Expect(tasks[0].Subject).To(Equal("first"))
		Expect(tasks[0].Status).To(Equal("in_progress"))
		Expect(tasks[0].Updates).To(Equal(1))
	})

	It("drops deleted tasks and ignores non-tool spans", func() {
		c1u, c1r := taskCreate("1", "keep")
		c2u, c2r := taskCreate("2", "gone")
		d2u, d2r := taskUpdate("2", "deleted")
		llmNoise := &derive.Span{SpanID: "llm_x", Kind: derive.SpanKindLLM, StartedAt: base}
		spans := []*derive.Span{
			toolSpan(0, base, c1u, c1r),
			llmNoise,
			toolSpan(1, base.Add(time.Second), c2u, c2r),
			toolSpan(2, base.Add(2*time.Second), d2u, d2r),
		}
		tasks := derive.FoldSessionTasks(spans)
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].ID).To(Equal("1"))
	})

	It("sorts by StartedAt then Seq before replaying, regardless of input order", func() {
		c1u, c1r := taskCreate("1", "first")
		u1u, u1r := taskUpdate("1", "completed")
		// Update handed in before the create; the fold must reorder by time.
		spans := []*derive.Span{
			toolSpan(1, base.Add(time.Second), u1u, u1r),
			toolSpan(0, base, c1u, c1r),
		}
		tasks := derive.FoldSessionTasks(spans)
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].Status).To(Equal("completed"))
	})
})
