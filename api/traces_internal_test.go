package api

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("session task folds", func() {
	It("folds Codex update_plan snapshots alongside Claude task events", func() {
		uses := []llm.ContentBlock{
			{
				Type:     "tool_use",
				ToolName: "TaskPlan",
				ToolInput: map[string]any{"plan": []any{
					map[string]any{"step": "Inspect traces", "status": "in_progress"},
					map[string]any{"step": "Verify UI", "status": "pending"},
				}},
			},
			{
				Type:     "tool_use",
				ToolName: "Parallel",
				ToolInput: map[string]any{"plan": []any{
					map[string]any{"step": "Inspect traces", "status": "completed"},
					map[string]any{"step": "Verify UI", "status": "in_progress"},
				}},
			},
		}

		tasks := foldTaskBlocks(uses, nil)
		Expect(tasks).To(Equal([]TreeTask{
			{ID: "codex-plan-1", Subject: "Inspect traces", Status: "completed", Updates: 1},
			{ID: "codex-plan-2", Subject: "Verify UI", Status: "in_progress", Updates: 1},
		}))
	})
})
