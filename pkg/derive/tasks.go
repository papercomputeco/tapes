package derive

import (
	"regexp"
	"sort"
	"strconv"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Task is one harness todo item, folded from TaskCreate/TaskUpdate tool
// calls at derive time. It is a session-scoped rollup fact persisted on
// sessions.tasks; the API serves it verbatim.
type Task struct {
	ID          string `json:"id"`
	Subject     string `json:"subject"`
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
	Updates     int    `json:"updates"`
}

// taskCreatedPattern extracts the task id the harness reports back in the
// TaskCreate tool_result ("Task #3 created successfully: …").
var taskCreatedPattern = regexp.MustCompile(`#(\d+)`)

// FoldSessionTasks replays a session's TaskCreate/TaskUpdate tool spans in
// event order into the current task list. The replay must run in event
// order, so tool spans are sorted by StartedAt then Seq (storage hands
// spans back sorted by trace_id, which is lexicographic, not chronological
// — the same reason the old read-time fold sorted).
func FoldSessionTasks(spans []*Span) []Task {
	tools := make([]*Span, 0, len(spans))
	for _, sp := range spans {
		if sp.Kind == SpanKindTool {
			tools = append(tools, sp)
		}
	}
	sort.SliceStable(tools, func(i, j int) bool {
		if !tools[i].StartedAt.Equal(tools[j].StartedAt) {
			return tools[i].StartedAt.Before(tools[j].StartedAt)
		}
		return tools[i].Seq < tools[j].Seq
	})

	resultText := map[string]string{}
	var uses []llm.ContentBlock
	for _, sp := range tools {
		if len(sp.Input) > 0 {
			uses = append(uses, sp.Input[0])
		}
		if len(sp.Output) > 0 {
			if _, ok := resultText[sp.SpanID]; !ok {
				resultText[sp.SpanID] = sp.Output[0].ToolOutput
			}
		}
	}
	return foldTaskBlocks(uses, resultText)
}

// foldTaskBlocks replays TaskCreate/TaskUpdate tool_use blocks (in capture
// order) against their results. The fold is a function of the calls, not
// of the storage model.
func foldTaskBlocks(uses []llm.ContentBlock, resultText map[string]string) []Task {
	byID := map[string]*Task{}
	var order []*Task
	for _, b := range uses {
		switch b.ToolName {
		case "TaskCreate":
			subject, _ := b.ToolInput["subject"].(string)
			description, _ := b.ToolInput["description"].(string)
			id := ""
			if m := taskCreatedPattern.FindStringSubmatch(resultText[b.ToolUseID]); m != nil {
				id = m[1]
			}
			task := &Task{ID: id, Subject: subject, Description: description, Status: "pending"}
			if id != "" {
				if _, dup := byID[id]; dup {
					continue
				}
				byID[id] = task
			}
			order = append(order, task)
		case "TaskUpdate":
			id, _ := b.ToolInput["taskId"].(string)
			if id == "" {
				if f, ok := b.ToolInput["taskId"].(float64); ok {
					id = strconv.Itoa(int(f))
				}
			}
			task, ok := byID[id]
			if !ok {
				continue
			}
			task.Updates++
			if status, ok := b.ToolInput["status"].(string); ok && status != "" {
				task.Status = status
			}
			if subject, ok := b.ToolInput["subject"].(string); ok && subject != "" {
				task.Subject = subject
			}
		}
	}
	out := make([]Task, 0, len(order))
	for _, t := range order {
		if t.Status == "deleted" {
			continue
		}
		out = append(out, *t)
	}
	return out
}
