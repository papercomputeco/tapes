package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/transcript"
)

// transcriptSpan builds one stored span with content-block payloads.
func transcriptSpan(traceID string, seq int, kind, callKind, thread, spanID string, at time.Time, input, output []llm.ContentBlock) storage.SpanRecord {
	return storage.SpanRecord{
		TraceID:   traceID,
		SpanID:    spanID,
		Kind:      kind,
		Name:      kind,
		Status:    "ok",
		CallKind:  callKind,
		ThreadID:  thread,
		StartedAt: at.Add(time.Duration(seq) * time.Second),
		Seq:       int64(seq),
		Input:     mustJSON(input),
		Output:    mustJSON(output),
		Usage:     json.RawMessage(`{}`),
	}
}

func text(s string) llm.ContentBlock { return llm.ContentBlock{Type: "text", Text: s} }

func toolUse(id, name string) llm.ContentBlock {
	return llm.ContentBlock{Type: "tool_use", ToolUseID: id, ToolName: name, ToolInput: map[string]any{"arg": id}}
}

func toolResult(id, out string, isErr bool) llm.ContentBlock {
	return llm.ContentBlock{Type: "tool_result", ToolResultID: id, ToolOutput: out, IsError: isErr}
}

// newTranscriptModel is a two-turn session shaped like a Claude Code
// capture: injected context, a title-gen offshoot, a tool loop whose
// results ride on tool spans, a subagent thread, and harness tags in the
// user's text. Trace ids run against started_at, so a walk that ordered
// traces by id would show up as a reordered transcript.
func newTranscriptModel() *pagedSpanModel {
	d := newPagedSpanModel(0, 0)
	d.Driver = inmemory.NewDriver()
	first, second := "trc-b", "trc-a"
	t0 := fixtureEpoch
	t1 := fixtureEpoch.Add(time.Hour)
	d.turns = []storage.TraceSummaryRecord{
		{TraceID: first, SessionID: pagedSessionID, StartedAt: t0, SpanCount: 10},
		{TraceID: second, SessionID: pagedSessionID, StartedAt: t1, SpanCount: 2},
	}
	d.spans = map[string][]storage.SpanRecord{
		first: {
			transcriptSpan(first, 0, "agent", "", "", "agent_main", t0, nil, nil),
			transcriptSpan(first, 1, "event", "injected:mcp-instructions", "", "evt_1", t0,
				[]llm.ContentBlock{text("# MCP Server Instructions")}, nil),
			transcriptSpan(first, 2, "llm", "main", "", "llm_1", t0,
				[]llm.ContentBlock{text("<system-reminder>today is a day</system-reminder>"), text("Hello world")},
				[]llm.ContentBlock{{Type: "thinking", Thinking: "hmm"}, text("Hi there"), toolUse("toolu_1", "Read")}),
			transcriptSpan(first, 3, "tool", "", "", "toolu_1", t0,
				[]llm.ContentBlock{toolUse("toolu_1", "Read")}, []llm.ContentBlock{toolResult("toolu_1", "file contents", false)}),
			transcriptSpan(first, 4, "llm", "offshoot:title-gen", "", "llm_title", t0,
				[]llm.ContentBlock{text("<session>Hello world</session>")}, []llm.ContentBlock{text(`{"title":"Hello"}`)}),
			transcriptSpan(first, 5, "llm", "main", "", "llm_2", t0,
				nil, []llm.ContentBlock{text("Done reading"), toolUse("toolu_2", "Bash"), toolUse("toolu_3", "Bash")}),
			transcriptSpan(first, 6, "tool", "", "", "toolu_2", t0,
				[]llm.ContentBlock{toolUse("toolu_2", "Bash")}, []llm.ContentBlock{toolResult("toolu_2", "boom", true)}),
			transcriptSpan(first, 7, "tool", "", "", "toolu_3", t0,
				[]llm.ContentBlock{toolUse("toolu_3", "Bash")}, []llm.ContentBlock{toolResult("toolu_3", "ok", false)}),
			transcriptSpan(first, 8, "agent", "", "sub1", "agent_sub1", t0, nil, nil),
			transcriptSpan(first, 9, "llm", "main", "sub1", "llm_sub", t0,
				[]llm.ContentBlock{text("Subagent task")}, []llm.ContentBlock{text("sub reply")}),
			transcriptSpan(first, 10, "llm", "", "", "llm_3", t0,
				nil, []llm.ContentBlock{text("Final answer")}),
		},
		second: {
			transcriptSpan(second, 0, "agent", "", "", "agent_main_2", t1, nil, nil),
			transcriptSpan(second, 1, "llm", "main", "", "llm_4", t1,
				[]llm.ContentBlock{text("<command-name>/model</command-name>\nSecond prompt")},
				[]llm.ContentBlock{text("Second ANSWER")}),
		},
	}
	return d
}

// rowSummary is the readable identity of a row for assertions.
func rowSummary(r transcript.Row) string {
	s := fmt.Sprintf("%s@%s:%s", r.Role, r.SpanID, r.Text)
	var sSb103 strings.Builder
	for _, c := range r.ToolCalls {
		sSb103.WriteString(" call(" + c.ID + ")")
	}
	s += sSb103.String()
	var sSb106 strings.Builder
	for _, res := range r.ToolResults {
		fmt.Fprintf(&sSb106, " result(%s,%t)", res.ToolUseID, res.IsError)
	}
	s += sSb106.String()
	return s
}

func summaries(rows []transcript.Row) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, rowSummary(r))
	}
	return out
}

func getTranscript(server *Server, sessionID, query string) (int, []byte) {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet,
		"/v1/sessions/"+sessionID+"/transcript"+query, nil)
	response, err := server.app.Test(request, fiber.TestConfig{Timeout: 30 * time.Second})
	Expect(err).NotTo(HaveOccurred())
	body, err := io.ReadAll(response.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.Body.Close()).To(Succeed())
	return response.StatusCode, body
}

func decodeTranscript(body []byte) TranscriptResponse {
	var resp TranscriptResponse
	ExpectWithOffset(1, json.Unmarshal(body, &resp)).To(Succeed(), string(body))
	return resp
}

// walkTranscript follows next_cursor to the end, returning every row.
func walkTranscript(server *Server, query string) ([]transcript.Row, int) {
	var rows []transcript.Row
	pages := 0
	cursor := ""
	for {
		q := query
		if cursor != "" {
			q += "&cursor=" + cursor
		}
		status, body := getTranscript(server, pagedSessionID, q)
		ExpectWithOffset(1, status).To(Equal(http.StatusOK), string(body))
		page := decodeTranscript(body)
		rows = append(rows, page.Items...)
		pages++
		if page.NextCursor == "" {
			return rows, pages
		}
		ExpectWithOffset(1, pages).To(BeNumerically("<", 100), "the walk must terminate")
		cursor = page.NextCursor
	}
}

var rootTranscript = []string{
	"user@llm_1:Hello world",
	"assistant@llm_1:Hi there call(toolu_1)",
	"user@toolu_1: result(toolu_1,false)",
	"assistant@llm_2:Done reading call(toolu_2) call(toolu_3)",
	"user@toolu_2: result(toolu_2,true) result(toolu_3,false)",
	"assistant@llm_3:Final answer",
	"user@llm_4:Second prompt",
	"assistant@llm_4:Second ANSWER",
}

var _ = Describe("GET /v1/sessions/:id/transcript", func() {
	var server *Server

	BeforeEach(func() {
		server = newPagedServer(newTranscriptModel())
	})

	It("renders the root conversation in turn order, then span order", func() {
		status, body := getTranscript(server, pagedSessionID, "")
		Expect(status).To(Equal(http.StatusOK), string(body))
		resp := decodeTranscript(body)
		Expect(resp.SessionID).To(Equal(pagedSessionID))
		Expect(resp.NextCursor).To(BeEmpty())
		Expect(summaries(resp.Items)).To(Equal(rootTranscript))
	})

	It("carries the row identity, tool call inputs, and tool result text", func() {
		_, body := getTranscript(server, pagedSessionID, "")
		resp := decodeTranscript(body)
		Expect(resp.Items[0].TraceID).To(Equal("trc-b"))
		Expect(resp.Items[0].Seq).To(Equal(int64(2)))
		Expect(resp.Items[0].StartedAt).To(BeTemporally("==", fixtureEpoch.Add(2*time.Second)))
		Expect(resp.Items[0].ThreadID).To(BeEmpty())
		Expect(resp.Items[1].ToolCalls).To(Equal([]transcript.ToolCall{
			{ID: "toolu_1", Name: "Read", Input: map[string]any{"arg": "toolu_1"}},
		}))
		Expect(resp.Items[2].Text).To(BeEmpty(), "tool results never land in text")
		Expect(resp.Items[2].ToolResults).To(Equal([]transcript.ToolResult{
			{ToolUseID: "toolu_1", Text: "file contents"},
		}))

		var raw struct {
			Items []map[string]any `json:"items"`
		}
		Expect(json.Unmarshal(body, &raw)).To(Succeed())
		Expect(raw.Items[0]).NotTo(HaveKey("thread_id"), "thread_id is omitted on the root thread")
		Expect(raw.Items[0]).NotTo(HaveKey("tool_calls"))
		Expect(raw.Items[0]).To(HaveKey("text"))
	})

	It("strips harness tags from user text and leaves offshoots and injected context out", func() {
		_, body := getTranscript(server, pagedSessionID, "")
		Expect(string(body)).NotTo(ContainSubstring("system-reminder"))
		Expect(string(body)).NotTo(ContainSubstring("today is a day"))
		Expect(string(body)).NotTo(ContainSubstring("command-name"))
		Expect(string(body)).NotTo(ContainSubstring("MCP Server Instructions"))
		Expect(string(body)).NotTo(ContainSubstring("title"))
		Expect(string(body)).NotTo(ContainSubstring("hmm"), "thinking is not transcript text")
	})

	It("includes subagent threads on request", func() {
		_, body := getTranscript(server, pagedSessionID, "?subagents=true")
		resp := decodeTranscript(body)
		Expect(summaries(resp.Items)).To(Equal([]string{
			"user@llm_1:Hello world",
			"assistant@llm_1:Hi there call(toolu_1)",
			"user@toolu_1: result(toolu_1,false)",
			"assistant@llm_2:Done reading call(toolu_2) call(toolu_3)",
			"user@toolu_2: result(toolu_2,true) result(toolu_3,false)",
			"user@llm_sub:Subagent task",
			"assistant@llm_sub:sub reply",
			"assistant@llm_3:Final answer",
			"user@llm_4:Second prompt",
			"assistant@llm_4:Second ANSWER",
		}))
		Expect(resp.Items[5].ThreadID).To(Equal("sub1"))
	})

	It("filters by role", func() {
		_, body := getTranscript(server, pagedSessionID, "?role=user")
		Expect(summaries(decodeTranscript(body).Items)).To(Equal([]string{
			"user@llm_1:Hello world",
			"user@llm_4:Second prompt",
		}), "role=user keeps only rows with genuine user text")

		_, body = getTranscript(server, pagedSessionID, "?role=assistant")
		Expect(summaries(decodeTranscript(body).Items)).To(Equal([]string{
			"assistant@llm_1:Hi there call(toolu_1)",
			"assistant@llm_2:Done reading call(toolu_2) call(toolu_3)",
			"assistant@llm_3:Final answer",
			"assistant@llm_4:Second ANSWER",
		}))
	})

	It("filters by a case-insensitive substring of the row text", func() {
		_, body := getTranscript(server, pagedSessionID, "?q=answer")
		Expect(summaries(decodeTranscript(body).Items)).To(Equal([]string{
			"assistant@llm_3:Final answer",
			"assistant@llm_4:Second ANSWER",
		}))

		_, body = getTranscript(server, pagedSessionID, "?q=answer&role=user")
		Expect(decodeTranscript(body).Items).To(BeEmpty())
	})

	DescribeTable("pages through the same rows at every page size",
		func(query string, want []string) {
			for limit := 1; limit <= len(want)+1; limit++ {
				rows, pages := walkTranscript(server, fmt.Sprintf("?limit=%d%s", limit, query))
				Expect(summaries(rows)).To(Equal(want), "limit=%d", limit)
				Expect(pages).To(Equal(max(1, (len(want)+limit-1)/limit)),
					"limit=%d: next_cursor appears only when another row matches", limit)
			}
		},
		Entry("root thread", "", rootTranscript),
		Entry("user rows", "&role=user", []string{"user@llm_1:Hello world", "user@llm_4:Second prompt"}),
	)

	It("pages consistently with subagents and a substring filter", func() {
		for _, query := range []string{"&subagents=true", "&q=e"} {
			full, _ := walkTranscript(server, "?limit=1000"+query)
			Expect(full).NotTo(BeEmpty())
			for limit := 1; limit <= len(full); limit++ {
				rows, _ := walkTranscript(server, fmt.Sprintf("?limit=%d%s", limit, query))
				Expect(summaries(rows)).To(Equal(summaries(full)), "limit=%d query=%s", limit, query)
			}
		}
	})

	It("closes a page on its byte budget", func() {
		saved := transcriptPageByteBudget
		transcriptPageByteBudget = 1
		DeferCleanup(func() { transcriptPageByteBudget = saved })

		_, body := getTranscript(server, pagedSessionID, "")
		resp := decodeTranscript(body)
		Expect(resp.Items).To(HaveLen(1))
		Expect(resp.NextCursor).NotTo(BeEmpty())

		rows, _ := walkTranscript(server, "?limit=200")
		Expect(summaries(rows)).To(Equal(rootTranscript))
	})

	It("returns 404 for an unknown session", func() {
		status, body := getTranscript(server, "11111111-2222-4333-8444-555555555555", "")
		Expect(status).To(Equal(http.StatusNotFound), string(body))
	})

	DescribeTable("rejects malformed requests",
		func(sessionID, query string) {
			status, body := getTranscript(server, sessionID, query)
			Expect(status).To(Equal(http.StatusBadRequest), string(body))
		},
		Entry("non-uuid id", "not-a-uuid", ""),
		Entry("unknown role", pagedSessionID, "?role=system"),
		Entry("non-boolean subagents", pagedSessionID, "?subagents=maybe"),
		Entry("zero limit", pagedSessionID, "?limit=0"),
		Entry("negative limit", pagedSessionID, "?limit=-1"),
		Entry("garbage cursor", pagedSessionID, "?cursor=%%%"),
		Entry("cursor for another session", pagedSessionID, "?cursor="+encodeCursor(transcriptPageCursor{
			Session: "11111111-2222-4333-8444-555555555555", TraceID: "trc-b",
		})),
	)

	It("serves the same page as the core MCP transcript tool", func(ctx SpecContext) {
		httpServer := httptest.NewServer(server.mcpServer.Handler())
		DeferCleanup(httpServer.Close)
		session, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil).
			Connect(ctx, &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(session.Close)

		tools, err := session.ListTools(ctx, nil)
		Expect(err).NotTo(HaveOccurred())
		names := make([]string, 0, len(tools.Tools))
		for _, t := range tools.Tools {
			names = append(names, t.Name)
		}
		Expect(names).To(ContainElement("transcript"))

		result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "transcript",
			Arguments: map[string]any{"session_id": pagedSessionID, "role": "user"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		encoded, err := json.Marshal(result.StructuredContent)
		Expect(err).NotTo(HaveOccurred())
		Expect(summaries(decodeTranscript(encoded).Items)).To(Equal([]string{
			"user@llm_1:Hello world",
			"user@llm_4:Second prompt",
		}))

		missing, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "transcript",
			Arguments: map[string]any{"session_id": "11111111-2222-4333-8444-555555555555"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(missing.IsError).To(BeTrue(), "an unknown session is a tool error")

		negative, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "transcript",
			Arguments: map[string]any{"session_id": pagedSessionID, "limit": -1},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(negative.IsError).To(BeTrue(), "a negative limit is rejected as it is on HTTP")
	})

	It("bounds an MCP transcript call by its own timeout", func(ctx SpecContext) {
		driver := newTranscriptModel()
		seen := make(chan bool, 1)
		driver.onListTraceSummaries = func(c context.Context) {
			_, ok := c.Deadline()
			seen <- ok
		}
		guarded := newPagedServer(driver)
		httpServer := httptest.NewServer(guarded.mcpServer.Handler())
		DeferCleanup(httpServer.Close)
		session, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil).
			Connect(ctx, &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(session.Close)

		_, err = session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "transcript",
			Arguments: map[string]any{"session_id": pagedSessionID},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(seen).To(Receive(BeTrue()), "the builder runs under a deadline")
	})
})
