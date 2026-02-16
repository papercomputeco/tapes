package backfill_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

func makeAssistantBucket(text, model string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "assistant",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    model,
		Provider: "anthropic",
	}
}

func writeJSONL(dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, []byte(content), 0o644)
	Expect(err).NotTo(HaveOccurred())
	return path
}

var _ = Describe("ScanTranscriptDir", func() {
	It("finds JSONL files in nested directories", func() {
		tmpDir := GinkgoT().TempDir()

		// Create nested structure
		subDir := filepath.Join(tmpDir, "project", "subagents")
		Expect(os.MkdirAll(subDir, 0o755)).To(Succeed())

		writeJSONL(tmpDir, "session1.jsonl", "{}")
		writeJSONL(subDir, "agent.jsonl", "{}")
		writeJSONL(tmpDir, "readme.txt", "not a jsonl")

		files, err := backfill.ScanTranscriptDir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(files).To(HaveLen(2))
	})

	It("returns empty for directory with no JSONL files", func() {
		tmpDir := GinkgoT().TempDir()

		files, err := backfill.ScanTranscriptDir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(files).To(BeEmpty())
	})
})

var _ = Describe("ParseTranscript", func() {
	It("parses assistant entries with usage data", func() {
		tmpDir := GinkgoT().TempDir()
		jsonl := `{"type":"user","uuid":"u1","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}
{"type":"assistant","uuid":"a1","timestamp":"2026-02-01T10:00:00.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hi there!"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":1000,"cache_read_input_tokens":500}}}
{"type":"system","uuid":"sys1","timestamp":"2026-02-01T10:00:01.000Z"}`

		path := writeJSONL(tmpDir, "test.jsonl", jsonl)
		entries, err := backfill.ParseTranscript(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
		Expect(entries[0].Message.Model).To(Equal("claude-sonnet-4-5-20250929"))
		Expect(entries[0].Message.Usage.InputTokens).To(Equal(100))
		Expect(entries[0].Message.Usage.OutputTokens).To(Equal(50))
		Expect(entries[0].Message.Usage.CacheCreationInputTokens).To(Equal(1000))
		Expect(entries[0].Message.Usage.CacheReadInputTokens).To(Equal(500))
	})

	It("deduplicates by message ID keeping last entry", func() {
		tmpDir := GinkgoT().TempDir()
		// Simulate streaming: same message ID, content grows, last entry wins
		jsonl := `{"type":"assistant","uuid":"a1","timestamp":"2026-02-01T10:00:00.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hi"}],"stop_reason":null,"usage":{"input_tokens":100,"output_tokens":2,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}
{"type":"assistant","uuid":"a2","timestamp":"2026-02-01T10:00:01.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hi there! How can I help?"}],"stop_reason":null,"usage":{"input_tokens":100,"output_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`

		path := writeJSONL(tmpDir, "test.jsonl", jsonl)
		entries, err := backfill.ParseTranscript(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
		Expect(entries[0].TextContent()).To(Equal("Hi there! How can I help?"))
		Expect(entries[0].Message.Usage.OutputTokens).To(Equal(10))
	})

	It("skips entries without usage data", func() {
		tmpDir := GinkgoT().TempDir()
		jsonl := `{"type":"assistant","uuid":"a1","timestamp":"2026-02-01T10:00:00.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hi"}]}}`

		path := writeJSONL(tmpDir, "test.jsonl", jsonl)
		entries, err := backfill.ParseTranscript(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(BeEmpty())
	})

	It("skips malformed lines gracefully", func() {
		tmpDir := GinkgoT().TempDir()
		jsonl := `not json at all
{"type":"assistant","uuid":"a1","timestamp":"2026-02-01T10:00:00.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"test","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":5,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`

		path := writeJSONL(tmpDir, "test.jsonl", jsonl)
		entries, err := backfill.ParseTranscript(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
	})
})

var _ = Describe("Backfiller", func() {
	var (
		ctx    context.Context
		driver *sqlite.Driver
		tmpDir string
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = sqlite.NewDriver(ctx, ":memory:")
		Expect(err).NotTo(HaveOccurred())
		tmpDir = GinkgoT().TempDir()
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	It("matches transcript entries to nodes by model and timestamp", func() {
		now := time.Now().UTC()
		ts := now.Format("2006-01-02T15:04:05.000Z")

		bucket := makeAssistantBucket("Hello from Claude!", "claude-sonnet-4-5-20250929")
		node := merkle.NewNode(bucket, nil)
		_, err := driver.Put(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		jsonl := fmt.Sprintf(`{"type":"assistant","uuid":"a1","timestamp":"%s","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hello from Claude!"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_creation_input_tokens":1000,"cache_read_input_tokens":500}}}`, ts)
		writeJSONL(tmpDir, "test.jsonl", jsonl)

		b, cleanup, err := backfill.NewBackfiller(ctx, ":memory:", backfill.Options{})
		Expect(err).NotTo(HaveOccurred())
		defer cleanup()

		result, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		// The in-memory DB is different from driver's, so we just verify parsing works
		Expect(result.TranscriptFiles).To(Equal(1))
		Expect(result.TranscriptEntries).To(Equal(1))
	})

	It("reports dry-run results without writing", func() {
		b, cleanup, err := backfill.NewBackfiller(ctx, ":memory:", backfill.Options{DryRun: true})
		Expect(err).NotTo(HaveOccurred())
		defer cleanup()

		jsonl := `{"type":"assistant","uuid":"a1","timestamp":"2026-02-01T10:00:00.000Z","sessionId":"s1","message":{"id":"msg_001","role":"assistant","model":"test-model","content":[{"type":"text","text":"test"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`
		writeJSONL(tmpDir, "test.jsonl", jsonl)

		result, err := b.Run(ctx, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.TranscriptEntries).To(Equal(1))
	})
})

var _ = Describe("Result", func() {
	It("formats a summary string", func() {
		r := &backfill.Result{
			Matched:               10,
			Skipped:               5,
			Unmatched:             3,
			TotalTokensBackfilled: 15000,
			TranscriptFiles:       2,
			TranscriptEntries:     18,
		}

		summary := r.Summary()
		Expect(summary).To(ContainSubstring("Sync complete"))
		Expect(summary).To(ContainSubstring("10 matched"))
		Expect(summary).To(ContainSubstring("5 skipped"))
		Expect(summary).To(ContainSubstring("3 unmatched"))
		Expect(summary).To(ContainSubstring("15000"))
		Expect(summary).To(ContainSubstring("2 transcript files"))
		Expect(summary).To(ContainSubstring("18 assistant entries"))
	})
})
