package derive

import (
	"encoding/json"
	"errors"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

func TestParsePiTranscript(t *testing.T) {
	g := NewWithT(t)
	records := json.RawMessage(`[
		{"type":"session","id":"pi-1","cwd":"/work","version":3},
		{"type":"message","message":{"role":"user","content":[{"type":"text","text":"inspect it"}]}},
		{"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"call_1","name":"bash","arguments":{"command":"pwd"}}]}},
		{"type":"message","message":{"role":"toolResult","toolCallId":"call_1","toolName":"bash","content":[{"type":"text","text":"/work"}],"isError":false}}
	]`)
	rec := &storage.RawTurnRecord{
		Source: "transcript", HarnessID: "pi", HarnessSessionID: "pi-1",
		Meta: json.RawMessage(`{"transcript":true}`), RawRequest: records,
	}
	file, err := ParseTranscriptFile(rec)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(file.signatures).To(HaveLen(3))

	unknown := *rec
	unknown.HarnessID = "future-harness"
	_, err = ParseTranscriptFile(&unknown)
	g.Expect(err).To(MatchError(`unsupported transcript harness "future-harness"`))
	g.Expect(errors.Is(err, ErrUnsupportedTranscriptHarness)).To(BeTrue())
}
