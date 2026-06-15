package derive

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// A corpus file is a gzipped JSONL dump of one session's raw_turns
// rows, captured live through a gateway. The corpus regression tests
// and the `tapes dev trace-fixtures` generator both replay these rows
// through the deriver, so the loader lives here rather than in test
// code.

// corpusRow mirrors the raw_turns columns a corpus dump carries.
type corpusRow struct {
	ID               int64           `json:"id"`
	OrgID            string          `json:"org_id"`
	Source           string          `json:"source"`
	Provider         string          `json:"provider"`
	AgentName        string          `json:"agent_name"`
	HarnessID        string          `json:"harness_id"`
	HarnessSessionID string          `json:"harness_session_id"`
	RequestID        string          `json:"request_id"`
	RawRequest       json.RawMessage `json:"raw_request"`
	Response         json.RawMessage `json:"response"`
	Meta             json.RawMessage `json:"meta"`
	SessionEnvelope  json.RawMessage `json:"session_envelope"`
	ReceivedAt       time.Time       `json:"received_at"`
}

// LoadCorpusFile reads a gzipped-JSONL corpus dump and returns its
// rows split by source: wire captures and transcript pushes.
func LoadCorpusFile(path string) (wire, transcripts []storage.RawTurnRecord, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open corpus %s: %w", path, err)
	}
	defer f.Close()

	wire, transcripts, err = LoadCorpus(f)
	if err != nil {
		return nil, nil, fmt.Errorf("corpus %s: %w", path, err)
	}
	return wire, transcripts, nil
}

// LoadCorpus reads a gzipped-JSONL corpus dump from r and returns its
// rows split by source: wire captures and transcript pushes. The
// io.Reader form serves embedded corpora (the demo seed bundles corpus
// files into the binary); LoadCorpusFile wraps it for on-disk dumps.
func LoadCorpus(r io.Reader) (wire, transcripts []storage.RawTurnRecord, err error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("gunzip corpus: %w", err)
	}

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row corpusRow
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, nil, fmt.Errorf("decode corpus row: %w", err)
		}
		rec := storage.RawTurnRecord{
			ID: row.ID, OrgID: row.OrgID, Source: row.Source,
			Provider: row.Provider, AgentName: row.AgentName,
			HarnessID: row.HarnessID, HarnessSessionID: row.HarnessSessionID,
			RequestID: row.RequestID, RawRequest: row.RawRequest,
			Response: row.Response, Meta: row.Meta,
			SessionEnvelope: row.SessionEnvelope, ReceivedAt: row.ReceivedAt,
		}
		if rec.Source == storage.RawTurnSourceTranscript {
			transcripts = append(transcripts, rec)
		} else {
			wire = append(wire, rec)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("scan corpus: %w", err)
	}
	return wire, transcripts, nil
}
