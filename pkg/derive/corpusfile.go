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

// CorpusWriter streams raw-turn rows into a gzipped-JSONL corpus dump —
// the inverse of LoadCorpus, and the on-disk shape `tapes dev
// dump-corpus` emits. Rows are appended one at a time so a scan of the
// interleaved raw layer (many sessions, insertion-ordered by id) can fan
// out to one open writer per session without buffering payloads. Each
// row carries its own `source`, so wire + transcript rows written to the
// same file round-trip back through LoadCorpus's source split. Close
// flushes the gzip trailer. The writer lives beside the loader so the
// two formats can never drift.
type CorpusWriter struct {
	gz  *gzip.Writer
	enc *json.Encoder
}

// NewCorpusWriter wraps w with the gzip + JSONL encoding the corpus
// format uses.
func NewCorpusWriter(w io.Writer) *CorpusWriter {
	gz := gzip.NewWriter(w)
	return &CorpusWriter{gz: gz, enc: json.NewEncoder(gz)}
}

// Write appends one raw-turn row, verbatim.
func (c *CorpusWriter) Write(rec *storage.RawTurnRecord) error {
	if err := c.enc.Encode(corpusRowFromRecord(rec)); err != nil {
		return fmt.Errorf("encode corpus row %d: %w", rec.ID, err)
	}
	return nil
}

// Close flushes and finalizes the gzip stream.
func (c *CorpusWriter) Close() error {
	if err := c.gz.Close(); err != nil {
		return fmt.Errorf("close corpus gzip: %w", err)
	}
	return nil
}

// WriteCorpus encodes a slice of raw-turn rows as a corpus dump in one
// call — the batch convenience over CorpusWriter.
func WriteCorpus(w io.Writer, rows []storage.RawTurnRecord) error {
	cw := NewCorpusWriter(w)
	for i := range rows {
		if err := cw.Write(&rows[i]); err != nil {
			return err
		}
	}
	return cw.Close()
}

// corpusRowFromRecord projects a storage record onto the corpus wire
// shape, the inverse of the assignment in LoadCorpus.
func corpusRowFromRecord(r *storage.RawTurnRecord) corpusRow {
	return corpusRow{
		ID: r.ID, OrgID: r.OrgID, Source: r.Source,
		Provider: r.Provider, AgentName: r.AgentName,
		HarnessID: r.HarnessID, HarnessSessionID: r.HarnessSessionID,
		RequestID: r.RequestID, RawRequest: r.RawRequest,
		Response: r.Response, Meta: r.Meta,
		SessionEnvelope: r.SessionEnvelope, ReceivedAt: r.ReceivedAt,
	}
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
