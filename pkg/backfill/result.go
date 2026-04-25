package backfill

import "fmt"

// Result contains statistics from a backfill run.
type Result struct {
	Matched               int `json:"matched"`
	Skipped               int `json:"skipped"`
	Unmatched             int `json:"unmatched"`
	TotalTokensBackfilled int `json:"total_tokens_backfilled"`
	TranscriptFiles       int `json:"transcript_files"`
	TranscriptEntries     int `json:"transcript_entries"`
}

// Summary returns a human-readable summary of the sync result.
func (r *Result) Summary() string {
	return fmt.Sprintf(
		"Sync complete: %d matched, %d skipped (already have tokens), %d unmatched\n"+
			"Scanned %d transcript files (%d assistant entries)\n"+
			"Total tokens synced: %d",
		r.Matched, r.Skipped, r.Unmatched,
		r.TranscriptFiles, r.TranscriptEntries,
		r.TotalTokensBackfilled,
	)
}
