package backfill

import "fmt"

// Result contains statistics from a backfill run.
type Result struct {
	Matched               int
	Skipped               int
	Unmatched             int
	TotalTokensBackfilled int
	TranscriptFiles       int
	TranscriptEntries     int
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
