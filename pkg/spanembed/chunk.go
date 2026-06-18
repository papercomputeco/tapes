package spanembed

const (
	// chunkTokenBudget is the per-piece token target used when splitting an
	// oversized span. It sits under text-embedding-3's 8192-token hard limit so
	// a proportional split — which is only approximate, since we divide by
	// characters against a token count — lands safely under the limit in one
	// pass most of the time, with recursion as the backstop when it doesn't.
	chunkTokenBudget = 8000

	// avgCharsPerToken approximates characters per token for the mixed
	// prose/code tapes embeds. OpenAI's embeddings oversize error
	// ("Invalid 'input': maximum context length is 8192 tokens.") reports no
	// token count, so when a provider omits it we estimate from text length to
	// size the split and the oversize metric. Recursive re-splitting backstops
	// an underestimate.
	avgCharsPerToken = 4
)

// estimateTokens approximates the token count of text from its length, for
// providers whose oversize error omits the measured count.
func estimateTokens(text string) int {
	return (len([]rune(text)) + avgCharsPerToken - 1) / avgCharsPerToken
}

// splitParts divides text into pieces small enough to embed. reportedTokens is
// the token count the provider measured for the whole text (0 when the provider
// didn't report one, in which case it is estimated from length); it sizes the
// split so a span of, say, 25k tokens is cut into four pieces rather than halved
// repeatedly. The pieces concatenate back to the original text exactly. Returns
// nil when the text is too short to split.
func splitParts(text string, reportedTokens int) []string {
	runes := []rune(text)
	if len(runes) < 2 {
		return nil
	}
	tokens := reportedTokens
	if tokens <= 0 {
		tokens = estimateTokens(text)
	}
	n := 2
	if tokens > chunkTokenBudget {
		n = (tokens + chunkTokenBudget - 1) / chunkTokenBudget
	}
	if n < 2 {
		n = 2
	}
	if n > len(runes) {
		n = len(runes)
	}
	return splitRunesInto(runes, n)
}

// splitRunesInto cuts runes into n contiguous pieces of roughly equal length,
// nudging each cut to a nearby newline so a piece doesn't slice through the
// middle of a line when a line break is close to the even-division point. The
// concatenation of the returned pieces equals string(runes).
func splitRunesInto(runes []rune, n int) []string {
	total := len(runes)
	parts := make([]string, 0, n)
	window := (total / n) / 8
	start := 0
	for i := 1; i <= n && start < total; i++ {
		end := total
		if i < n {
			end = adjustToNewline(runes, start+1, i*total/n, window)
		}
		if end <= start {
			end = start + 1
		}
		if end > total {
			end = total
		}
		parts = append(parts, string(runes[start:end]))
		start = end
	}
	return parts
}

// adjustToNewline returns target moved to just after the nearest newline within
// ±window (clamped to [lo, len(runes)]), or target unchanged when no newline is
// close. Cutting after the newline keeps it as the tail of the current piece.
func adjustToNewline(runes []rune, lo, target, window int) int {
	if window < 1 {
		return target
	}
	low := max(target-window, lo)
	high := min(target+window, len(runes))
	best := -1
	bestDist := window + 1
	for i := low; i < high; i++ {
		if runes[i] != '\n' {
			continue
		}
		dist := i - target
		if dist < 0 {
			dist = -dist
		}
		if dist < bestDist {
			bestDist = dist
			best = i
		}
	}
	if best < 0 {
		return target
	}
	return best + 1
}
