package derive

import (
	"encoding/json"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Folds: shadow calls whose §2g disposition is "fold" carry a value
// that belongs on the session rather than in the tree. title-gen is
// the first: its response IS the session's display title.

// maxFoldedTitleLen bounds the folded title; the generator contract is
// a short phrase, so anything longer is a malformed response we
// truncate rather than reject.
const maxFoldedTitleLen = 255

// SessionTitle extracts the generated title from a title-gen call's
// response ({"title": "…"}), or "" when the call isn't a title-gen or
// the response doesn't parse. Tolerates prose around the JSON object
// (models occasionally wrap it).
func SessionTitle(kind string, resp *llm.ChatResponse) string {
	if kind != KindTitleGen || resp == nil {
		return ""
	}
	text := strings.TrimSpace(responseText(resp))
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end <= start {
		return ""
	}
	var out struct {
		Title string `json:"title"`
	}
	if err := json.Unmarshal([]byte(text[start:end+1]), &out); err != nil {
		return ""
	}
	title := strings.TrimSpace(out.Title)
	if len(title) > maxFoldedTitleLen {
		title = title[:maxFoldedTitleLen]
	}
	return title
}
