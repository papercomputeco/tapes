package rawequiv

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// DiffKind classifies one structural difference.
type DiffKind string

const (
	// DiffMissingInRecomputed marks a path the stored reduction has and the
	// recomputed one does not — the direction that means moving to raw would
	// LOSE a field.
	DiffMissingInRecomputed DiffKind = "missing_in_recomputed"

	// DiffMissingInStored marks a path only the recomputed reduction has.
	DiffMissingInStored DiffKind = "missing_in_stored"

	// DiffType marks a path present on both sides with different JSON types.
	DiffType DiffKind = "type_mismatch"

	// DiffValue marks a path present on both sides, same type, different
	// value.
	DiffValue DiffKind = "value_mismatch"

	// DiffLength marks arrays of differing length. Element differences are
	// reported separately for the overlapping prefix.
	DiffLength DiffKind = "length_mismatch"
)

// FieldDiff is one structural difference between two reductions.
//
// Stored and Recomputed are RENDERINGS, never raw values. Reductions of real
// traffic contain prompts and completions, and this tool's output goes into
// terminals, CI logs and paste buffers; a diff that printed the values would
// make running it on production traffic a data-handling event. Renderings
// carry shape (type, length, digest) which is what a reviewer needs to decide
// whether a divergence is structural or cosmetic, and identifiers from a small
// allowlist of provider-controlled fields.
type FieldDiff struct {
	Path       string   `json:"path"`
	Kind       DiffKind `json:"kind"`
	Stored     string   `json:"stored,omitempty"`
	Recomputed string   `json:"recomputed,omitempty"`
}

// safePaths are the JSON paths whose string values may be printed verbatim.
//
// Every entry is provider-controlled vocabulary rather than model output: a
// model name, a stop reason, a role, a block or object type, a message id.
// Knowing that `stop_reason` went from "end_turn" to "" is the whole content
// of that divergence, and redacting it to a digest would make the report
// useless for the one thing it exists to do. Anything not listed here is
// rendered as shape only — the default is deny.
//
// Paths are normalized: array indices collapse to "[]", so
// message.content[3].type matches message.content[].type.
var safePaths = map[string]bool{
	"model":                         true,
	"stop_reason":                   true,
	"done":                          true,
	"message.role":                  true,
	"message.content[].type":        true,
	"extra.type":                    true,
	"extra.status":                  true,
	"extra.object":                  true,
	"extra.id":                      true,
	"extra.incomplete":              true,
	"extra.partial":                 true,
	"message.content[].name":        true,
	"message.content[].tool_use_id": true,
}

// normalizePath collapses array indices so a path can be looked up in
// safePaths regardless of position.
func normalizePath(path string) string {
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		if path[i] != '[' {
			b.WriteByte(path[i])
			continue
		}
		end := strings.IndexByte(path[i:], ']')
		if end < 0 {
			b.WriteByte(path[i])
			continue
		}
		b.WriteString("[]")
		i += end
	}
	return b.String()
}

// render produces a printable, content-safe description of a decoded JSON
// value at a path.
func render(path string, v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(t)
	case float64:
		// Numbers are token counts, indices, sizes and timestamps. None of
		// them carry prompt text, so they print verbatim.
		if t == math.Trunc(t) && math.Abs(t) < 1e15 {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	case string:
		if safePaths[normalizePath(path)] {
			return strconv.Quote(t)
		}
		sum := sha256.Sum256([]byte(t))
		return fmt.Sprintf("string(len=%d,sha256=%s)", len(t), hex.EncodeToString(sum[:4]))
	case []any:
		return fmt.Sprintf("array(len=%d)", len(t))
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return fmt.Sprintf("object(keys=[%s])", strings.Join(keys, " "))
	default:
		return "unknown"
	}
}

// jsonType names the JSON type of a decoded value, for type-mismatch reports.
func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

// decodeCanonical decodes JSON into the generic tree the differ walks.
//
// Both sides must go through this. The stored side comes back from Postgres
// jsonb, which has already normalized key order, whitespace, duplicate keys
// and number formatting; the recomputed side is a fresh Go marshal that has
// not. Comparing the two as bytes would therefore report divergences that are
// entirely artifacts of the storage round-trip. Decoding both to a generic
// tree and comparing structurally is what makes the comparison a statement
// about the reduction rather than about jsonb.
//
// Numbers land as float64, which is what makes 1e2 and 100 compare equal —
// the normalization jsonb performs and Go's marshaller does not. The cost is
// that integers beyond 2^53 would lose precision. Nothing in a reduction is
// that large (token counts, byte counts, block indices, and a nanosecond
// duration that is excluded from the comparison anyway), so the trade buys
// jsonb-insensitivity for no reachable loss.
func decodeCanonical(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// prune removes a path from a decoded tree. Used to drop the fields that are
// legitimately allowed to differ before the comparison runs, so those fields
// cannot mask or manufacture a divergence.
func prune(v any, path []string) {
	if len(path) == 0 {
		return
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return
	}
	if len(path) == 1 {
		delete(obj, path[0])
		return
	}
	prune(obj[path[0]], path[1:])
}

// diffValues walks two decoded trees in parallel and appends every structural
// difference to out, stopping once max differences have been collected.
//
// The bound is per comparison rather than per level: one row that diverges
// everywhere must not be able to produce an unbounded report, and the first
// handful of paths is enough to characterize a divergence class. The caller
// reports that truncation happened rather than hiding it.
func diffValues(path string, stored, recomputed any, out *[]FieldDiff, limit int) {
	if len(*out) >= limit {
		return
	}

	if jsonType(stored) != jsonType(recomputed) {
		*out = append(*out, FieldDiff{
			Path:       path,
			Kind:       DiffType,
			Stored:     jsonType(stored),
			Recomputed: jsonType(recomputed),
		})
		return
	}

	switch s := stored.(type) {
	case map[string]any:
		r := recomputed.(map[string]any)
		keys := make([]string, 0, len(s)+len(r))
		seen := map[string]bool{}
		for k := range s {
			if !seen[k] {
				keys = append(keys, k)
				seen[k] = true
			}
		}
		for k := range r {
			if !seen[k] {
				keys = append(keys, k)
				seen[k] = true
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			if len(*out) >= limit {
				return
			}
			child := k
			if path != "" {
				child = path + "." + k
			}
			sv, sok := s[k]
			rv, rok := r[k]
			switch {
			case sok && !rok:
				*out = append(*out, FieldDiff{Path: child, Kind: DiffMissingInRecomputed, Stored: render(child, sv)})
			case !sok && rok:
				*out = append(*out, FieldDiff{Path: child, Kind: DiffMissingInStored, Recomputed: render(child, rv)})
			default:
				diffValues(child, sv, rv, out, limit)
			}
		}

	case []any:
		r := recomputed.([]any)
		if len(s) != len(r) {
			*out = append(*out, FieldDiff{
				Path:       path,
				Kind:       DiffLength,
				Stored:     strconv.Itoa(len(s)),
				Recomputed: strconv.Itoa(len(r)),
			})
		}
		n := min(len(s), len(r))
		for i := range n {
			if len(*out) >= limit {
				return
			}
			diffValues(fmt.Sprintf("%s[%d]", path, i), s[i], r[i], out, limit)
		}

	default:
		if stored != recomputed {
			*out = append(*out, FieldDiff{
				Path:       path,
				Kind:       DiffValue,
				Stored:     render(path, stored),
				Recomputed: render(path, recomputed),
			})
		}
	}
}
