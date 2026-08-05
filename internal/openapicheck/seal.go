package openapicheck

// Contract seals.
//
// The coverage check in this package answers "does the contract describe the
// surface the server serves". It cannot answer "did the contract change",
// because it compares the document against the routes it was compiled from —
// both move together, and a rename that alters every consumer's generated
// client leaves coverage perfectly green.
//
// A seal answers that second question. Each surface commits the fingerprint of
// its compiled document to a CONTRACT file next to the server that publishes
// it, and a spec recompiles and compares. A change to a route, a schema, a
// status code or a field name moves the fingerprint and fails until someone
// writes the new value in — which turns "this pull request changes a published
// contract" from something a reviewer has to infer out of a diff into a
// one-line diff that says so.
//
// What is sealed is the PROSE-STRIPPED document: compiled with nil TypeDocs,
// the same thing `tapes dev openapi --docs-root ''` renders and the same thing
// a deployed binary serves, since it has no source tree to read comments from.
// Sealing the documented document instead would make every doc-comment edit a
// contract event, and a gate that fires on prose is a gate that gets bumped
// without being read. The published artifacts under `make contracts` are the
// opposite choice for the opposite reason: consumers reading a vendored
// document want the field prose, and nothing gates those bytes.
//
// The rule lives here for the reason the coverage rule does — two servers, one
// rule, and no second copy to drift.

import (
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// SealResult reports whether a compiled contract still matches its seal.
type SealResult struct {
	// Surface names the contract, as a developer refers to it: "api", "ingest".
	Surface string

	// Path is the CONTRACT file, relative to the repository root, for the
	// failure message to name.
	Path string

	// Sealed is the fingerprint the CONTRACT file records. Empty when the file
	// could not be understood, in which case Problem says why.
	Sealed string

	// Compiled is the fingerprint recompiling the surface just produced.
	Compiled string

	// Problem describes an unreadable CONTRACT file. Empty when the file
	// parsed, whether or not the two fingerprints agree.
	Problem string
}

// OK reports whether the seal still describes the compiled contract.
func (r SealResult) OK() bool {
	return r.Problem == "" && r.Sealed == r.Compiled
}

// CheckSeal compares a compiled document against the contents of its CONTRACT
// file.
//
// It takes the file's bytes rather than its path so that the caller owns the
// read: a spec that reads the file itself can say "this file is missing" in its
// own voice, and this package stays free of filesystem access.
func CheckSeal(surface, path string, contents []byte, compiled *tapesoapi.CompiledDoc) SealResult {
	res := SealResult{Surface: surface, Path: path, Compiled: compiled.Fingerprint()}

	sealed, err := ParseSeal(contents)
	if err != nil {
		res.Problem = err.Error()

		return res
	}
	res.Sealed = sealed

	return res
}

// ParseSeal reads the fingerprint out of a CONTRACT file.
//
// The format is one fingerprint, with `#` comment lines and blank lines
// ignored. The comments are why this is not just a bare line the way
// fixtures/envelope/DIGEST is: a CONTRACT file is the first thing a developer
// opens after the gate fails, and it should be able to say what it seals
// without them going to find the spec that reads it. Everything else about it
// is deliberately the same — one line, so a deliberate bump is a one-line diff
// a reviewer cannot miss.
func ParseSeal(contents []byte) (string, error) {
	var found []string
	for line := range strings.SplitSeq(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		found = append(found, line)
	}

	switch {
	case len(found) == 0:
		return "", fmt.Errorf("no fingerprint found; expected one %q line", "sha256:...")
	case len(found) > 1:
		return "", fmt.Errorf("found %d fingerprints; expected exactly one", len(found))
	case !strings.HasPrefix(found[0], "sha256:"):
		return "", fmt.Errorf("fingerprint %q does not start with %q", found[0], "sha256:")
	}

	return found[0], nil
}

// Explain renders a failed seal as instructions. Empty when the seal holds.
//
// The new fingerprint goes in the message so that a legitimate contract change
// is a copy-paste rather than a hunt for the command that prints it. A gate
// nobody can satisfy quickly is a gate someone deletes.
func (r SealResult) Explain() string {
	if r.OK() {
		return ""
	}

	var b strings.Builder
	if r.Problem != "" {
		fmt.Fprintf(&b, "\n%s could not be read: %s\n", r.Path, r.Problem)
	} else {
		fmt.Fprintf(&b, "\nthe %s contract no longer matches %s.\n\n", r.Surface, r.Path)
		fmt.Fprintf(&b, "  sealed:   %s\n", r.Sealed)
		fmt.Fprintf(&b, "  compiled: %s\n", r.Compiled)
	}

	fmt.Fprintf(&b, `
This fingerprint covers the prose-stripped document — the routes, schemas,
parameters and responses the %s server registers. Doc comments are not in it,
so a comment edit cannot have caused this: something about the published shape
moved.

If you changed the %s contract on purpose, write this line into %s:

    %s

Bump it in the same change that moved the contract, never as a follow-up: the
seal is the record that a reviewer was told the surface changed, and a bump
that arrives separately is a change nobody was asked about.

To see what moved, render both documents and diff them:

    make contracts   # writes build/contracts/tapes-{api,ingest}.yaml
`, r.Surface, r.Surface, r.Path, r.Compiled)

	return b.String()
}
