package storage

import (
	"fmt"
	"regexp"
	"strings"
)

// publishedViewSegment mirrors the admission grammar for one identifier
// segment of a published view name (pkg/cassette/v1alpha1): lowercase snake,
// at most 63 bytes. It is restated here rather than imported because storage
// must not trust its callers — the parse below is the only constructor, so
// the grammar holds by construction wherever a PublishedViewName appears.
var publishedViewSegment = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// PublishedViewName is an opaque, SQL-safe, schema-qualified view identifier.
//
// It exists for the same reason SortColumn does: this name reaches SQL in an
// identifier position, and a manifest is remote input. Its fields are
// unexported and ParsePublishedViewName is the only constructor, so a value
// that exists has passed the grammar; Quoted is the ONE place a published
// view identifier is rendered into SQL. No other manifest string may reach an
// identifier position.
type PublishedViewName struct {
	schema string
	view   string
}

// ParsePublishedViewName validates and constructs a published view name from
// its qualified "schema.view" spelling.
func ParsePublishedViewName(qualified string) (PublishedViewName, error) {
	schema, view, found := strings.Cut(qualified, ".")
	if !found || !publishedViewSegment.MatchString(schema) || !publishedViewSegment.MatchString(view) {
		return PublishedViewName{}, fmt.Errorf(
			"published view %q must be a schema-qualified lower-snake identifier (schema.view, each segment at most 63 bytes)",
			qualified)
	}

	return PublishedViewName{schema: schema, view: view}, nil
}

// Quoted renders the identifier for SQL, both segments double-quoted. The
// grammar above already excludes every character that could escape a quoted
// identifier (there is no `"` to double), so quoting here is defense in depth
// on top of the parse — and the single rendering point reviews are anchored
// to.
func (name PublishedViewName) Quoted() string {
	return `"` + name.schema + `"."` + name.view + `"`
}

// String returns the plain qualified spelling, empty for the zero value.
func (name PublishedViewName) String() string {
	if name.IsZero() {
		return ""
	}

	return name.schema + "." + name.view
}

// IsZero reports whether the name was never parsed. A zero name must never
// render into SQL; readers treat it as an evaluation failure, not as "no
// filter".
func (name PublishedViewName) IsZero() bool { return name.schema == "" }

// PublishedColumnName is an opaque, SQL-safe column identifier: the
// claim-declared value column (match.value_column) an EXISTS probe compares
// against. It exists for the same reason PublishedViewName does — the name
// reaches SQL in an identifier position and a manifest is remote input — and
// follows the same discipline: ParsePublishedColumnName is the only
// constructor, so a value that exists has passed the grammar, and Quoted is
// the ONE place it renders into SQL.
type PublishedColumnName struct {
	column string
}

// ParsePublishedColumnName validates and constructs a published value-column
// name: one lower-snake identifier segment of at most 63 bytes — the same
// grammar admission enforces on match.value_column, restated because storage
// must not trust its callers.
func ParsePublishedColumnName(column string) (PublishedColumnName, error) {
	if !publishedViewSegment.MatchString(column) {
		return PublishedColumnName{}, fmt.Errorf(
			"published value column %q must be a lower-snake identifier of at most 63 bytes", column)
	}

	return PublishedColumnName{column: column}, nil
}

// Quoted renders the identifier for SQL, double-quoted. The grammar already
// excludes every character that could escape a quoted identifier, so quoting
// is defense in depth on top of the parse.
func (name PublishedColumnName) Quoted() string {
	return `"` + name.column + `"`
}

// String returns the plain spelling, empty for the zero value.
func (name PublishedColumnName) String() string { return name.column }

// IsZero reports whether the name was never parsed. A zero column must never
// render into SQL; readers treat it as an evaluation failure, not as a
// hardcoded default.
func (name PublishedColumnName) IsZero() bool { return name.column == "" }

// PublishedFilter is a claim-derived filter over a cassette-published
// attachment view: the view to probe, the primitive-type value that scopes it
// to this surface, the claim-declared value column to compare, and the
// already-normalized filter values. It is generic —
// derived from whatever claim the handler consulted, never named after any
// particular cassette.
//
// Each value renders one independent EXISTS predicate (repeat is AND), inside
// the same query that sorts and paginates. Values arrive normalized per the
// claim's declared profile; storage binds them verbatim, which is what keeps
// the probe an exact-match index access.
type PublishedFilter struct {
	View      PublishedViewName
	TypeValue string
	// Column is the claim-declared value column the probe compares against
	// (match.value_column). Like View it reaches SQL in an identifier
	// position, so it is opaque: only its validating parser constructs it
	// and only its quoting helper renders it.
	Column PublishedColumnName
	Values []string
}
