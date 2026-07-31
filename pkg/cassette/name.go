package cassette

import (
	"fmt"
	"regexp"
	"strings"
)

// Name is a validated cassette identity. Construct names with ParseName.
type Name string

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)

// reservedNames are the names a cassette may not take, and what each collides
// with.
//
// The list is deliberately short. A cassette is always served beneath
// /v1/cassettes/<name>, so its name cannot shadow a core route whatever it is —
// a cassette called "ping" answers at /v1/cassettes/ping and leaves /ping
// alone. Reserving route-shaped names bought nothing and cost authors the
// obvious name for their cassette.
//
// What a name cannot escape is Postgres. It becomes the cassette's schema
// directly, in a database core also lives in, so the collisions worth refusing
// are the ones the database would enforce or the ones core has already taken.
var reservedNames = map[string]string{
	"public": "the Postgres schema core's own tables live in",
	"tapes":  "core's schema and role prefix",
}

// ParseName validates and constructs a cassette name.
func ParseName(value string) (Name, error) {
	if reason, ok := reservedNames[value]; ok {
		return "", fmt.Errorf("cassette name %q is reserved for %s", value, reason)
	}
	if strings.HasPrefix(value, "pg_") {
		return "", fmt.Errorf("cassette name %q uses the Postgres-reserved pg_ prefix", value)
	}
	if !namePattern.MatchString(value) {
		return "", fmt.Errorf("cassette name %q must match %s", value, namePattern)
	}

	return Name(value), nil
}

// SchemaName returns the cassette's Postgres schema name.
func (n Name) SchemaName() string { return string(n) }

// RoleName returns the cassette's Postgres role name.
func (n Name) RoleName() string { return "cassette_" + string(n) }
