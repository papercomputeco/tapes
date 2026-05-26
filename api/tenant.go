package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

const (
	// orgIDHeader carries the client-asserted tenant on read requests.
	// There is no auth layer to verify it: the value is trusted the same
	// way the ingest envelope's org_id and the admin handlers' body org_id
	// are trusted. It exists so a read can be scoped to one tenant — a
	// content hash that several tenants share would otherwise resolve to an
	// arbitrary one of them.
	orgIDHeader = "X-Tapes-Org-Id"

	// orgIDLocal is the fiber Locals key the tenant middleware writes and
	// the handlers read back.
	orgIDLocal = "org_id"

	// nilOrgID is the sentinel tenant used when no org header is supplied.
	// It matches the nil-UUID bucket that legacy and non-session writes
	// land in, so existing single-tenant callers keep working unchanged.
	nilOrgID = "00000000-0000-0000-0000-000000000000"
)

// withOrgContext canonicalises the client-asserted org_id header onto the
// request Locals. An absent or unparseable header falls back to the nil-org
// sentinel so the downstream lookup is always org-scoped to a valid UUID.
func (s *Server) withOrgContext(c *fiber.Ctx) error {
	c.Locals(orgIDLocal, canonicalOrgID(c.Get(orgIDHeader)))
	return c.Next()
}

// canonicalOrgID parses a raw header value to its canonical UUID string,
// falling back to the nil-org sentinel when empty or malformed.
func canonicalOrgID(raw string) string {
	if raw == "" {
		return nilOrgID
	}
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return nilOrgID
	}
	return parsed.String()
}

// orgIDFromCtx reads the canonical tenant the middleware stored, defaulting
// to the nil-org sentinel if the middleware did not run (e.g. in tests that
// invoke a handler helper directly).
func orgIDFromCtx(c *fiber.Ctx) string {
	if v, ok := c.Locals(orgIDLocal).(string); ok && v != "" {
		return v
	}
	return nilOrgID
}
