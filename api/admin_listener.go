package api

import (
	"fmt"
	"strings"
)

// AdminListener names the listener that serves the operator routes under
// /v1/admin/: the demo seed, the synchronous derive run, attribution repair,
// and the asynchronous derive runs.
//
// The routes are writes that run for as long as the corpus takes, and the API
// process performs no authorization of its own. On a deployment where the API
// listener is only reachable by its operator that is fine, and it is the
// default. A deployment that puts a gateway in front of the API listener — one
// that rewrites a public path prefix onto its root, say — makes every route
// there reachable by whoever the gateway admits; such a deployment moves the
// admin routes to the internal listener, which requires its bearer token on
// every request.
//
// The routes are described in the read API's OpenAPI document either way: the
// knob decides which port serves them, not what they are.
type AdminListener string

const (
	// AdminListenerAPI serves the admin routes on the API listener.
	AdminListenerAPI AdminListener = "api"

	// AdminListenerInternal serves them only on the internal listener.
	AdminListenerInternal AdminListener = "internal"
)

// EnvAdminListener selects the admin listener: "api" (the default) or
// "internal". Like the internal listener's own pair it is read from the
// environment only, because it is a fact about how a deployment is exposed and
// belongs to whatever orchestrates it.
const EnvAdminListener = "TAPES_ADMIN_LISTENER"

// ParseAdminListener reads an EnvAdminListener value. Empty means the default.
func ParseAdminListener(value string) (AdminListener, error) {
	switch AdminListener(strings.ToLower(strings.TrimSpace(value))) {
	case "", AdminListenerAPI:
		return AdminListenerAPI, nil
	case AdminListenerInternal:
		return AdminListenerInternal, nil
	default:
		return "", fmt.Errorf("%s must be %q or %q, got %q",
			EnvAdminListener, AdminListenerAPI, AdminListenerInternal, value)
	}
}
