package api

import (
	"fmt"
	"strings"
)

// AdminListener names the listener that serves the operator routes under
// /v1/admin/: the demo seed, the synchronous derive run, attribution repair,
// and the asynchronous derive runs.
//
// The routes are operator actions — writes that run for as long as the corpus
// takes — rather than part of the read surface. By default they share the API
// listener, as they always have. A deployment that wants the API listener to
// carry reads only moves them to the internal listener, beside the readiness
// evidence, where every request must present its bearer token.
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
