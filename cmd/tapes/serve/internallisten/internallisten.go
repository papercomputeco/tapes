// Package internallisten starts the API server's internal listener when a
// deployment has asked for one.
//
// It is its own package because both commands that build an API server need it
// — `tapes serve` and `tapes serve api` — and the first already imports the
// second. A leaf package is what lets them share this without a cycle.
//
// The configuration is read from the environment and from nowhere else: no
// flag, no config.toml key, no `tapes config set`. That is deliberate on both
// halves of the pair. The token is a credential, and one written into a config
// file outlives the process that needed it while one passed as a flag is
// readable from any process table on the host. The address is a container port
// assignment, which belongs to whatever orchestrates the deployment rather
// than to the settings an operator edits by hand. Neither is a knob a person
// tunes, so neither appears on the surface where people look for knobs.
package internallisten

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/papercomputeco/tapes/api"
)

// Config reads the internal listener's configuration from the environment.
//
// An empty ListenAddr means no listener was asked for, which is the default
// and the state every non-Kubernetes deployment stays in — the documented
// Docker-only cassette loop included. Nothing about a plain `tapes serve`
// changes because this exists.
func Config() api.InternalConfig {
	return api.InternalConfig{
		ListenAddr: strings.TrimSpace(os.Getenv(api.EnvInternalListen)),
		Token:      os.Getenv(api.EnvInternalToken),
	}
}

// New builds the internal listener for server, returning a nil server when
// config asks for none.
//
// A nil server with a nil error is the "not configured" answer rather than an
// error, because not running an internal listener is the ordinary case. A
// listener that was asked for and cannot be built safely — configured with no
// token — is an error and stops startup, so a deployment cannot end up serving
// it unauthenticated.
func New(server *api.Server, config api.InternalConfig, log *slog.Logger) (*api.InternalServer, error) {
	if config.ListenAddr == "" {
		log.Debug("internal listener not configured", "variable", api.EnvInternalListen)

		return nil, nil //nolint:nilnil // absence is the ordinary answer here, not a failure
	}

	return server.NewInternalServer(config)
}

// AdminListener reads which listener serves the /v1/admin/ routes from the
// environment, and refuses a choice the configuration cannot honor: moving the
// admin routes to an internal listener that is not configured would leave them
// served by nothing, which is a deployment that looks healthy while every
// operator call 404s.
func AdminListener(config api.InternalConfig) (api.AdminListener, error) {
	listener, err := api.ParseAdminListener(os.Getenv(api.EnvAdminListener))
	if err != nil {
		return "", err
	}
	if listener == api.AdminListenerInternal && config.ListenAddr == "" {
		return "", fmt.Errorf("%s=%s requires the internal listener; set %s (and %s)",
			api.EnvAdminListener, api.AdminListenerInternal, api.EnvInternalListen, api.EnvInternalToken)
	}

	return listener, nil
}
