package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// This file is the serving instance's account of itself: what configuration it
// loaded, and what admitting that configuration produced. It answers one
// question the read API cannot — "has *this* process actually taken the
// configuration it was given" — for a reader that can address the instance
// directly.
//
// It exists because being Ready proves almost nothing here. The readiness
// probe is /ping, which returns "pong" unconditionally, while cassette
// discovery resolves asynchronously and retryably after the process is already
// serving: a pod passes its probe and answers 404 from /v1/cassettes/<name>
// for a further refresh interval. Anything gating on "the fleet has converged"
// therefore has to ask each instance, and has to be told admission results
// rather than intent.
//
// Everything published here is an identity, a digest, an admission result, or
// a timestamp. No configuration *value* appears, and no secret — the same
// discipline the discovery document keeps when it publishes a cassette's
// settings as schema and withholds even a declared default. This is also why
// the payload names no plan, tier, or entitlement: core does not know those
// concepts, and a reader that does can map an identity onto one itself.

// EvidenceSchema identifies this payload's shape. A consumer branches on it
// rather than sniffing fields, so a future shape can be introduced beside this
// one instead of silently replacing it.
const EvidenceSchema = "tapes.evidence/v1"

// InternalEvidencePath is where evidence is served on the internal listener.
//
// It is deliberately not a route on the API listener. The tenant-facing
// gateway rewrites a path prefix onto that listener's root before forwarding,
// so *any* path added there becomes publicly addressable; and the API process
// performs no authorization of its own, because tenancy is settled by the
// gateway in front of it. A second listener is what keeps this reachable by an
// operator and unreachable by a tenant.
const InternalEvidencePath = "/internal/readiness/evidence"

// DefaultInternalListen is the address the internal listener is documented to
// use when a deployment enables it. It is not a default in the usual sense:
// the listener is off unless an address is configured, so this is the value to
// configure rather than the value assumed.
const DefaultInternalListen = ":8092"

// Environment variables that configure the internal listener.
//
// They are read from the environment only — never from a flag or config.toml.
// A bearer token in a config file outlives the process that needed it, and one
// in a flag is readable from any process table on the host; this pair is
// delivered by whatever orchestrates the deployment, as a mounted secret and a
// port assignment, and neither belongs to the configuration surface an
// operator edits by hand.
const (
	// EnvInternalListen carries the internal listener's address. Unset means
	// the listener is not started at all, which is what keeps a plain
	// `tapes serve` — the documented Docker-only loop included — unchanged.
	EnvInternalListen = "TAPES_INTERNAL_LISTEN"

	// EnvInternalToken carries the bearer token the internal listener
	// requires. The listener refuses to start without it.
	EnvInternalToken = "TAPES_INTERNAL_TOKEN"
)

// Environment variables carrying this instance's identity.
//
// Core cannot discover any of this for itself without becoming a Kubernetes
// client, which is exactly the coupling it must not take on: tapes has to stay
// runnable with no cluster at all. So the deployment supplies them — from the
// downward API, where a pod's own name, uid, ip and node are available as
// field references — and an unset variable simply yields an absent field. The
// evidence is still usable without them; pod_uid is the one a reader
// correlating an address back to a pod will miss.
const (
	EnvPodName     = "TAPES_POD_NAME"
	EnvPodUID      = "TAPES_POD_UID"
	EnvPodIP       = "TAPES_POD_IP"
	EnvNodeName    = "TAPES_NODE_NAME"
	EnvReplicaSet  = "TAPES_REPLICA_SET"
	EnvImageDigest = "TAPES_IMAGE_DIGEST"
)

// Evidence is one serving instance's account of what it loaded and admitted.
//
// Every configured source appears exactly once, in Cassettes when it resolved
// to a cassette and in UnresolvedSources when it did not. So
// len(Cassettes) + len(UnresolvedSources) == Configuration.SourceCount, which
// lets a reader detect a truncated or partially-assembled answer without
// knowing anything about the individual entries. A served instance always
// holds a resolution loop, so the equality holds on the wire; only a test that
// substitutes a bare spec cache can produce empty lists beside a non-zero
// count, and nothing reads that over HTTP.
type Evidence struct {
	Schema        string                `json:"schema"`
	Instance      EvidenceInstance      `json:"instance"`
	Configuration EvidenceConfiguration `json:"configuration"`

	// Cassettes is every source with a cassette identity, ordered by name.
	// A cassette here is not necessarily serving: Admission says whether it
	// is, and a source admitted once keeps its name through a later refusal
	// because the route it published is still mounted.
	Cassettes []EvidenceCassette `json:"cassettes"`

	// UnresolvedSources is every configured source that has produced no
	// cassette, ordered by subject — never fetched, unreachable, or refused
	// on first sight. There is no name to report for these, only what was
	// asked for, which kind of "no" it was, and what went wrong.
	UnresolvedSources []EvidenceUnresolvedSource `json:"unresolved_sources"`

	// CheckedAt is stamped when the request is served, so it dates the
	// answer rather than the state behind it. A reader bounding freshness
	// should use its own read time and keep this only for skew diagnosis:
	// an instance's clock is not something to make a liveness decision on.
	CheckedAt string `json:"checked_at"`
}

// EvidenceInstance identifies the process that answered.
//
// The point of it is that an address is not an identity. A reader fanning out
// across a set of endpoints has to be able to tell a replacement instance from
// the one it listed a moment ago, or a pass can silently count a fresh pod as
// evidence about the pod it replaced.
type EvidenceInstance struct {
	// InstanceID is generated when the process starts. It is the identity
	// that needs no cluster: two answers carrying it are from the same
	// process, and a restart is visible even where the pod name did not
	// change.
	InstanceID string `json:"instance_id"`

	// StartedAt is when this process built its API server.
	StartedAt string `json:"started_at"`

	// The remainder are supplied by the deployment and absent when it
	// supplies nothing.
	PodName     string `json:"pod_name,omitempty"`
	PodUID      string `json:"pod_uid,omitempty"`
	PodIP       string `json:"pod_ip,omitempty"`
	NodeName    string `json:"node_name,omitempty"`
	ReplicaSet  string `json:"replica_set,omitempty"`
	ImageDigest string `json:"image_digest,omitempty"`
}

// EvidenceConfiguration is the identity of what this process loaded.
type EvidenceConfiguration struct {
	// SourceListDigest identifies the cassette source list in effect, and
	// SourceCount how many entries it held. See
	// cassetterunner.SourceListDigest for the construction, which is fixed so
	// a reader can compute the same value from its own copy of the intended
	// list and compare.
	SourceListDigest string `json:"source_list_digest"`
	SourceCount      int    `json:"source_count"`

	// LoadedAt is when that list was installed, absent when this process has
	// never been told what to serve. It moves on a reconfiguration that does
	// not restart the process, which is the case nothing outside the process
	// can witness.
	LoadedAt string `json:"loaded_at,omitempty"`

	// ContractVersion is the tapes contract this core serves — the same one
	// the discovery document advertises, so the two cannot disagree about
	// which surface this is.
	ContractVersion string `json:"contract_version"`
}

// EvidenceCassette is one source that resolved to a cassette, and whether core
// is currently serving from it.
type EvidenceCassette struct {
	Name string `json:"name"`

	// Source is the configured document URL with any credential redacted.
	Source string `json:"source"`

	// Admission is the discriminator a registry lookup cannot supply:
	// admitted, rejected, or unresolved.
	Admission cassetterunner.Admission `json:"admission"`

	// ManifestDigest identifies the versioned metadata embedded in the
	// admitted document; OpenAPIDigest identifies the document core
	// republished from it. Both are always present and may be empty — a
	// cassette refused before it ever published has neither.
	ManifestDigest string `json:"manifest_digest"`
	OpenAPIDigest  string `json:"openapi_digest"`

	// OpenAPIStatus is how current the cached document is.
	OpenAPIStatus tapesoapi.Status `json:"openapi_status"`

	// RoutePrefix is where this cassette is mounted. It is reported even for
	// a rejected entry, because a cassette whose refresh was refused keeps
	// serving the document it published — the route is live, and a reader
	// checking that a withdrawn cassette stopped answering needs to see that.
	RoutePrefix string `json:"route_prefix"`

	// AdmittedAt is when the current admission began, absent when this
	// source is not currently admitted.
	AdmittedAt string `json:"admitted_at,omitempty"`

	// Rejection is the current problem, absent when there is none.
	Rejection *EvidenceProblem `json:"rejection,omitempty"`
}

// EvidenceProblem is something core could not do, and when it last noticed.
//
// One shape serves both a rejection under a cassette and an entry in
// UnresolvedSources, because both answer the same three questions and a reader
// gains nothing from learning two spellings of them.
type EvidenceProblem struct {
	// Subject is what the problem is about: the configured document URL,
	// credential redacted.
	Subject string `json:"subject"`

	// Reason is the human-facing explanation, never parsed. It is empty for
	// a configured source no resolution pass has visited yet, which is a
	// state with a subject but no failure.
	Reason string `json:"reason"`

	// ObservedAt is when the reason was last seen, restamped on every
	// failing pass so a reader can tell a current problem from a stale one.
	ObservedAt string `json:"observed_at,omitempty"`
}

// EvidenceUnresolvedSource is a configured source that never earned a cassette
// identity, and which kind of "no" that was.
//
// The admission is the whole point of the entry existing as its own shape.
// Without it a document core fetched and refused is on the wire exactly like a
// source core could not reach at all, and those call for opposite actions: the
// first is a reachable service serving something wrong — fix the document —
// and the second is an address, a network, or a service that is not up. A
// reader could in principle tell them apart by reading Reason, but Reason is
// prose, documented as never parsed, and free to be reworded.
//
// It stays in UnresolvedSources rather than moving into Cassettes under an
// empty name, because Cassettes is keyed and ordered by a name these entries
// do not have. The exact accounting is unaffected either way:
// len(Cassettes) + len(UnresolvedSources) still equals SourceCount.
type EvidenceUnresolvedSource struct {
	EvidenceProblem

	// Admission is rejected or unresolved, never admitted — a source core
	// admitted has a cassette identity and is reported in Cassettes. It is
	// always present: an entry here is by construction a source with an
	// admission result and no name.
	Admission cassetterunner.Admission `json:"admission"`
}

// instanceIdentity reads this process's identity once, at construction.
//
// Once, because it is an identity: re-reading per request would imply a
// process can change which pod it is, and would make a torn answer possible
// where a reader most needs a stable one.
func instanceIdentity() EvidenceInstance {
	return EvidenceInstance{
		InstanceID:  uuid.NewString(),
		StartedAt:   evidenceTime(time.Now()),
		PodName:     os.Getenv(EnvPodName),
		PodUID:      os.Getenv(EnvPodUID),
		PodIP:       os.Getenv(EnvPodIP),
		NodeName:    os.Getenv(EnvNodeName),
		ReplicaSet:  os.Getenv(EnvReplicaSet),
		ImageDigest: os.Getenv(EnvImageDigest),
	}
}

// evidenceTime renders every timestamp in this payload the same way: UTC, RFC
// 3339, second precision. A zero time renders empty, which is how an absent
// timestamp is distinguished from the epoch.
//
// Second precision is not a rounding of something finer — nothing here is
// measured more precisely than a refresh pass — and publishing nanoseconds
// would advertise a resolution the underlying events do not have.
func evidenceTime(at time.Time) string {
	if at.IsZero() {
		return ""
	}

	return at.UTC().Format(time.RFC3339)
}

// buildEvidence assembles this instance's account as of checkedAt.
func (s *Server) buildEvidence(checkedAt time.Time) Evidence {
	// The configuration identity and the per-source state come from one
	// snapshot, taken under the resolution loop's own lock. Reading them
	// separately would let a reconfiguration land between the two and produce
	// an answer that is new configuration beside old sources — which is
	// exactly the answer a convergence check must never accept, because it
	// looks like the fleet took a change it has not taken.
	//
	// A spec cache that keeps no admission record — a handler test's stub —
	// reports no sources rather than having some invented for it, and the
	// identity falls back to what this process recorded when it was
	// configured. That is what a deployment with no resolution loop would
	// honestly report, and it is the only path where the two records can be
	// told apart at all.
	var snapshot cassetterunner.SourceListEvidence
	if reporter, ok := s.cassetteSpecs.(cassetterunner.EvidenceReporter); ok {
		snapshot = reporter.EvidenceSnapshot()
	} else {
		loaded := s.loadedSourceList()
		snapshot.SourceList = cassetterunner.SourceListIdentity{
			Digest:   loaded.digest,
			Count:    loaded.count,
			LoadedAt: loaded.loadedAt,
		}
	}

	evidence := Evidence{
		Schema:   EvidenceSchema,
		Instance: s.instance,
		Configuration: EvidenceConfiguration{
			SourceListDigest: snapshot.SourceList.Digest,
			SourceCount:      snapshot.SourceList.Count,
			LoadedAt:         evidenceTime(snapshot.SourceList.LoadedAt),
			ContractVersion:  string(currentContractVersion(s.contracts)),
		},
		Cassettes:         make([]EvidenceCassette, 0),
		UnresolvedSources: make([]EvidenceUnresolvedSource, 0),
		CheckedAt:         evidenceTime(checkedAt),
	}

	for _, source := range snapshot.Sources {
		problem := EvidenceProblem{Subject: source.Source, ObservedAt: evidenceTime(source.ObservedAt)}
		if source.Rejection != nil {
			problem.Reason = source.Rejection.Reason
		}

		// A source with no cassette identity has nothing to report under a
		// name, whether it was unreachable or refused on first sight. The
		// admission says which of the two it was, so an operator can pick the
		// remediation without reading the prose.
		if source.Name == "" {
			evidence.UnresolvedSources = append(evidence.UnresolvedSources, EvidenceUnresolvedSource{
				EvidenceProblem: problem,
				Admission:       source.Admission,
			})

			continue
		}

		entry := EvidenceCassette{
			Name:           string(source.Name),
			Source:         source.Source,
			Admission:      source.Admission,
			ManifestDigest: string(source.ManifestDigest),
			OpenAPIDigest:  string(source.OpenAPIDigest),
			OpenAPIStatus:  source.OpenAPIStatus,
			RoutePrefix:    source.RoutePrefix,
		}
		// The timestamp is published only while the admission it dates is
		// current. Carrying it through a rejection would read as a live
		// admission to anything scanning for one.
		if source.Admission == cassetterunner.AdmissionAdmitted {
			entry.AdmittedAt = evidenceTime(source.AdmittedAt)
		}
		if source.Rejection != nil {
			entry.Rejection = &problem
		}
		evidence.Cassettes = append(evidence.Cassettes, entry)
	}

	// Both lists are ordered so a reader diffing two answers sees a change
	// only when something actually changed, the same reason discovery orders
	// its own.
	sort.Slice(evidence.Cassettes, func(i, j int) bool {
		return evidence.Cassettes[i].Name < evidence.Cassettes[j].Name
	})
	sort.Slice(evidence.UnresolvedSources, func(i, j int) bool {
		return evidence.UnresolvedSources[i].Subject < evidence.UnresolvedSources[j].Subject
	})

	return evidence
}

// handleReadinessEvidence serves this instance's account of itself.
func (s *Server) handleReadinessEvidence(c fiber.Ctx) error {
	c.Set(fiber.HeaderCacheControl, "no-store")

	// checked_at is stamped here rather than anywhere upstream: the whole
	// value of this endpoint is that the answer is assembled at read time.
	return c.JSON(s.buildEvidence(time.Now()))
}

// InternalConfig configures the internal listener.
type InternalConfig struct {
	// ListenAddr is the address to listen on. Empty is not a default; it is
	// a caller that should not have asked for a listener.
	ListenAddr string

	// Token is the bearer token every request must present.
	Token string
}

// InternalServer is the operator-facing listener carrying the evidence
// endpoint and nothing else.
//
// It is a separate server rather than a route group because the separation is
// the security property: the API listener sits behind a gateway that rewrites
// a tenant path prefix onto its root, so a path is either on a listener a
// tenant can reach or on one it cannot, and no middleware ordering makes the
// first case safe.
type InternalServer struct {
	app    *fiber.App
	addr   string
	logger *slog.Logger
}

// NewInternalServer builds this server's internal listener.
//
// It refuses rather than defaults when the token is missing. A listener that
// came up unauthenticated because a secret failed to mount would be a hole
// nothing reports: the deployment looks healthy, the endpoint answers, and the
// only signal is the absence of a rejection nobody is watching for.
func (s *Server) NewInternalServer(config InternalConfig) (*InternalServer, error) {
	if config.ListenAddr == "" {
		return nil, errors.New("internal listener: no listen address was configured")
	}
	if config.Token == "" {
		return nil, errors.New("internal listener: " + EnvInternalToken +
			" must be set when " + EnvInternalListen + " is; refusing to serve it unauthenticated")
	}

	app := fiber.New()
	// Recovery is outermost so a panic assembling evidence cannot take down a
	// process that is otherwise serving its tenant perfectly well.
	app.Use(recover.New())
	// Authentication is registered with Use rather than on the route, so
	// every path on this listener — including the ones that do not exist —
	// answers 401 before it answers 404. What is mounted here is not
	// something an unauthenticated caller should be able to enumerate.
	app.Use(requireBearer(config.Token))
	app.Get(InternalEvidencePath, s.handleReadinessEvidence)

	return &InternalServer{app: app, addr: config.ListenAddr, logger: s.logger}, nil
}

// requireBearer refuses any request not carrying the expected bearer token.
//
// Both sides are hashed before the comparison. subtle.ConstantTimeCompare
// short-circuits on a length mismatch, so comparing the raw strings would leak
// the token's length through timing; fixed-width digests remove the only
// input-dependent branch left.
func requireBearer(token string) fiber.Handler {
	expected := sha256.Sum256([]byte(token))

	return func(c fiber.Ctx) error {
		presented, ok := bearerToken(c.Get(fiber.HeaderAuthorization))
		if ok {
			candidate := sha256.Sum256([]byte(presented))
			ok = subtle.ConstantTimeCompare(candidate[:], expected[:]) == 1
		}
		if !ok {
			// The scheme is named because a client needs to know what to
			// present; nothing else is. A body distinguishing "no header"
			// from "wrong token" would turn this into an oracle — and
			// SendStatus would supply one, filling an empty body with the
			// status text, so the status is set without it.
			c.Set(fiber.HeaderWWWAuthenticate, "Bearer")
			c.Status(fiber.StatusUnauthorized)

			return nil
		}

		return c.Next()
	}
}

// bearerToken extracts the credential from an Authorization header. The scheme
// is matched case-insensitively, as the HTTP grammar requires.
func bearerToken(header string) (string, bool) {
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	credential = strings.TrimSpace(credential)

	return credential, credential != ""
}

// Run starts the internal listener and blocks.
func (i *InternalServer) Run() error {
	i.logger.Info("starting internal listener",
		"listen", i.addr,
		"path", InternalEvidencePath,
	)

	return i.app.Listen(i.addr, fiber.ListenConfig{DisableStartupMessage: true})
}

// RunWithListener starts the internal listener on an already-bound listener.
func (i *InternalServer) RunWithListener(listener net.Listener) error {
	i.logger.Info("starting internal listener",
		"listen", listener.Addr().String(),
		"path", InternalEvidencePath,
	)

	return i.app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true})
}

// Shutdown gracefully stops the internal listener.
func (i *InternalServer) Shutdown() error { return i.app.Shutdown() }
