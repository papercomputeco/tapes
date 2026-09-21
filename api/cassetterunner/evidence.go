package cassetterunner

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Admission is what resolving one configured source produced.
//
// It exists because the registry cannot answer the question an operator — or
// anything verifying that a configuration change actually took — is really
// asking. A cassette absent from the registry may have been withdrawn, may
// have had its document refused, or may be behind a service that has not come
// up yet, and those three call for three different actions. Rejection carries
// the reason; this carries which kind of thing went wrong.
type Admission string

// The states one configured source can be in.
const (
	// AdmissionAdmitted means the document was fetched, accepted, and
	// registered: core is serving this cassette from this source right now.
	AdmissionAdmitted Admission = "admitted"

	// AdmissionRejected means the document was fetched and refused — a
	// malformed manifest, a contract core does not serve, a claim collision,
	// a name change, a losing priority tiebreak. The source is reachable and
	// wrong, which is a different problem from being unreachable.
	AdmissionRejected Admission = "rejected"

	// AdmissionUnresolved means core could not read a document at all: the
	// URL is malformed, or the fetch failed. It is also the state of a source
	// no pass has visited yet, because "not yet" and "not reachable" are the
	// same fact to a reader that only sees this moment.
	AdmissionUnresolved Admission = "unresolved"
)

// SourceEvidence is one configured source and what the last pass made of it.
//
// It is deliberately identities and results only — names, digests, admission
// states and timestamps. Nothing here is a configuration *value*, which is the
// same discipline the discovery document keeps: a reader learns what core
// loaded and whether it took, never what it was configured with.
type SourceEvidence struct {
	// Source is the configured OpenAPI document URL with any credential
	// redacted, and the subject any rejection for it is filed under.
	Source string

	// Name is the cassette this source resolved to, empty until it does. A
	// source that has never resolved has no cassette identity to report, only
	// a subject and a reason.
	Name cassette.Name

	// Admission is the state above.
	Admission Admission

	// AdmittedAt is when the current admission began, zero when this source
	// has never been admitted. It survives a successful refresh, so it reads
	// as "admitted since".
	AdmittedAt time.Time

	// ManifestDigest identifies the versioned metadata embedded in the
	// admitted document; OpenAPIDigest identifies the document core
	// republished from it. They are different identities of different things,
	// which is why both are reported.
	ManifestDigest cassette.Digest
	OpenAPIDigest  cassette.Digest

	// OpenAPIStatus is how current the cached document is: fresh, stale, or
	// missing.
	OpenAPIStatus tapesoapi.Status

	// RoutePrefix is where this cassette is mounted on the public surface,
	// empty when the source has no cassette identity.
	RoutePrefix string

	// Rejection is the current problem for this source, nil when there is
	// none. ObservedAt is when it was last seen, and is zero with no
	// rejection.
	Rejection  *Rejection
	ObservedAt time.Time
}

// SourceListIdentity is the identity of a configured source list, as the
// runner holding it recorded when it was installed.
//
// It lives beside the catalog rather than above it because the two are one
// fact: a digest that says "list B" beside sources that are still list A is
// worse than no digest at all — it is a convergence check that passes on the
// wrong evidence. Keeping both under the runner's lock is what makes the pair
// unable to disagree.
type SourceListIdentity struct {
	// Digest identifies the list. See SourceListDigest for the construction,
	// which is fixed so a reader can compute the same value from its own copy
	// of the intended list and compare.
	Digest string

	// Count is how many entries the list held. It is not derivable from the
	// digest, and it is the cheap sanity check on a digest mismatch: a
	// different count says the list changed shape, an equal count with a
	// different digest says its contents moved.
	Count int

	// LoadedAt is when this list was installed, zero until a first
	// SetSources. It moves on a reconfiguration that does not restart the
	// process, which is the case nothing outside the process can witness.
	LoadedAt time.Time
}

// SourceListEvidence is one internally consistent account of a runner: the
// identity of the source list in effect, and what the last pass made of each
// of its members.
//
// The two travel together because a reader's whole question is whether *this*
// configuration took. Two separate reads could straddle a reconfiguration and
// answer it about two different configurations, which would break the exact
// source accounting a caller uses to detect a partial answer.
type SourceListEvidence struct {
	SourceList SourceListIdentity
	Sources    []SourceEvidence
}

// SourceListDigest is the stable identity of a configured source list.
//
// The construction is fixed because it is a cross-process agreement: anything
// checking that a process loaded a particular configuration computes the same
// digest from its own copy of the list and compares. It is SHA-256 over the
// exact configured URLs, in configured order, joined with a single NUL byte —
// a byte no valid URL contains, so no two distinct lists can produce the same
// input, which a plain concatenation would allow.
//
// Order is part of the identity rather than normalized away, because order is
// meaningful here: it is the tiebreak that decides which of two sources
// claiming one cassette name wins, so a reordered list is a different
// configuration even when its members are identical.
//
// The URLs are hashed exactly as configured, credentials included. A digest
// discloses nothing, and redacting first would make two deployments differing
// only in a credential indistinguishable.
func SourceListDigest(sources []string) string {
	sum := sha256.Sum256([]byte(strings.Join(sources, "\x00")))

	return "sha256:" + hex.EncodeToString(sum[:])
}

// EvidenceReporter is a spec cache that can account for its configured
// sources.
//
// It is separate from SpecCache because the two answer different questions:
// SpecCache is the documents core publishes, and this is the record of how
// they got there. A cache that only states documents — a handler test's stub —
// honestly reports no admission evidence rather than being forced to invent
// some.
type EvidenceReporter interface {
	EvidenceSnapshot() SourceListEvidence
}

// compile-time proof that a Runner can account for its sources.
var _ EvidenceReporter = (*Runner)(nil)
