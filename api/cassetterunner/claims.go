package cassetterunner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// ActiveClaim is one admitted filter-param claim, flattened for the request
// path: everything a handler needs to execute the filter without touching the
// manifest again.
type ActiveClaim struct {
	Cassette      cassette.Name
	Param         string
	Surface       string
	View          string
	PrimitiveType string
	ValueColumn   string
	Normalize     []string
}

// ManifestClaims lifts a manifest's filter claims into their flattened form.
// The type switch is the version-aware edge, the same one discovery performs:
// a schema that does not declare claims simply holds none. It is exported so
// a server that installed an instance directly can name the same claims the
// runner would derive — arming state is keyed off this exact flattening.
func ManifestClaims(name cassette.Name, declared cassette.Manifest) []ActiveClaim {
	versioned, ok := declared.(*v1alpha1.Manifest)
	if !ok || versioned.Publishes == nil {
		return nil
	}
	claims := make([]ActiveClaim, 0, len(versioned.Publishes.Filters))
	for _, filter := range versioned.Publishes.Filters {
		claims = append(claims, ActiveClaim{
			Cassette:      name,
			Param:         filter.Param,
			Surface:       filter.Surface,
			View:          filter.View,
			PrimitiveType: filter.Match.PrimitiveType,
			ValueColumn:   filter.Match.ValueColumn,
			Normalize:     append([]string(nil), filter.Normalize...),
		})
	}

	return claims
}

// claimKey identifies one claim for arming state: the ownership coordinates
// plus every field the probe verified. Normalize verbs are deliberately
// excluded — they change how supplied values fold, not whether the view is
// readable — so a normalization-only manifest edit keeps its armed state.
func claimKey(claim ActiveClaim) string {
	return strings.Join([]string{
		string(claim.Cassette), claim.Surface, claim.Param,
		claim.View, claim.PrimitiveType, claim.ValueColumn,
	}, "\x1f")
}

// claimSubject is the stable, claim-qualified subject an un-armed claim's
// rejection is filed under. Param-scoped rather than view-scoped so the
// operator-facing problem survives a manifest repointing the claim at a new
// view: the question being answered is "why does this param do nothing".
func claimSubject(claim ActiveClaim) string {
	return fmt.Sprintf("cassette %s: claim %q", claim.Cassette, claim.Param)
}

// ClaimsFor returns the armed filter-param claims held by admitted cassettes
// on one core surface, in cassette-name order.
//
// The lookup reads only in-memory state under the registry's own lock — no
// I/O of any kind — which is what makes a per-request consult affordable and
// correct at once: claims flip on admit/withdraw/arm/disarm with no route
// change, and a request between refreshes sees exactly the currently armed
// set. Un-armed claims are withheld, so the request path treats their params
// exactly as it treats unclaimed ones.
func (registry *Registry) ClaimsFor(surface string) []ActiveClaim {
	admitted := registry.admittedClaimsFor(surface)
	claims := admitted[:0]
	registry.mutex.RLock()
	for _, claim := range admitted {
		if _, armed := registry.armed[claimKey(claim)]; armed {
			claims = append(claims, claim)
		}
	}
	registry.mutex.RUnlock()

	return claims
}

// admittedClaimsFor returns every admitted claim on one surface, armed or
// not. Admission's first-claim-wins check consults this rather than
// ClaimsFor because arming gates execution, never ownership: a claim whose
// view is currently unreadable still holds its param.
func (registry *Registry) admittedClaimsFor(surface string) []ActiveClaim {
	claims := make([]ActiveClaim, 0)
	for _, instance := range registry.Instances() {
		for _, claim := range ManifestClaims(instance.Name, instance.Manifest) {
			if claim.Surface == surface {
				claims = append(claims, claim)
			}
		}
	}

	return claims
}

// AdvertisedEntity is one entry in the aggregated entity registry: an entity
// declaration plus where it came from. Cassette is empty for core-native
// entities, because core is not a cassette.
type AdvertisedEntity struct {
	Type        string                    `json:"type"`
	IDKind      string                    `json:"id_kind"`
	DisplayName string                    `json:"display_name,omitempty"`
	Relations   []v1alpha1.EntityRelation `json:"relations,omitempty"`
	Cassette    string                    `json:"cassette,omitempty"`
}

// CoreEntities are the primitives core itself offers, declared in the same
// shape cassettes use so the aggregated registry speaks one vocabulary — a
// consumer that annotates or links entities need not treat core's specially.
var CoreEntities = []AdvertisedEntity{
	{Type: "session", IDKind: "uuid", DisplayName: "Session"},
	// Traces and spans are the derived read model's primitives. Their ids
	// are deterministic content-derived strings (stable across re-derive),
	// so an annotating consumer can key rows off them; a span's canonical
	// external id is "<trace_id>~<span_id>" since span identity is
	// composite within a trace. The separator is "~" deliberately: both
	// components embed provider-verbatim ids full of "_"/"-", and "~" is
	// RFC 3986 unreserved — no encoder produces an escaped form for a
	// path normalizer to rewrite (an escaped "/" does not survive
	// gateway escaped-slash handling). The id is primarily an OPAQUE
	// round-trip handle: attachment-style consumers must store and
	// replay it whole, never reconstruct it. Splitting (on the FIRST
	// "~") is a display/navigation convenience only — provider-verbatim
	// components are not formally barred from containing "~", so the
	// split is best-effort by declared contract, not a parsing
	// guarantee.
	{Type: "trace", IDKind: "string", DisplayName: "Trace"},
	{Type: "span", IDKind: "string", DisplayName: "Span"},
}

// Entities returns the current aggregated entity set: the core-native
// declarations plus every admitted cassette's, in cassette-name order.
//
// The set is derived from admitted manifests on read, so it follows the
// registry with no bookkeeping to invalidate: withdrawal drops a cassette's
// entities and a re-admission with a changed manifest replaces them, both as
// a consequence of the instance itself changing.
func (registry *Registry) Entities() []AdvertisedEntity {
	entities := append([]AdvertisedEntity(nil), CoreEntities...)
	for _, instance := range registry.Instances() {
		versioned, ok := instance.Manifest.(*v1alpha1.Manifest)
		if !ok {
			continue
		}
		for _, entity := range versioned.Entities {
			entities = append(entities, AdvertisedEntity{
				Type:        entity.Type,
				IDKind:      entity.IDKind,
				DisplayName: entity.DisplayName,
				Relations:   append([]v1alpha1.EntityRelation(nil), entity.Relations...),
				Cassette:    string(instance.Name),
			})
		}
	}

	return entities
}

// ReservedParamsFromParser derives the core-owned query params for each
// claimable surface from the parser core's own routes registered into.
//
// Deriving from the live route table rather than a hand-maintained list is
// the point: the reserved set tracks core's actual contract, so a param core
// documents tomorrow is reserved tomorrow with no second list to forget.
// surfaces maps a claimable surface name to the OpenAPI path anchoring it.
func ReservedParamsFromParser(parser *tapesoapi.Parser, surfaces map[string]string) func(surface string) []string {
	return func(surface string) []string {
		path, ok := surfaces[surface]
		if !ok || parser == nil {
			return nil
		}
		seen := map[string]struct{}{}
		params := make([]string, 0)
		collect := func(list []*tapesoapi.Parameter) {
			for _, parameter := range list {
				if parameter == nil || parameter.In != tapesoapi.InQuery {
					continue
				}
				if _, duplicate := seen[parameter.Name]; duplicate {
					continue
				}
				seen[parameter.Name] = struct{}{}
				params = append(params, parameter.Name)
			}
		}
		for _, fragment := range parser.Fragments() {
			item := fragment.Paths[path]
			if item == nil {
				continue
			}
			collect(item.Parameters)
			for _, operation := range item.Operations {
				if operation != nil {
					collect(operation.Parameters)
				}
			}
		}
		sort.Strings(params)

		return params
	}
}
