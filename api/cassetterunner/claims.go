package cassetterunner

import (
	"sort"

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

// manifestClaims lifts a manifest's filter claims into their flattened form.
// The type switch is the version-aware edge, the same one discovery performs:
// a schema that does not declare claims simply holds none.
func manifestClaims(name cassette.Name, declared cassette.Manifest) []ActiveClaim {
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

// ClaimsFor returns the filter-param claims held by admitted cassettes on one
// core surface, in cassette-name order.
//
// The lookup reads only in-memory admitted state under the registry's own
// lock — no I/O of any kind — which is what makes a per-request consult
// affordable and correct at once: claims flip on admit/withdraw with no route
// change, and a request between refreshes sees exactly the admitted set.
func (registry *Registry) ClaimsFor(surface string) []ActiveClaim {
	claims := make([]ActiveClaim, 0)
	for _, instance := range registry.Instances() {
		for _, claim := range manifestClaims(instance.Name, instance.Manifest) {
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
