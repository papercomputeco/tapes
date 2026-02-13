package deck

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/facet"
)

// EntFacetStore implements FacetStore using ent.
type EntFacetStore struct {
	client *ent.Client
}

// NewEntFacetStore creates a new EntFacetStore.
func NewEntFacetStore(client *ent.Client) *EntFacetStore {
	return &EntFacetStore{client: client}
}

func (s *EntFacetStore) SaveFacet(ctx context.Context, f *SessionFacet) error {
	data, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal facet: %w", err)
	}

	var facetMap map[string]any
	if err := json.Unmarshal(data, &facetMap); err != nil {
		return fmt.Errorf("unmarshal facet map: %w", err)
	}

	// Delete existing facet for this session if present.
	_, _ = s.client.Facet.Delete().
		Where(facet.SessionID(f.SessionID)).
		Exec(ctx)

	_, err = s.client.Facet.Create().
		SetID(f.SessionID).
		SetSessionID(f.SessionID).
		SetFacets(facetMap).
		SetCreatedAt(time.Now()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("save facet: %w", err)
	}

	return nil
}

func (s *EntFacetStore) GetFacet(ctx context.Context, sessionID string) (*SessionFacet, error) {
	f, err := s.client.Facet.Query().
		Where(facet.SessionID(sessionID)).
		Only(ctx)
	if err != nil {
		return nil, fmt.Errorf("get facet: %w", err)
	}

	return entFacetToSessionFacet(f)
}

func (s *EntFacetStore) ListFacets(ctx context.Context) ([]*SessionFacet, error) {
	facets, err := s.client.Facet.Query().All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list facets: %w", err)
	}

	result := make([]*SessionFacet, 0, len(facets))
	for _, f := range facets {
		sf, err := entFacetToSessionFacet(f)
		if err != nil {
			continue
		}
		result = append(result, sf)
	}

	return result, nil
}

func entFacetToSessionFacet(f *ent.Facet) (*SessionFacet, error) {
	data, err := json.Marshal(f.Facets)
	if err != nil {
		return nil, fmt.Errorf("marshal facets field: %w", err)
	}

	var sf SessionFacet
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("unmarshal session facet: %w", err)
	}

	sf.SessionID = f.SessionID
	return &sf, nil
}
