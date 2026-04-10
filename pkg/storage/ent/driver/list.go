package entdriver

import (
	"context"
	"fmt"

	"entgo.io/ent/dialect/sql"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
	"github.com/papercomputeco/tapes/pkg/storage/ent/predicate"
)

// ListSessions returns a page of leaf nodes (sessions), ordered by created_at
// descending then hash descending, optionally filtered by opts.
func (ed *EntDriver) ListSessions(ctx context.Context, opts storage.ListOpts) (*storage.Page[*merkle.Node], error) {
	opts = opts.Normalize()

	cursor, err := storage.DecodeCursor(opts.Cursor)
	if err != nil {
		return nil, err
	}

	preds := buildFilterPredicates(opts)
	preds = append(preds, node.Not(node.HasChildren()))
	if opts.Cursor != "" {
		preds = append(preds, keysetBefore(cursor))
	}

	// Fetch limit+1 to know whether another page exists.
	entNodes, err := ed.Client.Node.Query().
		Where(preds...).
		Order(
			ent.Desc(node.FieldCreatedAt),
			ent.Desc(node.FieldID),
		).
		Limit(opts.Limit + 1).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing sessions: %w", err)
	}

	hasMore := len(entNodes) > opts.Limit
	if hasMore {
		entNodes = entNodes[:opts.Limit]
	}

	items, err := ed.entNodesToMerkleNodes(entNodes)
	if err != nil {
		return nil, err
	}

	page := &storage.Page[*merkle.Node]{Items: items}
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		page.NextCursor = storage.Cursor{
			CreatedAt: last.CreatedAt,
			Hash:      last.Hash,
		}.Encode()
	}

	return page, nil
}

// CountSessions returns aggregate counts for the slice of data matching opts.
// Pagination fields on opts are ignored.
func (ed *EntDriver) CountSessions(ctx context.Context, opts storage.ListOpts) (storage.SessionStats, error) {
	filter := buildFilterPredicates(opts)

	sessionCount, err := ed.Client.Node.Query().
		Where(append(filter, node.Not(node.HasChildren()))...).
		Count(ctx)
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("counting sessions: %w", err)
	}

	turnCount, err := ed.Client.Node.Query().
		Where(filter...).
		Count(ctx)
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("counting turns: %w", err)
	}

	rootCount, err := ed.Client.Node.Query().
		Where(append(filter, node.Or(node.ParentHashIsNil(), node.ParentHashEQ("")))...).
		Count(ctx)
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("counting roots: %w", err)
	}

	return storage.SessionStats{
		SessionCount: sessionCount,
		TurnCount:    turnCount,
		RootCount:    rootCount,
	}, nil
}

// buildFilterPredicates translates the per-field filters in opts into ent
// predicates. Empty / nil values are skipped.
func buildFilterPredicates(opts storage.ListOpts) []predicate.Node {
	var preds []predicate.Node
	if opts.Project != "" {
		preds = append(preds, node.ProjectEQ(opts.Project))
	}
	if opts.Agent != "" {
		preds = append(preds, node.AgentNameEQ(opts.Agent))
	}
	if opts.Model != "" {
		preds = append(preds, node.ModelEQ(opts.Model))
	}
	if opts.Provider != "" {
		preds = append(preds, node.ProviderEQ(opts.Provider))
	}
	if opts.Since != nil {
		preds = append(preds, node.CreatedAtGTE(*opts.Since))
	}
	if opts.Until != nil {
		preds = append(preds, node.CreatedAtLT(*opts.Until))
	}
	return preds
}

// ListParentRefs returns the (hash, parent_hash) tuple for every node in the
// store. It projects only the two edge columns so integrity checks over large
// databases don't pay the cost of deserializing every node's bucket JSON.
func (ed *EntDriver) ListParentRefs(ctx context.Context) ([]storage.ParentRef, error) {
	var rows []struct {
		Hash       string  `sql:"hash"`
		ParentHash *string `sql:"parent_hash"`
	}
	if err := ed.Client.Node.Query().
		Select(node.FieldID, node.FieldParentHash).
		Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("failed to scan parent refs: %w", err)
	}
	refs := make([]storage.ParentRef, len(rows))
	for i, r := range rows {
		refs[i] = storage.ParentRef{Hash: r.Hash, ParentHash: r.ParentHash}
	}
	return refs, nil
}

// keysetBefore returns a predicate matching rows that come strictly after the
// cursor in (created_at DESC, hash DESC) order — i.e. rows with an earlier
// created_at, or with the same created_at and a smaller hash.
func keysetBefore(c storage.Cursor) predicate.Node {
	return predicate.Node(func(s *sql.Selector) {
		s.Where(sql.Or(
			sql.LT(s.C(node.FieldCreatedAt), c.CreatedAt),
			sql.And(
				sql.EQ(s.C(node.FieldCreatedAt), c.CreatedAt),
				sql.LT(s.C(node.FieldID), c.Hash),
			),
		))
	})
}
