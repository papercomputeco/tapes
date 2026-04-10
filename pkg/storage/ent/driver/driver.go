// Package entdriver
package entdriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
)

// Dialect constants matching the values from entgo's dialect package.
const (
	dialectSQLite   = "sqlite3"
	dialectPostgres = "postgres"
)

// EntDriver provides storage operations using an ent client.
// It is database-agnostic and can be embedded by specific drivers.
//
// DB is the underlying *sql.DB held by the wrapping driver (sqlite,
// postgres). It is optional — when populated together with Dialect,
// the perf-critical bulk paths (currently AncestryChains) drop down to
// raw SQL via a recursive CTE, which is roughly 20× faster than ent's
// per-row ORM scaffolding at the cost of being database-specific. When
// DB is nil those paths fall back to the ent-only implementation.
//
// Dialect must be one of "sqlite3" or "postgres" — the values from
// entgo's dialect package — and is used to pick the right placeholder
// syntax for the raw query.
type EntDriver struct {
	Client  *ent.Client
	DB      *sql.DB
	Dialect string
}

// Put stores a node. Returns true if the node was newly inserted,
// false if it already existed. This is a no-op due to content-addressing.
func (ed *EntDriver) Put(ctx context.Context, n *merkle.Node) (bool, error) {
	if n == nil {
		return false, errors.New("cannot store nil node")
	}

	// Check if node already exists (idempotent insert)
	exists, err := ed.Client.Node.Query().
		Where(node.ID(n.Hash)).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}
	if exists {
		return false, nil
	}

	create := ed.Client.Node.Create().
		SetID(n.Hash).
		SetNillableParentHash(n.ParentHash).
		SetType(n.Bucket.Type).
		SetRole(n.Bucket.Role).
		SetModel(n.Bucket.Model).
		SetProvider(n.Bucket.Provider).
		SetStopReason(n.StopReason)

	if n.Project != "" {
		create.SetProject(n.Project)
	}

	if n.Bucket.AgentName != "" {
		create.SetAgentName(n.Bucket.AgentName)
	}

	// Honor an explicit CreatedAt when supplied (e.g. by tests). When zero,
	// the schema default (CURRENT_TIMESTAMP) applies.
	if !n.CreatedAt.IsZero() {
		create.SetCreatedAt(n.CreatedAt)
	}

	// Marshal bucket to JSON for storage
	bucketJSON, err := json.Marshal(n.Bucket)
	if err != nil {
		return false, fmt.Errorf("failed to marshal bucket: %w", err)
	}
	var bucketMap map[string]any
	if err := json.Unmarshal(bucketJSON, &bucketMap); err != nil {
		return false, fmt.Errorf("failed to unmarshal bucket to map: %w", err)
	}
	create.SetBucket(bucketMap)

	// Marshal content blocks
	contentJSON, err := json.Marshal(n.Bucket.Content)
	if err != nil {
		return false, fmt.Errorf("failed to marshal content: %w", err)
	}
	var contentSlice []map[string]any
	if err := json.Unmarshal(contentJSON, &contentSlice); err != nil {
		return false, fmt.Errorf("failed to unmarshal content to slice: %w", err)
	}
	create.SetContent(contentSlice)

	// Set usage fields if available
	if n.Usage != nil {
		if n.Usage.PromptTokens > 0 {
			create.SetPromptTokens(n.Usage.PromptTokens)
		}
		if n.Usage.CompletionTokens > 0 {
			create.SetCompletionTokens(n.Usage.CompletionTokens)
		}
		if n.Usage.TotalTokens > 0 {
			create.SetTotalTokens(n.Usage.TotalTokens)
		}
		if n.Usage.CacheCreationInputTokens > 0 {
			create.SetCacheCreationInputTokens(n.Usage.CacheCreationInputTokens)
		}
		if n.Usage.CacheReadInputTokens > 0 {
			create.SetCacheReadInputTokens(n.Usage.CacheReadInputTokens)
		}
		if n.Usage.TotalDurationNs > 0 {
			create.SetTotalDurationNs(n.Usage.TotalDurationNs)
		}
		if n.Usage.PromptDurationNs > 0 {
			create.SetPromptDurationNs(n.Usage.PromptDurationNs)
		}
	}

	err = create.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("could not execute node creation: %w", err)
	}

	return true, nil
}

// Get retrieves a node by its hash.
func (ed *EntDriver) Get(ctx context.Context, hash string) (*merkle.Node, error) {
	entNode, err := ed.Client.Node.Get(ctx, hash)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, storage.NotFoundError{Hash: hash}
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	return ed.entNodeToMerkleNode(entNode)
}

// Has checks if a node exists by its hash.
func (ed *EntDriver) Has(ctx context.Context, hash string) (bool, error) {
	return ed.Client.Node.Query().
		Where(node.ID(hash)).
		Exist(ctx)
}

// GetByParent retrieves all nodes that have the given parent hash.
// Uses the children edge for efficient lookups.
func (ed *EntDriver) GetByParent(ctx context.Context, parentHash *string) ([]*merkle.Node, error) {
	var entNodes []*ent.Node
	var err error

	if parentHash == nil {
		// Root nodes have no parent
		entNodes, err = ed.Client.Node.Query().
			Where(node.ParentHashIsNil()).
			All(ctx)
	} else {
		// Use the edge to find children
		entNodes, err = ed.Client.Node.Query().
			Where(node.ID(*parentHash)).
			QueryChildren().
			All(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// List returns all nodes in the store.
func (ed *EntDriver) List(ctx context.Context) ([]*merkle.Node, error) {
	entNodes, err := ed.Client.Node.Query().
		Order(ent.Asc(node.FieldCreatedAt)).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// Roots returns all root nodes (nodes with no parent).
func (ed *EntDriver) Roots(ctx context.Context) ([]*merkle.Node, error) {
	return ed.GetByParent(ctx, nil)
}

// Leaves returns all leaf nodes (nodes with no children).
// Uses the children edge for efficient detection.
func (ed *EntDriver) Leaves(ctx context.Context) ([]*merkle.Node, error) {
	entNodes, err := ed.Client.Node.Query().
		Where(node.Not(node.HasChildren())).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query leaves: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// Ancestry returns the path from a node back to its root (node first, root last).
// Uses the parent edge for traversal. See AncestryChain for a variant that
// also signals when the walk stopped at a missing parent.
func (ed *EntDriver) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
	chain, err := ed.AncestryChain(ctx, hash)
	if err != nil {
		return nil, err
	}
	return chain.Nodes, nil
}

// AncestryChain walks the parent chain starting at hash and returns a Chain
// describing whether the walk reached a real root or stopped at a parent
// that is not present in this store. A missing parent is treated as an
// expected edge case (e.g. trimmed history, foreign chain, offloaded data)
// and surfaced via Chain.Incomplete / Chain.MissingParent rather than as an
// error.
func (ed *EntDriver) AncestryChain(ctx context.Context, hash string) (*storage.Chain, error) {
	var path []*merkle.Node

	current, err := ed.Client.Node.Get(ctx, hash)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, storage.NotFoundError{Hash: hash}
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	// seen guards the walk against a corrupt parent edge that would
	// otherwise loop forever. The cost is one map insert + one lookup
	// per node, on top of a slice we're already building — trivial next
	// to the per-step SQL query.
	seen := make(map[string]struct{})
	chain := &storage.Chain{}
	for current != nil {
		if _, loop := seen[current.ID]; loop {
			chain.Incomplete = true
			chain.CycleDetected = true
			break
		}
		seen[current.ID] = struct{}{}

		n, err := ed.entNodeToMerkleNode(current)
		if err != nil {
			return nil, err
		}
		path = append(path, n)

		parent, err := current.QueryParent().Only(ctx)
		if ent.IsNotFound(err) {
			// Distinguish a real root (no parent_hash) from a dangling
			// pointer (parent_hash set but referenced node missing).
			if current.ParentHash != nil && *current.ParentHash != "" {
				chain.Incomplete = true
				chain.MissingParent = *current.ParentHash
			}
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query parent: %w", err)
		}
		current = parent
	}

	chain.Nodes = path
	return chain, nil
}

// ancestryBatchChunk caps the IN-list size for each batched parent lookup.
// SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 32766 since 3.32.0, so a
// chunk of 500 is comfortably under the limit and keeps each query small
// enough that a single slow iteration doesn't dominate a page.
const ancestryBatchChunk = 500

// maxAncestryDepth is a safety rail on ancestry walks. Real sessions are
// at most a few thousand turns; anything deeper is almost certainly a
// cycle or a misconfigured import, and we'd rather return partial chains
// than loop until OOM. Both the BFS fallback and the CTE templates use
// this value so behavior is consistent regardless of the backing store.
const maxAncestryDepth = 5_000

// AncestryChains walks the ancestry of each input hash and returns a
// Chain per starting hash. When the underlying *sql.DB is available
// the fast path issues a single recursive CTE query that walks every
// chain in one round trip; otherwise it falls back to a batched BFS
// that issues one query per BFS depth level.
//
// Both SQLite and Postgres have dialect-specific CTE templates — see
// cteQuerySQLite and cteQueryPostgres — that differ only in the
// label_hint extraction (json_each/json_extract vs
// jsonb_array_elements/->>) and bind-parameter syntax (?  vs $N).
//
// This is the hot path behind /v1/sessions/summary. The naive loop of
// calling AncestryChain per leaf issues one SQL query per parent edge
// per leaf; on a real store with tens of thousands of leaves, that never
// completes. The CTE path collapses the walk to a single round trip and
// is roughly 20× faster than the BFS fallback on the same data.
func (ed *EntDriver) AncestryChains(ctx context.Context, hashes []string) (map[string]*storage.Chain, error) {
	if len(hashes) == 0 {
		return map[string]*storage.Chain{}, nil
	}
	if ed.DB != nil && (ed.Dialect == dialectSQLite || ed.Dialect == dialectPostgres) {
		return ed.ancestryChainsCTE(ctx, hashes)
	}
	return ed.ancestryChainsBFS(ctx, hashes)
}

// ancestryChainsBFS is the original BFS implementation, kept as a fallback
// for ent clients constructed without an underlying *sql.DB (e.g., a
// hypothetical future driver that doesn't expose one). It batches at most
// O(depth) queries, which is good enough to avoid the N×D blowup but
// loses the round-trip-collapse win of the CTE path.
func (ed *EntDriver) ancestryChainsBFS(ctx context.Context, hashes []string) (map[string]*storage.Chain, error) {
	// Dedupe input hashes: multiple starts pointing at the same node
	// would otherwise produce duplicate chains.
	uniqueStarts := dedupeHashes(hashes)

	// Bootstrap: fetch the starting nodes themselves.
	startNodes, err := ed.getNodesByHashes(ctx, uniqueStarts)
	if err != nil {
		return nil, fmt.Errorf("fetch starting nodes: %w", err)
	}

	chains := make(map[string]*storage.Chain, len(startNodes))
	// pending[leafHash] = the next parent_hash this leaf is waiting on.
	pending := make(map[string]string, len(startNodes))
	// perLeafSeen guards against a cycle in a single chain looping forever.
	perLeafSeen := make(map[string]map[string]struct{}, len(startNodes))

	for hash, n := range startNodes {
		chains[hash] = &storage.Chain{Nodes: []*merkle.Node{n}}
		perLeafSeen[hash] = map[string]struct{}{hash: {}}
		if n.ParentHash != nil && *n.ParentHash != "" {
			pending[hash] = *n.ParentHash
		}
	}

	// BFS one depth level at a time. Each iteration pulls every unique
	// pending parent_hash in a single batched query.
	for depth := 0; len(pending) > 0 && depth < maxAncestryDepth; depth++ {
		frontier := dedupePendingTargets(pending)
		parents, err := ed.getNodesByHashes(ctx, frontier)
		if err != nil {
			return nil, fmt.Errorf("fetch parents at depth %d: %w", depth, err)
		}

		nextPending := make(map[string]string, len(pending))
		for leafHash, parentHash := range pending {
			parentNode, ok := parents[parentHash]
			if !ok {
				// Target missing → dangling pointer. Record on the
				// chain and stop walking this leaf.
				chains[leafHash].Incomplete = true
				chains[leafHash].MissingParent = parentHash
				continue
			}
			if _, seen := perLeafSeen[leafHash][parentHash]; seen {
				// Cycle in the parent chain — stop walking this leaf
				// and surface the state. The chain up to this point is
				// the largest acyclic prefix we can reach from the leaf.
				chains[leafHash].Incomplete = true
				chains[leafHash].CycleDetected = true
				continue
			}
			perLeafSeen[leafHash][parentHash] = struct{}{}
			chains[leafHash].Nodes = append(chains[leafHash].Nodes, parentNode)
			if parentNode.ParentHash != nil && *parentNode.ParentHash != "" {
				nextPending[leafHash] = *parentNode.ParentHash
			}
		}
		pending = nextPending
	}

	// Anything still pending after the depth cap gets marked Incomplete
	// at the hash we would have chased next. This is the same shape as a
	// dangling-parent outcome; the cap is the only remaining way the walk
	// can terminate without reaching a real root.
	for leafHash, parentHash := range pending {
		chains[leafHash].Incomplete = true
		chains[leafHash].MissingParent = parentHash
	}

	return chains, nil
}

// getNodesByHashes fetches the given hashes in chunks of ancestryBatchChunk
// and returns a map keyed by hash. Hashes not present in the store are
// simply absent from the map; callers distinguish present/absent by map
// membership.
func (ed *EntDriver) getNodesByHashes(ctx context.Context, hashes []string) (map[string]*merkle.Node, error) {
	out := make(map[string]*merkle.Node, len(hashes))
	for start := 0; start < len(hashes); start += ancestryBatchChunk {
		end := min(start+ancestryBatchChunk, len(hashes))
		chunk := hashes[start:end]
		entNodes, err := ed.Client.Node.Query().
			Where(node.IDIn(chunk...)).
			All(ctx)
		if err != nil {
			return nil, err
		}
		for _, en := range entNodes {
			n, err := ed.entNodeToMerkleNode(en)
			if err != nil {
				return nil, err
			}
			out[en.ID] = n
		}
	}
	return out, nil
}

func dedupeHashes(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, h := range in {
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	return out
}

func dedupePendingTargets(pending map[string]string) []string {
	seen := make(map[string]struct{}, len(pending))
	out := make([]string, 0, len(pending))
	for _, target := range pending {
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}

// cteQuerySQLite is the recursive CTE template for SQLite. It uses
// json_each / json_extract / group_concat for the label_hint extraction
// and `?` bind parameters.
//
//	%[1]s — the node-hash projection (used twice for start_hash + hash)
//	%[2]s — the comma-separated bind-placeholder list
//	%[3]d — maxAncestryDepth (recursion depth cap)
const cteQuerySQLite = `
WITH RECURSIVE walk(
    start_hash, hash, parent_hash, depth,
    type, role, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at,
    label_hint
) AS (
    SELECT
        %[1]s, %[1]s, n.parent_hash, 0,
        n.type, n.role, n.model, n.provider, n.agent_name, n.stop_reason,
        n.prompt_tokens, n.completion_tokens, n.total_tokens,
        n.cache_creation_input_tokens, n.cache_read_input_tokens,
        n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at,
        CASE WHEN n.role = 'user' THEN
            (SELECT group_concat(json_extract(je.value, '$.text'), char(10))
             FROM json_each(n.content) je
             WHERE json_extract(je.value, '$.type') = 'text'
               AND json_extract(je.value, '$.text') IS NOT NULL)
        ELSE NULL END
    FROM nodes n
    WHERE n.hash IN (%[2]s)

    UNION ALL

    SELECT
        w.start_hash, n.hash, n.parent_hash, w.depth + 1,
        n.type, n.role, n.model, n.provider, n.agent_name, n.stop_reason,
        n.prompt_tokens, n.completion_tokens, n.total_tokens,
        n.cache_creation_input_tokens, n.cache_read_input_tokens,
        n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at,
        CASE WHEN n.role = 'user' THEN
            (SELECT group_concat(json_extract(je.value, '$.text'), char(10))
             FROM json_each(n.content) je
             WHERE json_extract(je.value, '$.type') = 'text'
               AND json_extract(je.value, '$.text') IS NOT NULL)
        ELSE NULL END
    FROM nodes n
    JOIN walk w ON n.hash = w.parent_hash
    WHERE w.depth < %[3]d
)
SELECT
    start_hash, hash, parent_hash, depth,
    type, role, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at,
    label_hint
FROM walk
ORDER BY start_hash, depth
`

// cteQueryPostgres is the recursive CTE template for PostgreSQL. The
// content column is JSONB, so label_hint extraction uses
// jsonb_array_elements / ->> / string_agg instead of SQLite's json_each
// / json_extract / group_concat. Bind parameters use $N syntax.
//
//	%[1]s — the node-hash projection (used twice for start_hash + hash)
//	%[2]s — the comma-separated bind-placeholder list
//	%[3]d — maxAncestryDepth (recursion depth cap)
const cteQueryPostgres = `
WITH RECURSIVE walk(
    start_hash, hash, parent_hash, depth,
    type, role, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at,
    label_hint
) AS (
    SELECT
        %[1]s, %[1]s, n.parent_hash, 0,
        n.type, n.role, n.model, n.provider, n.agent_name, n.stop_reason,
        n.prompt_tokens, n.completion_tokens, n.total_tokens,
        n.cache_creation_input_tokens, n.cache_read_input_tokens,
        n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at,
        CASE WHEN n.role = 'user' THEN
            (SELECT string_agg(je->>'text', E'\n')
             FROM jsonb_array_elements(n.content) je
             WHERE je->>'type' = 'text'
               AND je->>'text' IS NOT NULL)
        ELSE NULL END
    FROM nodes n
    WHERE n.hash IN (%[2]s)

    UNION ALL

    SELECT
        w.start_hash, n.hash, n.parent_hash, w.depth + 1,
        n.type, n.role, n.model, n.provider, n.agent_name, n.stop_reason,
        n.prompt_tokens, n.completion_tokens, n.total_tokens,
        n.cache_creation_input_tokens, n.cache_read_input_tokens,
        n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at,
        CASE WHEN n.role = 'user' THEN
            (SELECT string_agg(je->>'text', E'\n')
             FROM jsonb_array_elements(n.content) je
             WHERE je->>'type' = 'text'
               AND je->>'text' IS NOT NULL)
        ELSE NULL END
    FROM nodes n
    JOIN walk w ON n.hash = w.parent_hash
    WHERE w.depth < %[3]d
)
SELECT
    start_hash, hash, parent_hash, depth,
    type, role, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at,
    label_hint
FROM walk
ORDER BY start_hash, depth
`

// ancestryChainsCTE walks every input chain in a single recursive CTE
// query and assembles per-leaf Chain values from the streamed result rows.
// Compared to the BFS path it eliminates ~50 SQL round trips per page on
// realistic data (one per BFS depth level), which is the dominant cost
// at the page sizes deck uses.
//
// The CTE projects every column merkle.Node needs in one shot EXCEPT
// the content blob — that's the heavy field, ~1.4KB average on real
// data. Instead we extract `label_hint`, the first text block of any
// user-role node, server-side via dialect-specific JSON functions. The
// label-building code on the Go side reads from a synthetic content
// block built from this hint, recovering correct labels without paying
// for the full content payload.
//
// The trade-off: tool_calls / has_tool_error / has_git_activity all
// derive from content too, and they end up zero/false on the fast
// path. None of those are surfaced in the deck overview list — they
// only appear in the per-session detail view, which still goes
// through AncestryChain (with full content) and is unaffected.
//
// Per-row depth caps recursion in SQL as a backstop against cycles;
// a per-chain seen-set in Go is the primary guard.
//
// ORDER BY (start_hash, depth) lets us stream rows in chain order:
// each chain's nodes arrive consecutively from depth 0 outward.
//
// Cycles are guarded both in SQL (the recursive step caps depth) and in
// Go (a per-chain seen-set). Dangling parents are detected after the
// stream completes by checking each chain's tail for a non-null
// parent_hash with no resolved successor.
func (ed *EntDriver) ancestryChainsCTE(ctx context.Context, hashes []string) (map[string]*storage.Chain, error) {
	uniqueStarts := dedupeHashes(hashes)

	placeholders := make([]string, len(uniqueStarts))
	args := make([]any, len(uniqueStarts))
	for i, h := range uniqueStarts {
		switch ed.Dialect {
		case dialectPostgres:
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		default:
			placeholders[i] = "?"
		}
		args[i] = h
	}
	inList := strings.Join(placeholders, ",")

	var tmpl string
	switch ed.Dialect {
	case dialectPostgres:
		tmpl = cteQueryPostgres
	default:
		tmpl = cteQuerySQLite
	}

	// #nosec G202 -- inList is composed of generated placeholders only
	// (`?` or `$N`); the leaf hashes themselves go in via QueryContext
	// args, never interpolated into the SQL string.
	//
	// We use fmt.Sprintf to weave the placeholder list into the query
	// AND to keep the two `n.hash` projections from sitting next to
	// each other in any single string literal — golangci-lint's
	// `dupword` linter has auto-fix on and silently deletes one of
	// them when they appear consecutively. Same with the trailing
	// `(` after `IN`, which whitespace cleanup eats. The placeholders
	// route around both.
	//
	//   %[1]s — the node-hash projection (used twice for start_hash + hash)
	//   %[2]s — the comma-separated bind-placeholder list
	//   %[3]d — maxAncestryDepth (recursion depth cap)
	query := fmt.Sprintf(tmpl, "n.hash", inList, maxAncestryDepth)

	rows, err := ed.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ancestry CTE query: %w", err)
	}
	defer rows.Close()

	chains := make(map[string]*storage.Chain, len(uniqueStarts))
	// perChainSeen guards against a corrupt parent edge spinning a chain.
	// We allocate it lazily so a clean store with thousands of leaves
	// doesn't pay for thousands of empty maps it never uses.
	perChainSeen := make(map[string]map[string]struct{}, len(uniqueStarts))
	// dead marks chains we've stopped accumulating into (because of a
	// cycle hit). Further rows for those start_hashes are skipped.
	dead := make(map[string]struct{})
	// expectedParent tracks the parent_hash we'd need to see next for
	// each chain. After streaming, any entry remaining here is a
	// dangling parent — the CTE walk couldn't continue because the
	// referenced node is missing.
	expectedParent := make(map[string]string, len(uniqueStarts))

	for rows.Next() {
		var (
			startHash        string
			hash             string
			parentHash       sql.NullString
			depth            int
			ntype            sql.NullString
			role             sql.NullString
			model            sql.NullString
			provider         sql.NullString
			agentName        sql.NullString
			stopReason       sql.NullString
			promptTokens     sql.NullInt64
			completionTokens sql.NullInt64
			totalTokens      sql.NullInt64
			cacheCreate      sql.NullInt64
			cacheRead        sql.NullInt64
			totalDur         sql.NullInt64
			promptDur        sql.NullInt64
			project          sql.NullString
			createdAt        time.Time
			labelHint        sql.NullString
		)
		if err := rows.Scan(
			&startHash, &hash, &parentHash, &depth,
			&ntype, &role, &model, &provider, &agentName, &stopReason,
			&promptTokens, &completionTokens, &totalTokens,
			&cacheCreate, &cacheRead,
			&totalDur, &promptDur, &project, &createdAt,
			&labelHint,
		); err != nil {
			return nil, fmt.Errorf("scan ancestry row: %w", err)
		}

		if _, isDead := dead[startHash]; isDead {
			continue
		}

		seen, ok := perChainSeen[startHash]
		if !ok {
			seen = make(map[string]struct{}, 32)
			perChainSeen[startHash] = seen
		}
		if _, loop := seen[hash]; loop {
			// Cycle in this chain — stop appending and mark.
			if c, ok := chains[startHash]; ok {
				c.Incomplete = true
				c.CycleDetected = true
			}
			dead[startHash] = struct{}{}
			delete(expectedParent, startHash)
			continue
		}
		seen[hash] = struct{}{}

		n := &merkle.Node{
			Hash:       hash,
			CreatedAt:  createdAt,
			StopReason: stopReason.String,
		}
		if parentHash.Valid && parentHash.String != "" {
			ph := parentHash.String
			n.ParentHash = &ph
		}
		if project.Valid {
			n.Project = project.String
		}
		n.Bucket = merkle.Bucket{
			Type:      ntype.String,
			Role:      role.String,
			Model:     model.String,
			Provider:  provider.String,
			AgentName: agentName.String,
		}
		// We deliberately do NOT ship the content blob on this path —
		// it's the dominant cost. For label building, sessions.BuildLabel
		// only needs the first text block of each user-role node, which
		// the CTE extracted server-side via json_extract. We synthesize a
		// minimal content slice from the hint so BuildLabel works
		// unchanged. tool_calls / has_tool_error / has_git_activity all
		// stay zero/false on this path; they're not surfaced in the
		// overview list anyway.
		if labelHint.Valid && labelHint.String != "" {
			n.Bucket.Content = []llm.ContentBlock{{Type: "text", Text: labelHint.String}}
		}
		if promptTokens.Valid || completionTokens.Valid || totalTokens.Valid ||
			cacheCreate.Valid || cacheRead.Valid || totalDur.Valid || promptDur.Valid {
			n.Usage = &llm.Usage{}
			if promptTokens.Valid {
				n.Usage.PromptTokens = int(promptTokens.Int64)
			}
			if completionTokens.Valid {
				n.Usage.CompletionTokens = int(completionTokens.Int64)
			}
			if totalTokens.Valid {
				n.Usage.TotalTokens = int(totalTokens.Int64)
			}
			if cacheCreate.Valid {
				n.Usage.CacheCreationInputTokens = int(cacheCreate.Int64)
			}
			if cacheRead.Valid {
				n.Usage.CacheReadInputTokens = int(cacheRead.Int64)
			}
			if totalDur.Valid {
				n.Usage.TotalDurationNs = totalDur.Int64
			}
			if promptDur.Valid {
				n.Usage.PromptDurationNs = promptDur.Int64
			}
		}

		chain, ok := chains[startHash]
		if !ok {
			chain = &storage.Chain{}
			chains[startHash] = chain
		}
		chain.Nodes = append(chain.Nodes, n)

		// Track the parent we'd need to see next; clear it on a real root.
		if parentHash.Valid && parentHash.String != "" {
			expectedParent[startHash] = parentHash.String
		} else {
			delete(expectedParent, startHash)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ancestry CTE rows: %w", err)
	}

	// Anything still in expectedParent after the stream finished means
	// the walk reached a node whose parent_hash was set but the
	// referenced node was missing from the store.
	for startHash, missing := range expectedParent {
		if _, isDead := dead[startHash]; isDead {
			continue
		}
		if c, ok := chains[startHash]; ok {
			c.Incomplete = true
			c.MissingParent = missing
		}
	}

	return chains, nil
}

// Depth returns the depth of a node (0 for roots).
func (ed *EntDriver) Depth(ctx context.Context, hash string) (int, error) {
	path, err := ed.Ancestry(ctx, hash)
	if err != nil {
		return 0, err
	}
	return len(path) - 1, nil
}

// UpdateUsage updates only the token usage fields on an existing node by hash.
func (ed *EntDriver) UpdateUsage(ctx context.Context, hash string, usage *llm.Usage) error {
	if usage == nil {
		return errors.New("cannot update with nil usage")
	}

	update := ed.Client.Node.UpdateOneID(hash)

	if usage.PromptTokens > 0 {
		update.SetPromptTokens(usage.PromptTokens)
	}
	if usage.CompletionTokens > 0 {
		update.SetCompletionTokens(usage.CompletionTokens)
	}
	if usage.TotalTokens > 0 {
		update.SetTotalTokens(usage.TotalTokens)
	}
	if usage.CacheCreationInputTokens > 0 {
		update.SetCacheCreationInputTokens(usage.CacheCreationInputTokens)
	}
	if usage.CacheReadInputTokens > 0 {
		update.SetCacheReadInputTokens(usage.CacheReadInputTokens)
	}

	return update.Exec(ctx)
}

// Close closes the database connection.
func (ed *EntDriver) Close() error {
	return ed.Client.Close()
}

// Conversion helpers
func (ed *EntDriver) entNodeToMerkleNode(entNode *ent.Node) (*merkle.Node, error) {
	// Build the bucket from the flat columns instead of round-tripping
	// the redundant bucket JSON field. Put() writes both copies, so the
	// flat columns always carry the same information; reading from them
	// avoids two JSON ops per node (one marshal, one unmarshal). The
	// bucket column stays in the schema for content-addressing audit
	// purposes but is no longer touched on the read path.
	bucket := merkle.Bucket{
		Type:      entNode.Type,
		Role:      entNode.Role,
		Model:     entNode.Model,
		Provider:  entNode.Provider,
		AgentName: entNode.AgentName,
	}

	// Content is stored as []map[string]any by ent and needs one round-
	// trip to land in the typed []llm.ContentBlock shape. Skip the dance
	// for empty rows so the cheap-message common case never allocates.
	if len(entNode.Content) > 0 {
		contentJSON, err := json.Marshal(entNode.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal content: %w", err)
		}
		if err := json.Unmarshal(contentJSON, &bucket.Content); err != nil {
			return nil, fmt.Errorf("failed to unmarshal content: %w", err)
		}
	}

	node := &merkle.Node{
		Hash:       entNode.ID,
		ParentHash: entNode.ParentHash,
		Bucket:     bucket,
		StopReason: entNode.StopReason,
		CreatedAt:  entNode.CreatedAt,
	}

	if entNode.Project != nil {
		node.Project = *entNode.Project
	}

	// Rebuild usage metrics if they exist.
	if entNode.PromptTokens != nil ||
		entNode.CompletionTokens != nil ||
		entNode.TotalTokens != nil ||
		entNode.CacheCreationInputTokens != nil ||
		entNode.CacheReadInputTokens != nil ||
		entNode.TotalDurationNs != nil ||
		entNode.PromptDurationNs != nil {
		node.Usage = &llm.Usage{}

		if entNode.PromptTokens != nil {
			node.Usage.PromptTokens = *entNode.PromptTokens
		}

		if entNode.CompletionTokens != nil {
			node.Usage.CompletionTokens = *entNode.CompletionTokens
		}

		if entNode.TotalTokens != nil {
			node.Usage.TotalTokens = *entNode.TotalTokens
		}

		if entNode.CacheCreationInputTokens != nil {
			node.Usage.CacheCreationInputTokens = *entNode.CacheCreationInputTokens
		}

		if entNode.CacheReadInputTokens != nil {
			node.Usage.CacheReadInputTokens = *entNode.CacheReadInputTokens
		}

		if entNode.TotalDurationNs != nil {
			node.Usage.TotalDurationNs = *entNode.TotalDurationNs
		}

		if entNode.PromptDurationNs != nil {
			node.Usage.PromptDurationNs = *entNode.PromptDurationNs
		}
	}

	return node, nil
}

func (ed *EntDriver) entNodesToMerkleNodes(entNodes []*ent.Node) ([]*merkle.Node, error) {
	nodes := make([]*merkle.Node, 0, len(entNodes))
	for _, entNode := range entNodes {
		n, err := ed.entNodeToMerkleNode(entNode)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}
