// Package postgres
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	// Whole sale bring in the golang-migrate pgx/v5 libraries.
	// These are of the form: pgx5://user:password@host:port/dbname?query
	// See: https://github.com/golang-migrate/migrate/tree/master/database/pgx/v5
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	tapesmigrations "github.com/papercomputeco/tapes/migrations"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

const (
	maxAncestryDepth = 5000
)

type Driver struct {
	dsn  string
	conn *pgxpool.Pool
	q    *gensqlc.Queries
}

func NewDriver(ctx context.Context, connStr string) (*Driver, error) {
	d := &Driver{dsn: connStr}
	if err := d.Open(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if err := migrateUp(dsn); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func (d *Driver) Open(ctx context.Context) error {
	if d == nil {
		return errors.New("nil postgres driver")
	}
	if strings.TrimSpace(d.dsn) == "" {
		return errors.New("empty postgres dsn")
	}
	if d.conn != nil {
		d.conn.Close()
		d.conn = nil
		d.q = nil
	}

	pool, err := Open(ctx, d.dsn)
	if err != nil {
		return fmt.Errorf("open postgres driver: %w", err)
	}
	p := pool
	q := gensqlc.New(pool)
	d.conn = p
	d.q = q
	return nil
}

func (d *Driver) Put(ctx context.Context, n *merkle.Node) (bool, error) {
	if n == nil {
		return false, errors.New("cannot store nil node")
	}

	bucketJSON, err := json.Marshal(n.Bucket)
	if err != nil {
		return false, fmt.Errorf("marshal bucket: %w", err)
	}
	contentJSON, err := json.Marshal(n.Bucket.Content)
	if err != nil {
		return false, fmt.Errorf("marshal content: %w", err)
	}

	createdAt := n.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	rows, err := d.q.InsertNode(ctx, gensqlc.InsertNodeParams{
		Hash:                     n.Hash,
		Bucket:                   bucketJSON,
		Type:                     nullStringValue(n.Bucket.Type),
		Role:                     nullStringValue(n.Bucket.Role),
		Content:                  contentJSON,
		Model:                    nullStringValue(n.Bucket.Model),
		Provider:                 nullStringValue(n.Bucket.Provider),
		AgentName:                nullStringValue(n.Bucket.AgentName),
		StopReason:               nullStringValue(n.StopReason),
		PromptTokens:             nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.PromptTokens }),
		CompletionTokens:         nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CompletionTokens }),
		TotalTokens:              nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.TotalTokens }),
		CacheCreationInputTokens: nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheCreationInputTokens }),
		CacheReadInputTokens:     nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheReadInputTokens }),
		TotalDurationNs:          nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.TotalDurationNs }),
		PromptDurationNs:         nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.PromptDurationNs }),
		Project:                  nullStringValue(n.Project),
		CreatedAt:                pgtype.Timestamptz{Time: createdAt, Valid: true},
		ParentHash:               nullStringPtr(n.ParentHash),
	})
	if err != nil {
		return false, fmt.Errorf("insert node: %w", err)
	}

	return rows > 0, nil
}

func (d *Driver) Get(ctx context.Context, hash string) (*merkle.Node, error) {
	row, err := d.q.GetNode(ctx, hash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.NotFoundError{Hash: hash}
		}
		return nil, fmt.Errorf("get node: %w", err)
	}
	return merkleNodeFromRow(row)
}

func (d *Driver) Has(ctx context.Context, hash string) (bool, error) {
	exists, err := d.q.HasNode(ctx, hash)
	if err != nil {
		return false, fmt.Errorf("check node existence: %w", err)
	}
	return exists, nil
}

func (d *Driver) GetByParent(ctx context.Context, parentHash *string) ([]*merkle.Node, error) {
	var (
		rows []gensqlc.Node
		err  error
	)
	if parentHash == nil {
		rows, err = d.q.GetRootNodes(ctx)
	} else {
		rows, err = d.q.GetNodesByParent(ctx, nullStringValue(*parentHash))
	}
	if err != nil {
		return nil, fmt.Errorf("get nodes by parent: %w", err)
	}
	return merkleNodesFromRows(rows)
}

func (d *Driver) List(ctx context.Context) ([]*merkle.Node, error) {
	rows, err := d.q.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	return merkleNodesFromRows(rows)
}

func (d *Driver) Roots(ctx context.Context) ([]*merkle.Node, error) {
	rows, err := d.q.GetRootNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list roots: %w", err)
	}
	return merkleNodesFromRows(rows)
}

func (d *Driver) Leaves(ctx context.Context) ([]*merkle.Node, error) {
	rows, err := d.q.ListLeaves(ctx)
	if err != nil {
		return nil, fmt.Errorf("list leaves: %w", err)
	}
	return merkleNodesFromRows(rows)
}

func (d *Driver) ListSessions(ctx context.Context, opts storage.ListOpts) (*storage.Page[*merkle.Node], error) {
	opts = opts.Normalize()
	cursor, err := storage.DecodeCursor(opts.Cursor)
	if err != nil {
		return nil, err
	}

	rows, err := d.q.ListSessions(ctx, gensqlc.ListSessionsParams{
		ProjectFilter:   nullStringValue(opts.Project),
		AgentFilter:     nullStringValue(opts.Agent),
		ModelFilter:     nullStringValue(opts.Model),
		ProviderFilter:  nullStringValue(opts.Provider),
		SinceFilter:     nullTimePtr(opts.Since),
		UntilFilter:     nullTimePtr(opts.Until),
		CursorCreatedAt: nullCursorTime(opts.Cursor, cursor.CreatedAt),
		CursorHash:      nullCursorHash(opts.Cursor, cursor.Hash),
		LimitCount:      safeLimitCount(opts.Limit + 1),
	})
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}

	hasMore := len(rows) > opts.Limit
	if hasMore {
		rows = rows[:opts.Limit]
	}

	items, err := merkleNodesFromRows(rows)
	if err != nil {
		return nil, err
	}

	page := &storage.Page[*merkle.Node]{Items: items}
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		page.NextCursor = storage.Cursor{CreatedAt: last.CreatedAt, Hash: last.Hash}.Encode()
	}
	return page, nil
}

func (d *Driver) CountSessions(ctx context.Context, opts storage.ListOpts) (storage.SessionStats, error) {
	params := gensqlc.AggregateSessionsParams{
		ProjectFilter:  nullStringValue(opts.Project),
		AgentFilter:    nullStringValue(opts.Agent),
		ModelFilter:    nullStringValue(opts.Model),
		ProviderFilter: nullStringValue(opts.Provider),
		SinceFilter:    nullTimePtr(opts.Since),
		UntilFilter:    nullTimePtr(opts.Until),
	}

	// Run both aggregates inside a single read-only REPEATABLE READ
	// snapshot so a concurrent Put between the two queries cannot make
	// the per-model cost rollup reflect more nodes than the scalar
	// token totals — both reads see the same MVCC view. /v1/stats
	// returning self-inconsistent fields was raised on the PR review.
	tx, err := d.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("begin stats snapshot tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := d.q.WithTx(tx)

	row, err := qtx.AggregateSessions(ctx, params)
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("aggregate sessions: %w", err)
	}

	byModel, err := qtx.AggregateSessionsByModel(ctx, gensqlc.AggregateSessionsByModelParams(params))
	if err != nil {
		return storage.SessionStats{}, fmt.Errorf("aggregate sessions by model: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.SessionStats{}, fmt.Errorf("commit stats snapshot tx: %w", err)
	}

	var perModel map[string]storage.ModelTokenStats
	for _, m := range byModel {
		if !m.Model.Valid || m.Model.String == "" {
			continue
		}
		if perModel == nil {
			perModel = make(map[string]storage.ModelTokenStats, len(byModel))
		}
		perModel[m.Model.String] = storage.ModelTokenStats{
			InputTokens:         m.InputTokens,
			OutputTokens:        m.OutputTokens,
			CacheCreationTokens: m.CacheCreationTokens,
			CacheReadTokens:     m.CacheReadTokens,
		}
	}

	return storage.SessionStats{
		SessionCount:        int(row.SessionCount),
		TurnCount:           int(row.TurnCount),
		RootCount:           int(row.RootCount),
		CompletedCount:      int(row.CompletedCount),
		InputTokens:         row.InputTokens,
		OutputTokens:        row.OutputTokens,
		CacheCreationTokens: row.CacheCreationTokens,
		CacheReadTokens:     row.CacheReadTokens,
		TotalDurationNs:     row.TotalDurationNs,
		ToolCalls:           int(row.ToolCalls),
		PerModel:            perModel,
	}, nil
}

func (d *Driver) LoadDag(ctx context.Context, hash string) (*merkle.Dag, error) {
	dag := merkle.NewDag()

	ancestry, err := d.Ancestry(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting ancestry for %s: %w", hash, err)
	}

	if len(ancestry) == 0 {
		return nil, fmt.Errorf("node %s not found", hash)
	}

	for i := len(ancestry) - 1; i >= 0; i-- {
		if _, err := dag.AddNode(ancestry[i]); err != nil {
			return nil, fmt.Errorf("adding ancestor node %s: %w", ancestry[i].Hash, err)
		}
	}

	seen := map[string]struct{}{hash: {}}
	var addDescendants func(string) error
	addDescendants = func(parentHash string) error {
		children, err := d.GetByParent(ctx, &parentHash)
		if err != nil {
			return fmt.Errorf("getting children of %s: %w", parentHash, err)
		}
		for _, child := range children {
			if _, err := dag.AddNode(child); err != nil {
				return fmt.Errorf("adding child node %s: %w", child.Hash, err)
			}
			if _, ok := seen[child.Hash]; ok {
				continue
			}
			seen[child.Hash] = struct{}{}
			if err := addDescendants(child.Hash); err != nil {
				return err
			}
		}
		return nil
	}

	if err := addDescendants(hash); err != nil {
		return nil, err
	}

	return dag, nil
}

func (d *Driver) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
	chain, err := d.AncestryChain(ctx, hash)
	if err != nil {
		return nil, err
	}
	return chain.Nodes, nil
}

func (d *Driver) AncestryChain(ctx context.Context, hash string) (*storage.Chain, error) {
	chains, err := d.AncestryChains(ctx, []string{hash})
	if err != nil {
		return nil, err
	}
	chain, ok := chains[hash]
	if !ok || len(chain.Nodes) == 0 {
		return nil, storage.NotFoundError{Hash: hash}
	}
	return chain, nil
}

func (d *Driver) AncestryChains(ctx context.Context, hashes []string) (map[string]*storage.Chain, error) {
	unique := dedupeHashes(hashes)
	if len(unique) == 0 {
		return map[string]*storage.Chain{}, nil
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin duckdb transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := d.q.WithTx(tx)
	if err := qtx.DuckdbForceExecution(ctx); err != nil {
		return nil, fmt.Errorf("enable pg_duckdb execution: %w", err)
	}

	rows, err := qtx.AncestryChains(ctx, gensqlc.AncestryChainsParams{
		Hashes:   unique,
		MaxDepth: int32(maxAncestryDepth),
	})
	if err != nil {
		return nil, fmt.Errorf("run ancestry query: %w", err)
	}

	chains := make(map[string]*storage.Chain, len(unique))
	seen := make(map[string]map[string]struct{}, len(unique))
	expectedParent := make(map[string]string, len(unique))
	foundStart := make(map[string]bool, len(unique))

	for _, row := range rows {
		startHash := interfaceString(row.StartHash)
		hash := interfaceString(row.Hash)

		foundStart[startHash] = true
		if _, ok := chains[startHash]; !ok {
			chains[startHash] = &storage.Chain{}
			seen[startHash] = map[string]struct{}{}
		}
		if _, ok := seen[startHash][hash]; ok {
			chains[startHash].Incomplete = true
			chains[startHash].CycleDetected = true
			continue
		}
		seen[startHash][hash] = struct{}{}

		node, err := merkleNodeFromAncestryRow(row)
		if err != nil {
			return nil, err
		}
		chains[startHash].Nodes = append(chains[startHash].Nodes, node)
		if row.ParentHash != nil {
			if parentHash := interfaceString(row.ParentHash); parentHash != "" {
				expectedParent[startHash] = parentHash
			} else {
				delete(expectedParent, startHash)
			}
		} else {
			delete(expectedParent, startHash)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit ancestry transaction: %w", err)
	}

	for _, start := range unique {
		if !foundStart[start] {
			continue
		}
		if missing, ok := expectedParent[start]; ok {
			chains[start].Incomplete = true
			chains[start].MissingParent = missing
		}
	}

	return chains, nil
}

func (d *Driver) Depth(ctx context.Context, hash string) (int, error) {
	chain, err := d.AncestryChain(ctx, hash)
	if err != nil {
		return 0, err
	}
	return len(chain.Nodes) - 1, nil
}

func (d *Driver) ListParentRefs(ctx context.Context) ([]storage.ParentRef, error) {
	rows, err := d.q.ListParentRefs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list parent refs: %w", err)
	}
	out := make([]storage.ParentRef, len(rows))
	for i, row := range rows {
		out[i] = storage.ParentRef{Hash: row.Hash, ParentHash: stringPtr(row.ParentHash)}
	}
	return out, nil
}

func (d *Driver) UpdateUsage(ctx context.Context, hash string, usage *llm.Usage) error {
	if usage == nil {
		return errors.New("cannot update with nil usage")
	}

	return d.q.UpdateUsage(ctx, gensqlc.UpdateUsageParams{
		Hash:                     hash,
		PromptTokens:             nullPositiveInt32Value(usage.PromptTokens),
		CompletionTokens:         nullPositiveInt32Value(usage.CompletionTokens),
		TotalTokens:              nullPositiveInt32Value(usage.TotalTokens),
		CacheCreationInputTokens: nullPositiveInt32Value(usage.CacheCreationInputTokens),
		CacheReadInputTokens:     nullPositiveInt32Value(usage.CacheReadInputTokens),
		TotalDurationNs:          nullPositiveInt64Value(usage.TotalDurationNs),
		PromptDurationNs:         nullPositiveInt64Value(usage.PromptDurationNs),
	})
}

func (d *Driver) DB() *pgxpool.Pool {
	if d == nil {
		return nil
	}
	return d.conn
}

func (d *Driver) Close() error {
	if d == nil || d.conn == nil {
		return nil
	}
	d.conn.Close()
	return nil
}

func migrateUp(dsn string) error {
	src, err := iofs.New(tapesmigrations.FS, ".")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toMigrateDSN(dsn))
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func toMigrateDSN(dsn string) string {
	for _, p := range []string{"pgx5://", "postgres://", "postgresql://"} {
		if strings.HasPrefix(dsn, p) {
			if p == "pgx5://" {
				return dsn
			}
			return "pgx5://" + strings.TrimPrefix(dsn, p)
		}
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return dsn
	}

	port := cfg.Port
	if port == 0 {
		port = 5432
	}

	v := url.URL{
		Scheme: "pgx5",
		Host:   fmt.Sprintf("%s:%d", cfg.Host, port),
		Path:   "/" + cfg.Database,
	}
	if cfg.User != "" {
		if cfg.Password != "" {
			v.User = url.UserPassword(cfg.User, cfg.Password)
		} else {
			v.User = url.User(cfg.User)
		}
	}
	queryParams := migrateQueryParams(dsn)
	maps.Copy(queryParams, cfg.RuntimeParams)
	if len(queryParams) > 0 {
		q := url.Values{}
		keys := make([]string, 0, len(queryParams))
		for k := range queryParams {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			q.Set(k, queryParams[k])
		}
		v.RawQuery = q.Encode()
	}

	return v.String()
}

func migrateQueryParams(dsn string) map[string]string {
	queryParams := make(map[string]string)
	settings, err := parseKeywordValueDSN(dsn)
	if err != nil {
		return queryParams
	}

	for _, k := range []string{"sslmode", "sslcert", "sslkey", "sslrootcert", "sslpassword", "sslnegotiation", "sslsni"} {
		if v, ok := settings[k]; ok {
			queryParams[k] = v
		}
	}
	return queryParams
}

func parseKeywordValueDSN(s string) (map[string]string, error) {
	settings := make(map[string]string)
	for len(s) > 0 {
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return nil, errors.New("invalid keyword/value")
		}

		key := strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		var val string
		if len(s) > 0 && s[0] == '\'' {
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return nil, errors.New("unterminated quoted string in connection info string")
			}
			val = strings.ReplaceAll(strings.ReplaceAll(s[:end], "\\\\", "\\"), "\\'", "'")
			s = s[end+1:]
		} else {
			end := 0
			for ; end < len(s); end++ {
				if strings.ContainsRune(" \t\n\r\v\f", rune(s[end])) {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, errors.New("invalid backslash")
					}
				}
			}
			val = strings.ReplaceAll(strings.ReplaceAll(s[:end], "\\\\", "\\"), "\\'", "'")
			s = s[end:]
		}
		if key == "" {
			return nil, errors.New("invalid keyword/value")
		}
		settings[key] = val
		s = strings.TrimLeft(s, " \t\n\r\v\f")
	}
	return settings, nil
}

func merkleNodesFromRows(rows []gensqlc.Node) ([]*merkle.Node, error) {
	out := make([]*merkle.Node, len(rows))
	for i, row := range rows {
		n, err := merkleNodeFromRow(row)
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

func merkleNodeFromAncestryRow(row gensqlc.AncestryChainsRow) (*merkle.Node, error) {
	promptTokens := pgtype.Int4{}
	completionTokens := pgtype.Int4{}
	totalTokens := pgtype.Int4{}
	cacheCreation := pgtype.Int4{}
	cacheRead := pgtype.Int4{}
	totalDuration := pgtype.Int8{}
	promptDuration := pgtype.Int8{}
	if row.HasUsage {
		promptTokens = nullInt32Value(interfaceInt32(row.PromptTokens))
		completionTokens = nullInt32Value(interfaceInt32(row.CompletionTokens))
		totalTokens = nullInt32Value(interfaceInt32(row.TotalTokens))
		cacheCreation = nullInt32Value(interfaceInt32(row.CacheCreationInputTokens))
		cacheRead = nullInt32Value(interfaceInt32(row.CacheReadInputTokens))
		totalDuration = nullInt64Value(interfaceInt64(row.TotalDurationNs))
		promptDuration = nullInt64Value(interfaceInt64(row.PromptDurationNs))
	}

	return merkleNodeFromParts(
		interfaceString(row.Hash),
		nullStringValue(interfaceString(row.ParentHash)),
		interfaceBytes(row.Bucket),
		nullStringValue(interfaceString(row.Type)),
		nullStringValue(interfaceString(row.Role)),
		interfaceBytes(row.Content),
		nullStringValue(interfaceString(row.Model)),
		nullStringValue(interfaceString(row.Provider)),
		nullStringValue(interfaceString(row.AgentName)),
		nullStringValue(interfaceString(row.StopReason)),
		promptTokens,
		completionTokens,
		totalTokens,
		cacheCreation,
		cacheRead,
		totalDuration,
		promptDuration,
		nullStringValue(interfaceString(row.Project)),
		row.CreatedAt,
	)
}

func merkleNodeFromRow(row gensqlc.Node) (*merkle.Node, error) {
	return merkleNodeFromParts(
		row.Hash,
		row.ParentHash,
		row.Bucket,
		row.Type,
		row.Role,
		row.Content,
		row.Model,
		row.Provider,
		row.AgentName,
		row.StopReason,
		row.PromptTokens,
		row.CompletionTokens,
		row.TotalTokens,
		row.CacheCreationInputTokens,
		row.CacheReadInputTokens,
		row.TotalDurationNs,
		row.PromptDurationNs,
		row.Project,
		row.CreatedAt,
	)
}

func merkleNodeFromParts(
	hash string,
	parentHash pgtype.Text,
	bucketRaw []byte,
	typeVal pgtype.Text,
	role pgtype.Text,
	contentRaw []byte,
	model pgtype.Text,
	provider pgtype.Text,
	agentName pgtype.Text,
	stopReason pgtype.Text,
	promptTokens pgtype.Int4,
	completionTokens pgtype.Int4,
	totalTokens pgtype.Int4,
	cacheCreation pgtype.Int4,
	cacheRead pgtype.Int4,
	totalDuration pgtype.Int8,
	promptDuration pgtype.Int8,
	project pgtype.Text,
	createdAt pgtype.Timestamptz,
) (*merkle.Node, error) {
	bucket, err := decodeBucket(bucketRaw, typeVal, role, contentRaw, model, provider, agentName)
	if err != nil {
		return nil, fmt.Errorf("decode node %s: %w", hash, err)
	}

	n := &merkle.Node{
		Hash:       hash,
		ParentHash: stringPtr(parentHash),
		Bucket:     bucket,
		StopReason: stopReason.String,
		Project:    project.String,
		CreatedAt:  createdAt.Time,
	}
	if usage := usageFromNulls(promptTokens, completionTokens, totalTokens, cacheCreation, cacheRead, totalDuration, promptDuration); usage != nil {
		n.Usage = usage
	}
	return n, nil
}

func decodeBucket(
	bucketRaw []byte,
	typeVal pgtype.Text,
	role pgtype.Text,
	contentRaw []byte,
	model pgtype.Text,
	provider pgtype.Text,
	agentName pgtype.Text,
) (merkle.Bucket, error) {
	if len(bucketRaw) > 0 {
		var bucket merkle.Bucket
		if err := json.Unmarshal(bucketRaw, &bucket); err == nil {
			return bucket, nil
		}
	}

	var content []llm.ContentBlock
	if len(contentRaw) > 0 {
		if err := json.Unmarshal(contentRaw, &content); err != nil {
			return merkle.Bucket{}, fmt.Errorf("decode content: %w", err)
		}
	}

	return merkle.Bucket{
		Type:      typeVal.String,
		Role:      role.String,
		Content:   content,
		Model:     model.String,
		Provider:  provider.String,
		AgentName: agentName.String,
	}, nil
}

func usageFromNulls(
	promptTokens pgtype.Int4,
	completionTokens pgtype.Int4,
	totalTokens pgtype.Int4,
	cacheCreation pgtype.Int4,
	cacheRead pgtype.Int4,
	totalDuration pgtype.Int8,
	promptDuration pgtype.Int8,
) *llm.Usage {
	if !promptTokens.Valid && !completionTokens.Valid && !totalTokens.Valid &&
		!cacheCreation.Valid && !cacheRead.Valid && !totalDuration.Valid && !promptDuration.Valid {
		return nil
	}
	return &llm.Usage{
		PromptTokens:             int(promptTokens.Int32),
		CompletionTokens:         int(completionTokens.Int32),
		TotalTokens:              int(totalTokens.Int32),
		CacheCreationInputTokens: int(cacheCreation.Int32),
		CacheReadInputTokens:     int(cacheRead.Int32),
		TotalDurationNs:          totalDuration.Int64,
		PromptDurationNs:         promptDuration.Int64,
	}
}

func nullStringValue(s string) pgtype.Text {
	if strings.TrimSpace(s) == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

func nullStringPtr(s *string) pgtype.Text {
	if s == nil || strings.TrimSpace(*s) == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func stringPtr(s pgtype.Text) *string {
	if !s.Valid {
		return nil
	}
	v := s.String
	return &v
}

func nullTimePtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func nullCursorTime(cursor string, t time.Time) pgtype.Timestamptz {
	if cursor == "" {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func nullCursorHash(cursor, hash string) pgtype.Text {
	if cursor == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: hash, Valid: true}
}

func nullInt32FromUsage(usage *llm.Usage, get func(*llm.Usage) int) pgtype.Int4 {
	if usage == nil {
		return pgtype.Int4{}
	}
	v := max(min(get(usage), math.MaxInt32), math.MinInt32)
	return pgtype.Int4{Int32: int32(v), Valid: true} //nolint:gosec // bounded above
}

func safeLimitCount(v int) int32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

func nullInt64FromUsage(usage *llm.Usage, get func(*llm.Usage) int64) pgtype.Int8 {
	if usage == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: get(usage), Valid: true}
}

func nullInt32Value(v int32) pgtype.Int4 {
	return pgtype.Int4{Int32: v, Valid: true}
}

func nullInt64Value(v int64) pgtype.Int8 {
	return pgtype.Int8{Int64: v, Valid: true}
}

func nullPositiveInt32Value(v int) pgtype.Int4 {
	if v <= 0 {
		return pgtype.Int4{}
	}
	bounded := max(min(v, math.MaxInt32), math.MinInt32)
	return pgtype.Int4{Int32: int32(bounded), Valid: true} //nolint:gosec // bounded above
}

func nullPositiveInt64Value(v int64) pgtype.Int8 {
	if v <= 0 {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: v, Valid: true}
}

func interfaceString(v any) string {
	s, _ := v.(string)
	return s
}

func interfaceBytes(v any) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	case nil:
		return nil
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return b
	}
}

func interfaceInt32(v any) int32 {
	switch i := v.(type) {
	case int32:
		return i
	case int64:
		return int32(i) //nolint:gosec
	}
	return 0
}

func interfaceInt64(v any) int64 {
	switch i := v.(type) {
	case int64:
		return i
	case int32:
		return int64(i)
	}
	return 0
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
	sort.Strings(out)
	return out
}
