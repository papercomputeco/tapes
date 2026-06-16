// Package postgres
package postgres

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

type Driver struct {
	dsn      string
	poolOpts []PoolOption
	conn     *pgxpool.Pool
	q        *gensqlc.Queries
}

// PoolOption tunes the pgx pool configuration built from the DSN.
// Options are applied after the DSN is parsed, so they take precedence
// over pool parameters embedded in the connection string.
type PoolOption func(*pgxpool.Config)

// WithMaxConns caps the pool size. Single-purpose services (e.g. the
// derive worker, which processes one session at a time) should set a
// small cap instead of inheriting pgx's NumCPU-based default.
func WithMaxConns(n int32) PoolOption {
	return func(c *pgxpool.Config) { c.MaxConns = n }
}

// WithConnectTimeout bounds each connection attempt. pgx has no
// built-in connect timeout, so an unreachable-but-blackholed host can
// otherwise hang a startup for the OS TCP timeout (minutes).
func WithConnectTimeout(d time.Duration) PoolOption {
	return func(c *pgxpool.Config) { c.ConnConfig.ConnectTimeout = d }
}

func NewDriver(ctx context.Context, connStr string, opts ...PoolOption) (*Driver, error) {
	d := &Driver{dsn: connStr, poolOpts: opts}
	if err := d.Open(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func Open(ctx context.Context, dsn string, opts ...PoolOption) (*pgxpool.Pool, error) {
	if err := migrateUp(dsn); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	for _, opt := range opts {
		opt(cfg)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
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

	pool, err := Open(ctx, d.dsn, d.poolOpts...)
	if err != nil {
		return fmt.Errorf("open postgres driver: %w", err)
	}
	p := pool
	q := gensqlc.New(pool)
	d.conn = p
	d.q = q
	return nil
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

func nullStringValue(s string) pgtype.Text {
	if strings.TrimSpace(s) == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

func nullTimePtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}
