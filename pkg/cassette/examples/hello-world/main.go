// Command hello-world is the minimum viable tapes cassette.
//
// It exists to be run, not read about: the deployment starts it, and tapes
// proxies /v1/cassettes/hello-world/* to its declared URL, fetches its OpenAPI
// document, and publishes it in discovery.
//
// Four things make it a cassette:
//
//  1. /ping answers 200, which is the api.health anchor core probes.
//  2. /openapi serves its OpenAPI document, which core fetches and aggregates.
//  3. /api/hello-world/hello is its actual API, served under the prefix its
//     OpenAPI metadata declares (api.prefix_path) rather than at /hello. That prefix
//     is what makes core's containment check enforceable: every path this
//     process serves, and every path in its OpenAPI document, lies under one
//     head core can name in advance. Core strips that head and republishes the
//     paths under /v1/cassettes/hello-world, which is the one rewrite in the
//     pipeline and the only thing standing between the two names.
//  4. It declares a `hello` table in its own schema and runs its own migration
//     for it. The declaration is metadata for deployment tooling; core does not
//     apply grants, create schemas, or create the cassette's tables.
//
// Configuration arrives entirely through the environment supplied by the
// deployment. The cassette reads env vars and nothing else — no config file,
// no flags, and no knowledge of which runtime started it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaults for every environment variable this cassette reads, so it runs with
// none of them set. A cassette that requires a configured environment to start
// is a cassette nobody can try.
const (
	defaultListen   = "0.0.0.0:9999"
	defaultName     = "hello-world"
	defaultGreeting = "Hello"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := run(logger); err != nil {
		logger.Error("hello-world cassette failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	name := envOrDefault("CASSETTE_NAME", defaultName)
	listen := envOrDefault("CASSETTE_LISTEN", defaultListen)
	greeting := envOrDefault("GREETING", defaultGreeting)

	store, err := openStore(ctx, os.Getenv("TAPES_DATABASE_URL"), name)
	if err != nil {
		return err
	}
	defer store.Close()

	cassette := &cassette{name: name, greeting: greeting, store: store, logger: logger}
	server := &http.Server{
		Addr:              listen,
		Handler:           cassette.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Serve in the background so the signal context can shut the listener down
	// cleanly when its process manager asks it to stop.
	errs := make(chan error, 1)
	go func() {
		logger.Info("hello-world cassette listening",
			"listen", listen, "name", name, "store", store.Kind())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
			return
		}
		errs <- nil
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return server.Shutdown(shutdownCtx)
}

// cassette is the whole program: an identity, a greeting, and somewhere to put
// rows.
type cassette struct {
	name     string
	greeting string
	store    store
	logger   *slog.Logger
}

func (c *cassette) routes() http.Handler {
	mux := http.NewServeMux()

	// Anchors. These live at the root of the listener because they describe the
	// process, not the API — core probes and fetches them directly and never
	// proxies them.
	mux.HandleFunc("GET /ping", c.handlePing)
	mux.HandleFunc("GET /openapi", c.handleOpenAPI)

	// The API itself, under the prefix clients call through tapes.
	prefix := "/api/" + c.name
	mux.HandleFunc("GET "+prefix+"/hello", c.handleGetHello)
	mux.HandleFunc("POST "+prefix+"/hello", c.handlePostHello)

	return mux
}

func (c *cassette) handlePing(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "cassette": c.name})
}

func (c *cassette) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPIDocument(c.name))
}

// handleGetHello reads back what was written, so the table is demonstrably a
// table and not a log line.
func (c *cassette) handleGetHello(w http.ResponseWriter, r *http.Request) {
	rows, err := c.store.List(r.Context())
	if err != nil {
		c.fail(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":  c.greeting + " world",
		"greeting": c.greeting,
		"cassette": c.name,
		"store":    c.store.Kind(),
		"rows":     rows,
	})
}

func (c *cassette) handlePostHello(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Hello string `json:"hello"`
		World string `json:"world"`
	}
	// An empty body is a valid request: POST with no payload writes the
	// default row, which keeps the demo to a single curl with no -d.
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be JSON: " + err.Error()})
			return
		}
	}
	if body.Hello == "" {
		body.Hello = "hello"
	}
	if body.World == "" {
		body.World = "world"
	}

	row, err := c.store.Insert(r.Context(), body.Hello, body.World)
	if err != nil {
		c.fail(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, row)
}

// fail reports a storage error as a 500 without leaking the DSN, which is the
// one string in this process that must never reach a response body.
func (c *cassette) fail(w http.ResponseWriter, err error) {
	c.logger.Error("request failed", "error", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage unavailable"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}

	return fallback
}

// Row is one record in the cassette's `hello` table.
type Row struct {
	ID        int64     `json:"id"`
	Hello     string    `json:"hello"`
	World     string    `json:"world"`
	CreatedAt time.Time `json:"created_at"`
}

// store is the cassette's own persistence. It has two implementations because
// a cassette must be runnable without Postgres: the memory store says so in
// every response
// rather than pretending durability it does not have.
type store interface {
	// Kind names the backing store, and is echoed in responses so a demo can
	// never be misread as durable when it is not.
	Kind() string
	Insert(ctx context.Context, hello, world string) (Row, error)
	List(ctx context.Context) ([]Row, error)
	Close()
}

func openStore(ctx context.Context, dsn, name string) (store, error) {
	if strings.TrimSpace(dsn) == "" {
		return &memoryStore{}, nil
	}

	return openPostgresStore(ctx, dsn, name)
}

// memoryStore is the no-database fallback.
type memoryStore struct {
	mu   sync.Mutex
	rows []Row
}

func (s *memoryStore) Kind() string { return "memory" }

func (s *memoryStore) Insert(_ context.Context, hello, world string) (Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row := Row{ID: int64(len(s.rows) + 1), Hello: hello, World: world, CreatedAt: time.Now().UTC()}
	s.rows = append(s.rows, row)

	return row, nil
}

func (s *memoryStore) List(_ context.Context) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]Row(nil), s.rows...), nil
}

func (s *memoryStore) Close() {}

// postgresStore owns exactly one table in exactly one schema, and creates both
// itself. The deployment provisions its role and grants; what goes inside is
// the cassette's business and core never sees the DDL.
type postgresStore struct {
	pool   *pgxpool.Pool
	schema string
}

func openPostgresStore(ctx context.Context, dsn, name string) (store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	s := &postgresStore{pool: pool, schema: name}
	if err := s.migrate(ctx); err != nil {
		pool.Close()

		return nil, err
	}

	return s, nil
}

func (s *postgresStore) Kind() string { return "postgres" }

// migrate is the cassette's own migration, run at startup. The identifiers are
// quoted because a cassette name may legally contain a hyphen (`hello-world`)
// and an unquoted hyphen is a subtraction operator, not an identifier.
func (s *postgresStore) migrate(ctx context.Context) error {
	statements := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdentifier(s.schema)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.hello (
			id         BIGSERIAL PRIMARY KEY,
			hello      TEXT        NOT NULL,
			world      TEXT        NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, quoteIdentifier(s.schema)),
	}
	for _, statement := range statements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrating hello table: %w", err)
		}
	}

	return nil
}

func (s *postgresStore) Insert(ctx context.Context, hello, world string) (Row, error) {
	var row Row
	query := fmt.Sprintf(
		`INSERT INTO %s.hello (hello, world) VALUES ($1, $2) RETURNING id, hello, world, created_at`,
		quoteIdentifier(s.schema),
	)
	err := s.pool.QueryRow(ctx, query, hello, world).
		Scan(&row.ID, &row.Hello, &row.World, &row.CreatedAt)
	if err != nil {
		return Row{}, fmt.Errorf("inserting hello row: %w", err)
	}

	return row, nil
}

func (s *postgresStore) List(ctx context.Context) ([]Row, error) {
	query := fmt.Sprintf(
		`SELECT id, hello, world, created_at FROM %s.hello ORDER BY id`,
		quoteIdentifier(s.schema),
	)
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("listing hello rows: %w", err)
	}
	defer rows.Close()

	result := make([]Row, 0)
	for rows.Next() {
		var row Row
		if err := rows.Scan(&row.ID, &row.Hello, &row.World, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning hello row: %w", err)
		}
		result = append(result, row)
	}

	return result, rows.Err()
}

func (s *postgresStore) Close() { s.pool.Close() }

// quoteIdentifier renders a SQL identifier safely. Cassette names are already
// validated against a strict pattern upstream, so this is belt and braces —
// but a schema name reaching SQL unquoted is exactly the kind of thing that is
// fine until the day it is not.
func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
