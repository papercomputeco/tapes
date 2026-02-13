package deckcmder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/papercomputeco/tapes/pkg/deck"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	deckweb "github.com/papercomputeco/tapes/web/deck"
)

type insightOptions struct {
	enabled bool
	model   string
	target  string
}

type facetService struct {
	query     deck.Querier
	extractor *deck.FacetExtractor
	store     deck.FacetStore

	backfillMu   sync.Mutex
	backfillCh   chan error
	backfillDone bool
}

func newFacetService(query deck.Querier, store deck.FacetStore, llmCall deck.LLMCallFunc) *facetService {
	return &facetService{
		query:     query,
		extractor: deck.NewFacetExtractor(query, llmCall, store),
		store:     store,
	}
}

func (s *facetService) Aggregate(ctx context.Context) (*deck.FacetAnalytics, error) {
	return s.extractor.AggregateFacets(ctx)
}

func (s *facetService) GetFacet(ctx context.Context, sessionID string) (*deck.SessionFacet, error) {
	if s.store == nil {
		return s.extractor.Extract(ctx, sessionID)
	}

	facet, err := s.store.GetFacet(ctx, sessionID)
	if err == nil {
		return facet, nil
	}
	if !ent.IsNotFound(err) {
		return nil, err
	}

	return s.extractor.Extract(ctx, sessionID)
}

func (s *facetService) BackfillAll(ctx context.Context) error {
	s.backfillMu.Lock()
	if s.backfillDone {
		s.backfillMu.Unlock()
		return nil
	}
	if s.backfillCh != nil {
		ch := s.backfillCh
		s.backfillMu.Unlock()
		return <-ch
	}

	ch := make(chan error, 1)
	s.backfillCh = ch
	s.backfillMu.Unlock()

	err := s.backfillAll(ctx)

	s.backfillMu.Lock()
	if err == nil {
		s.backfillDone = true
	}
	s.backfillCh = nil
	s.backfillMu.Unlock()

	ch <- err
	close(ch)
	return err
}

func (s *facetService) backfillAll(ctx context.Context) error {
	overview, err := s.query.Overview(ctx, deck.Filters{})
	if err != nil {
		return err
	}

	for _, session := range overview.Sessions {
		if _, err := s.GetFacet(ctx, session.ID); err != nil {
			// Skip sessions that fail extraction rather than aborting.
			fmt.Printf("facet extraction skipped for %s: %v\n", session.ID, err)
			continue
		}
	}

	return nil
}

func runDeckWeb(ctx context.Context, query deck.Querier, filters deck.Filters, port int, insights insightOptions) error {
	address := fmt.Sprintf("127.0.0.1:%d", port)

	var facets *facetService
	if insights.enabled {
		queryWithClient, ok := query.(*deck.Query)
		if !ok {
			return errors.New("insights require sqlite-backed deck query")
		}
		store := deck.NewEntFacetStore(queryWithClient.Client())
		llmCall := deck.NewOllamaFacetLLM(insights.target, insights.model)
		facets = newFacetService(query, store, llmCall)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", func(w http.ResponseWriter, r *http.Request) {
		queryFilters, err := applyWebFilters(filters, r)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		overview, err := query.Overview(r.Context(), queryFilters)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, overview)
	})

	mux.HandleFunc("/api/analytics", func(w http.ResponseWriter, r *http.Request) {
		queryFilters, err := applyWebFilters(filters, r)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		analytics, err := query.AnalyticsOverview(r.Context(), queryFilters)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, analytics)
	})

	mux.HandleFunc("/api/analytics/session/", func(w http.ResponseWriter, r *http.Request) {
		sessionID := strings.TrimPrefix(r.URL.Path, "/api/analytics/session/")
		if sessionID == "" {
			http.Error(w, "missing session id", http.StatusBadRequest)
			return
		}
		sa, err := query.SessionAnalytics(r.Context(), sessionID)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, sa)
	})

	mux.HandleFunc("/api/session/", func(w http.ResponseWriter, r *http.Request) {
		sessionID := strings.TrimPrefix(r.URL.Path, "/api/session/")
		if sessionID == "" {
			http.Error(w, "missing session id", http.StatusBadRequest)
			return
		}

		detail, err := query.SessionDetail(r.Context(), sessionID)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, detail)
	})

	// Facet endpoints — return empty data when no extractor is configured.
	mux.HandleFunc("/api/facets", func(w http.ResponseWriter, r *http.Request) {
		if facets == nil {
			writeJSON(w, deck.FacetAnalytics{
				GoalDistribution:    map[string]int{},
				OutcomeDistribution: map[string]int{},
				SessionTypes:        map[string]int{},
			})
			return
		}
		if err := facets.BackfillAll(r.Context()); err != nil {
			writeJSONError(w, err)
			return
		}
		analytics, err := facets.Aggregate(r.Context())
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, analytics)
	})

	mux.HandleFunc("/api/facets/session/", func(w http.ResponseWriter, r *http.Request) {
		sessionID := strings.TrimPrefix(r.URL.Path, "/api/facets/session/")
		if sessionID == "" {
			http.Error(w, "missing session id", http.StatusBadRequest)
			return
		}
		if facets == nil {
			writeJSON(w, deck.SessionFacet{SessionID: sessionID})
			return
		}
		facet, err := facets.GetFacet(r.Context(), sessionID)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, facet)
	})

	mux.HandleFunc("/session/", func(w http.ResponseWriter, _ *http.Request) {
		serveIndex(w)
	})

	mux.HandleFunc("/analytics", func(w http.ResponseWriter, _ *http.Request) {
		serveIndex(w)
	})

	fileServer := http.FileServer(http.FS(deckweb.FS))
	mux.Handle("/", fileServer)

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}

	fmt.Printf("deck web running at http://%s\n", address)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	return server.Serve(listener)
}

func applyWebFilters(base deck.Filters, r *http.Request) (deck.Filters, error) {
	filters := base
	query := r.URL.Query()

	if value := strings.TrimSpace(query.Get("sort")); value != "" {
		filters.Sort = strings.ToLower(value)
	}
	if value := strings.TrimSpace(query.Get("sort_dir")); value != "" {
		filters.SortDir = strings.ToLower(value)
	}
	if value := strings.TrimSpace(query.Get("status")); value != "" {
		filters.Status = strings.ToLower(value)
	}
	if value := strings.TrimSpace(query.Get("model")); value != "" {
		filters.Model = value
	}
	if value := strings.TrimSpace(query.Get("since")); value != "" {
		duration, err := parseSince(value)
		if err != nil {
			return filters, err
		}
		filters.Since = duration
	}
	if value := strings.TrimSpace(query.Get("from")); value != "" {
		parsed, err := parseTime(value)
		if err != nil {
			return filters, err
		}
		filters.From = &parsed
	}
	if value := strings.TrimSpace(query.Get("to")); value != "" {
		parsed, err := parseTime(value)
		if err != nil {
			return filters, err
		}
		filters.To = &parsed
	}

	return filters, nil
}

func parseSince(value string) (time.Duration, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return 0, nil
	}
	if before, ok := strings.CutSuffix(value, "d"); ok {
		number := before
		days, err := strconv.Atoi(number)
		if err != nil {
			return 0, fmt.Errorf("invalid since days: %w", err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	if strings.HasSuffix(value, "m") && !strings.HasSuffix(value, "ms") {
		number := strings.TrimSuffix(value, "m")
		months, err := strconv.Atoi(number)
		if err != nil {
			return 0, fmt.Errorf("invalid since months: %w", err)
		}
		return time.Duration(months*30) * 24 * time.Hour, nil
	}
	return time.ParseDuration(value)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSONError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	resp := map[string]string{"error": err.Error()}
	if encErr := json.NewEncoder(w).Encode(resp); encErr != nil {
		http.Error(w, encErr.Error(), http.StatusInternalServerError)
	}
}

func serveIndex(w http.ResponseWriter) {
	data, err := deckweb.FS.ReadFile("index.html")
	if err != nil {
		http.Error(w, "missing index", http.StatusInternalServerError)
		return
	}
	// HTML payload for client-side routing
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
