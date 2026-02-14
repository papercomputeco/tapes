package deckcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/deck"
	deckweb "github.com/papercomputeco/tapes/web/deck"
)

// facetDeps holds optional facet extraction dependencies for the web server.
type facetDeps struct {
	extractor *deck.FacetExtractor
	worker    *deck.FacetWorker
	store     deck.FacetStore
}

func runDeckWeb(ctx context.Context, query deck.Querier, filters deck.Filters, port int, facets *facetDeps) error {
	address := fmt.Sprintf("127.0.0.1:%d", port)

	// Start background facet worker if configured
	if facets != nil && facets.worker != nil {
		go facets.worker.Run(ctx)
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

	// Facet endpoints — real data when extractor is configured, empty stubs otherwise.
	mux.HandleFunc("/api/facets", func(w http.ResponseWriter, r *http.Request) {
		if facets == nil || facets.extractor == nil {
			writeJSON(w, deck.FacetAnalytics{
				GoalDistribution:    map[string]int{},
				OutcomeDistribution: map[string]int{},
				SessionTypes:        map[string]int{},
			})
			return
		}

		analytics, err := facets.extractor.AggregateFacets(r.Context())
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

		if facets == nil || facets.store == nil {
			writeJSON(w, deck.SessionFacet{SessionID: sessionID})
			return
		}

		facet, err := facets.store.GetFacet(r.Context(), sessionID)
		if err != nil {
			writeJSON(w, deck.SessionFacet{SessionID: sessionID})
			return
		}
		writeJSON(w, facet)
	})

	mux.HandleFunc("/api/facets/status", func(w http.ResponseWriter, _ *http.Request) {
		if facets == nil || facets.worker == nil {
			writeJSON(w, map[string]int{"done": 0, "total": 0})
			return
		}
		done, total := facets.worker.Progress()
		writeJSON(w, map[string]int{"done": done, "total": total})
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
	if value := strings.TrimSpace(query.Get("project")); value != "" {
		filters.Project = value
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
