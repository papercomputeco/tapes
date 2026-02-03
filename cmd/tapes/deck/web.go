package deckcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/deck"
	deckweb "github.com/papercomputeco/tapes/web/deck"
)

func runDeckWeb(ctx context.Context, query *deck.Query, filters deck.Filters, port int) error {
	address := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", func(w http.ResponseWriter, r *http.Request) {
		overview, err := query.Overview(r.Context(), filters)
		if err != nil {
			writeJSONError(w, err)
			return
		}
		writeJSON(w, overview)
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

	fileServer := http.FileServer(http.FS(deckweb.FS))
	mux.Handle("/", fileServer)

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	fmt.Printf("deck web running at http://%s\n", address)

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	return server.Serve(listener)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
