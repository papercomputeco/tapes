/*
Copyright 2026 Paper Compute Co. All rights reserved.

tapes-extproc is a gRPC External Processor that observes LLM traffic flowing
through Envoy AI Gateway and forwards completed request/response pairs to
the tapes ingest server for capture to the immutable raw_turns log.

It implements the envoy.service.ext_proc.v3.ExternalProcessor service.
*/

package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg := ConfigFromEnv()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting tapes-extproc",
		"listen_addr", cfg.ListenAddr,
		"metrics_addr", cfg.MetricsAddr,
		"ingest_url", cfg.IngestURL,
		"raw_response_mode", cfg.RawResponseMode,
	)

	// Create the ext_proc server.
	processor, err := NewProcessor(cfg)
	if err != nil {
		slog.Error("Failed to create processor", "error", err)
		os.Exit(1)
	}

	// gRPC server.
	grpcServer := grpc.NewServer()
	RegisterServer(grpcServer, processor)

	// Health check.
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Metrics/health HTTP server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", processor.Metrics().Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	httpServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start gRPC listener.
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		slog.Error("Failed to listen", "addr", cfg.ListenAddr, "error", err)
		os.Exit(1)
	}

	// Start servers.
	errCh := make(chan error, 2)
	go func() {
		slog.Info("gRPC server listening", "addr", cfg.ListenAddr)
		errCh <- grpcServer.Serve(lis)
	}()
	go func() {
		slog.Info("HTTP server listening", "addr", cfg.MetricsAddr)
		errCh <- httpServer.ListenAndServe()
	}()

	// Wait for signal.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		slog.Error("Server error", "error", err)
	case <-ctx.Done():
		slog.Info("Shutting down")
	}

	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
