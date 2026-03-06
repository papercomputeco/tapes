package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

// TestKafkaE2E runs the opt-in proxy Kafka e2e test suite against a local
// Confluent Local Kafka service.
func (t *Tapes) TestKafkaE2E(ctx context.Context) (string, error) {
	ollamaSvc, err := t.OllamaStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up ollama stack: %v", err)
	}
	defer ollamaSvc.Stop(ctx)

	kafkaSvc, err := t.KafkaStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up kafka stack: %v", err)
	}
	defer kafkaSvc.Stop(ctx)

	// --- Build the tapes binary ---
	tapesBin := t.goContainer().
		WithExec([]string{"go", "build", "-o", "/usr/local/bin/tapes", "./cli/tapes/"}).
		File("/usr/local/bin/tapes")

	// Base container for running tapes services (needs the binary + service bindings).
	tapesBase := dag.Container().
		From("golang:1.25-bookworm").
		WithFile("/usr/local/bin/tapes", tapesBin).
		WithServiceBinding("kafka", kafkaSvc).
		WithServiceBinding("ollama", ollamaSvc)

	// --- tapes proxy service ---
	proxySvc := tapesBase.
		WithExposedPort(8080).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "proxy",
				"--upstream", fmt.Sprintf("http://ollama:%d", ollamaPort),
				"--provider", "ollama",
				"--listen", ":8080",
				"--project", "e2e-test",
				"--kafka-brokers", fmt.Sprintf("http://kafka:%d", kafkaPort),
				"--kafka-topic", "tapes.e2e.proxy",
			},
		})
	proxySvc.Start(ctx)
	defer proxySvc.Stop(ctx)

	// --- tapes API service ---
	apiSvc := tapesBase.
		WithExposedPort(8081).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "api",
				"--listen", ":8081",
			},
		})
	apiSvc.Start(ctx)
	defer apiSvc.Stop(ctx)

	return t.goContainer().
		WithServiceBinding("kafka", kafkaSvc).
		WithEnvVariable("TAPES_E2E_KAFKA_BROKERS", "kafka:9092").
		WithEnvVariable("TAPES_E2E_KAFKA_TOPIC", "tapes.e2e.proxy").
		WithExec([]string{"go", "test", "-v", "./cmd/tapes/serve/proxy", "-run", "TestProxyKafkaE2E", "-count=1", "-timeout", "3m"}).
		Stdout(ctx)
}
