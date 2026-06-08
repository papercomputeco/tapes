package main

import (
	"context"
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

	postgresSvc, err := t.PostgresStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up postgres stack: %v", err)
	}

	tapesBase := t.BuildTapesDevImage(ctx)
	tapesSvc := TapesSvc(
		ctx,
		tapesBase,
		WithPostgresSvc(postgresSvc),
		WithKafkaSvc(kafkaSvc),
		WithOllamaSvc(ollamaSvc),
	)

	return t.goContainer().
		WithServiceBinding("kafka", kafkaSvc).
		WithServiceBinding("tapes-proxy", tapesSvc).
		WithEnvVariable("TAPES_E2E_KAFKA_BROKERS", fmt.Sprintf("kafka:%d", KAFKA_PORT)).
		WithEnvVariable("TAPES_E2E_KAFKA_TOPIC", kafkaE2ETopic).
		WithEnvVariable("TAPES_E2E_PROXY_URL", fmt.Sprintf("http://tapes-proxy:%d", TAPES_PROXY_DEFAULT_PORT)).
		WithExec([]string{"go", "test", "-v", "./cmd/tapes/serve/proxy", "-run", "TestProxyKafkaE2E", "-count=1", "-timeout", "3m"}).
		Stdout(ctx)
}
