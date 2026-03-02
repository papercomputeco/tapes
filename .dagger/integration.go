package main

import "context"

// TestKafkaE2E runs the opt-in proxy Kafka e2e test suite against a local
// Confluent Local Kafka service.
func (t *Tapes) TestKafkaE2E(ctx context.Context) (string, error) {
	const kafkaImg = "confluentinc/confluent-local:latest"
	kafka := dag.Container().
		From(kafkaImg).
		WithEnvVariable(
			"KAFKA_ADVERTISED_LISTENERS",
			"PLAINTEXT://localhost:29092,PLAINTEXT_HOST://kafka:9092",
		).
		WithExposedPort(9092).
		AsService()

	if _, err := dag.Container().
		From(kafkaImg).
		WithServiceBinding("kafka", kafka).
		WithExec([]string{
			"kafka-topics",
			"--bootstrap-server", "kafka:9092",
			"--create",
			"--if-not-exists",
			"--topic", "tapes.e2e.proxy",
			"--partitions", "1",
			"--replication-factor", "1",
		}).
		Stdout(ctx); err != nil {
		return "", err
	}

	return t.goContainer().
		WithServiceBinding("kafka", kafka).
		WithEnvVariable("TAPES_E2E_KAFKA_BROKERS", "kafka:9092").
		WithEnvVariable("TAPES_E2E_KAFKA_TOPIC", "tapes.e2e.proxy").
		WithExec([]string{"go", "test", "-v", "./cmd/tapes/serve/proxy", "-run", "TestProxyKafkaE2E", "-count=1", "-timeout", "3m"}).
		Stdout(ctx)
}
