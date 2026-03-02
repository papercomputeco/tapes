package main

import "context"

// TestKafkaE2E runs the opt-in proxy Kafka e2e test suite against a local
// Redpanda (Kafka-compatible) service.
func (t *Tapes) TestKafkaE2E(ctx context.Context) (string, error) {
	redpanda := dag.Container().
		From("redpandadata/redpanda:v25.3.9").
		WithDefaultArgs([]string{
			"/usr/bin/rpk", "redpanda", "start",
			"--overprovisioned",
			"--smp", "1",
			"--memory", "512M",
			"--reserve-memory", "0M",
			"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
			"--advertise-kafka-addr", "PLAINTEXT://redpanda:9092",
		}).
		WithExposedPort(9092).
		AsService()

	if _, err := dag.Container().
		From("redpandadata/redpanda:v25.3.9").
		WithServiceBinding("redpanda", redpanda).
		WithExec([]string{
			"/usr/bin/rpk", "topic", "create", "tapes.e2e.proxy",
			"--brokers", "redpanda:9092",
			"--if-not-exists",
		}).
		Stdout(ctx); err != nil {
		return "", err
	}

	return t.goContainer().
		WithServiceBinding("redpanda", redpanda).
		WithEnvVariable("TAPES_E2E_KAFKA_BROKERS", "redpanda:9092").
		WithEnvVariable("TAPES_E2E_KAFKA_TOPIC", "tapes.e2e.proxy").
		WithExec([]string{"go", "test", "-v", "./cmd/tapes/serve/proxy", "-run", "TestProxyKafkaE2E", "-count=1", "-timeout", "3m"}).
		Stdout(ctx)
}
