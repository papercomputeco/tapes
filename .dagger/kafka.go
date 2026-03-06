package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

const (
	kafkaUIPort       = 9091
	kafkaPort         = 9092
	kafkaInternalPort = 29092

	// kafkaE2ETopic is the default topic created for e2e proxy testing.
	kafkaE2ETopic = "tapes.e2e.proxy"
)

// KafkaStack starts Kafka, creates the e2e topic, and returns the Kafka UI
// dashboard service. Use "dagger call kafka-stack up" to bring up the full stack.
func (m *Tapes) KafkaStack(ctx context.Context) (*dagger.Service, error) {
	kafkaSvc := m.kafkaService()
	kafkaSvc, err := kafkaSvc.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka service: %w", err)
	}

	// Create the e2e topic using a sidecar container.
	_, err = m.kafkaCreateTopic(ctx, kafkaE2ETopic, kafkaSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka topic %s: %w", kafkaE2ETopic, err)
	}

	return m.kafkaUIService(kafkaSvc), nil
}

// kafkaService provides a ready to run Kafka service using the Confluent local image.
// The broker listens on port 9092 (external) and 29092 (internal).
func (m *Tapes) kafkaService() *dagger.Service {
	advertisedListeners := fmt.Sprintf(
		"PLAINTEXT://localhost:%d,PLAINTEXT_HOST://kafka:%d",
		kafkaInternalPort, kafkaPort,
	)

	return dag.Container().
		From("confluentinc/confluent-local:latest").
		WithEnvVariable("KAFKA_ADVERTISED_LISTENERS", advertisedListeners).
		WithExposedPort(kafkaPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

// kafkaCreateTopic creates a topic on a running Kafka service using a sidecar container.
func (m *Tapes) kafkaCreateTopic(ctx context.Context, topic string, kafkaSvc *dagger.Service) (string, error) {
	return dag.Container().
		From("confluentinc/confluent-local:latest").
		WithServiceBinding("kafka", kafkaSvc).
		WithExec([]string{
			"kafka-topics",
			"--bootstrap-server", fmt.Sprintf("kafka:%d", kafkaPort),
			"--create",
			"--if-not-exists",
			"--topic", topic,
			"--partitions", "1",
			"--replication-factor", "1",
		}).
		Stdout(ctx)
}

// kafkaUIService provides a Kafka UI dashboard service bound to a running Kafka instance.
// The dashboard is exposed on port 8081.
func (m *Tapes) kafkaUIService(kafkaSvc *dagger.Service) *dagger.Service {
	return dag.Container().
		From("provectuslabs/kafka-ui:latest").
		WithServiceBinding("kafka", kafkaSvc).
		WithEnvVariable("SERVER_PORT", fmt.Sprintf("%d", kafkaUIPort)).
		WithEnvVariable("DYNAMIC_CONFIG_ENABLED", "true").
		WithEnvVariable("KAFKA_CLUSTERS_0_NAME", "local").
		WithEnvVariable("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", fmt.Sprintf("kafka:%d", kafkaPort)).
		WithExposedPort(kafkaUIPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}
