package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

const (
	KAFKA_UI_PORT       = 9091
	KAFKA_PORT          = 9092
	KAFKA_INTERNAL_PORT = 29092

	// kafkaE2ETopic is the default topic created for e2e proxy testing.
	kafkaE2ETopic = "tapes.e2e.proxy"
)

// KafkaStack starts Kafka, creates the e2e topic, starts the Kafka UI dashboard service
// and returns both services. Use "dagger call kafka-stack up" to bring up the full stack.
func (t *Tapes) KafkaStack(ctx context.Context) (*dagger.Service, error) {
	kafkaSvc := kafkaService()
	kafkaSvc, err := kafkaSvc.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka service: %w", err)
	}

	// Create the e2e topic using a sidecar container.
	_, err = kafkaCreateTopic(ctx, kafkaE2ETopic, kafkaSvc)
	if err != nil {
		kafkaSvc.Stop(ctx)
		return nil, fmt.Errorf("failed to create kafka topic %s: %w", kafkaE2ETopic, err)
	}

	return kafkaSvc, nil
}

// kafkaService provides a ready to run Kafka service using the Confluent local image.
// The broker listens on port 9092 (external) and 29092 (internal).
func kafkaService() *dagger.Service {
	advertisedListeners := fmt.Sprintf(
		"PLAINTEXT://localhost:%d,PLAINTEXT_HOST://kafka:%d",
		KAFKA_INTERNAL_PORT, KAFKA_PORT,
	)

	return dag.Container().
		From("confluentinc/confluent-local:latest").
		WithEnvVariable("KAFKA_ADVERTISED_LISTENERS", advertisedListeners).
		WithExposedPort(KAFKA_PORT).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

// kafkaCreateTopic creates a topic on a running Kafka service using a sidecar container.
func kafkaCreateTopic(ctx context.Context, topic string, kafkaSvc *dagger.Service) (string, error) {
	return dag.Container().
		From("confluentinc/confluent-local:latest").
		WithServiceBinding("kafka", kafkaSvc).
		WithExec([]string{
			"kafka-topics",
			"--bootstrap-server", fmt.Sprintf("kafka:%d", KAFKA_PORT),
			"--create",
			"--if-not-exists",
			"--topic", topic,
			"--partitions", "1",
			"--replication-factor", "1",
		}).
		Stdout(ctx)
}

// KafkaUIStack starts the Kafka UI dashboard service
// and returns its services. Use "dagger call kafka-ui-stack up" to bring up the UI stack.
func (t *Tapes) KafkaUIStack(ctx context.Context, kafkaSvc *dagger.Service) (*dagger.Service, error) {
	kafkaUISvc := kafkaUIService(kafkaSvc)
	return kafkaUISvc.Start(ctx)
}

// kafkaUIService provides a Kafka UI dashboard service bound to a running Kafka instance.
// The dashboard is exposed on port 8081.
func kafkaUIService(kafkaSvc *dagger.Service) *dagger.Service {
	return dag.Container().
		From("provectuslabs/kafka-ui:latest").
		WithServiceBinding("kafka", kafkaSvc).
		WithEnvVariable("SERVER_PORT", fmt.Sprintf("%d", KAFKA_UI_PORT)).
		WithEnvVariable("DYNAMIC_CONFIG_ENABLED", "true").
		WithEnvVariable("KAFKA_CLUSTERS_0_NAME", "local").
		WithEnvVariable("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", fmt.Sprintf("kafka:%d", KAFKA_PORT)).
		WithExposedPort(KAFKA_UI_PORT).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}
