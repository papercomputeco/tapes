// Package kafka provides a Kafka-backed Publisher implementation.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	skafka "github.com/segmentio/kafka-go"

	basepublisher "github.com/papercomputeco/tapes/pkg/publisher"
)

const (
	defaultPublishTimeout = 5 * time.Second
)

var (
	errMissingBrokers = errors.New("kafka brokers are required")
	errMissingTopic   = errors.New("kafka topic is required")
	errNilEvent       = errors.New("event is required")
)

// Message is the writer message type used by this publisher.
type Message = skafka.Message

// Config configures a Kafka publisher.
type Config struct {
	Brokers        []string
	Topic          string
	ClientID       string
	PublishTimeout time.Duration
}

type writer interface {
	WriteMessages(ctx context.Context, msgs ...Message) error
	Close() error
}

// Publisher publishes node events to Kafka.
type Publisher struct {
	writer         writer
	publishTimeout time.Duration
}

// Ensure interface compatibility.
var _ basepublisher.Publisher = (*Publisher)(nil)

// NewPublisher creates a Kafka publisher using the provided config.
func NewPublisher(c Config) (*Publisher, error) {
	if len(c.Brokers) == 0 {
		return nil, errMissingBrokers
	}
	if c.Topic == "" {
		return nil, errMissingTopic
	}

	kw := &skafka.Writer{
		Addr:     skafka.TCP(c.Brokers...),
		Topic:    c.Topic,
		Balancer: &skafka.Hash{},
	}

	if c.ClientID != "" {
		kw.Transport = &skafka.Transport{
			ClientID: c.ClientID,
		}
	}

	return newPublisherWithWriter(c, kw)
}

func newPublisherWithWriter(c Config, w writer) (*Publisher, error) {
	if c.Topic == "" {
		return nil, errMissingTopic
	}
	if w == nil {
		return nil, errors.New("writer is required")
	}

	timeout := c.PublishTimeout
	if timeout <= 0 {
		timeout = defaultPublishTimeout
	}

	return &Publisher{
		writer:         w,
		publishTimeout: timeout,
	}, nil
}

// Publish publishes a single event to Kafka.
func (p *Publisher) Publish(ctx context.Context, event *basepublisher.Event) error {
	if event == nil {
		return errNilEvent
	}
	if event.RootHash == "" {
		return basepublisher.ErrEmptyRootHash
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	err = p.writer.WriteMessages(publishCtx, Message{
		Key:   []byte(event.RootHash),
		Value: value,
		Time:  event.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	return nil
}

// Close closes the underlying writer.
func (p *Publisher) Close() error {
	return p.writer.Close()
}
