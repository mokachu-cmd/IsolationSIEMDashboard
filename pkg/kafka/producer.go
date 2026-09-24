package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// NewProducer initializes the Kafka writer client
func NewProducer(brokerAddr string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Distributes across partitions based on event key
		BatchSize:    100,           // Batch up to 100 messages before flushing
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,          // Non-blocking writes for high throughput
	}

	return &Producer{
		writer: writer,
	}
}

// PublishEvent pushes a serialized ECS event JSON message to Kafka
func (p *Producer) PublishEvent(ctx context.Context, key string, message []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: message,
		Time:  time.Now(),
	})

	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// Close gracefully closes the producer connection
func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		log.Printf("Error closing Kafka writer: %v", err)
	}
}