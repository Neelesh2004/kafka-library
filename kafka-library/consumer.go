package kafkalibrary

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consume continuously polls for Kafka messages and processes them using the handler function.
// It blocks indefinitely unless an error occurs or context is cancelled.
func (r *KafkaProvider) Consume(ctx context.Context, topicHandlers map[string]func(record *kgo.Record)) error {
	// Extract topic names
	var topics []string
	for topic := range topicHandlers {
		topics = append(topics, topic)
	}

	// Subscribe to topics
	r.client.AddConsumeTopics(topics...)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Kafka consumer stopped via context cancellation.")
			return ctx.Err()
		default:
		}

		fetches := r.client.PollFetches(ctx)

		fetches.EachError(func(topic string, partition int32, err error) {
			fmt.Printf("Error fetching from topic %s partition %d: %v\n", topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			if handler, exists := topicHandlers[record.Topic]; exists {
				handler(record)
			} else {
				fmt.Printf("No handler found for topic %s. Skipping message...\n", record.Topic)
			}
		})
	}
}
