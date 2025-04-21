package kafkalibrary

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

//Produces a message using an existing Kafka client
func (r *KafkaProvider) Produce(ctx context.Context, data interface{}, topic string, key string) error {
	recordData, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Failed to marshal request to JSON: %v\n", err)
		return err
	}

	kafkaRecord := &kgo.Record{
		Topic:     topic,
		Key:       []byte(key),
		Value:     recordData,
		Timestamp: time.Now().UTC(),
	}

	r.applyProducerTracking(kafkaRecord)

	maxRetries := r.retries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := r.client.ProduceSync(context.Background(), kafkaRecord).FirstErr()
		if err == nil {
			fmt.Printf("Published message to topic %v\n", topic)
			return nil
		}
		fmt.Printf("Attempt %d failed to produce: %v\n", attempt, err)
	}

	return fmt.Errorf("failed to produce message to topic %s after %d retries", topic, maxRetries)
}


func (r *KafkaProvider) applyProducerTracking(record *kgo.Record) {
	if r.producerTracking {
		record.Headers = []kgo.RecordHeader{
			{Key: "Test-Header", Value: []byte("Sent from Test-Application")},
		}
	}
}
