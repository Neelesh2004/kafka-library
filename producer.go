package kafkarepo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

//Produces a message using an existing Kafka client
func (r *KafkaRepository) Produce(data interface{}, topic string, key string) error {
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
		Headers: []kgo.RecordHeader{
			{Key: "Test-Header", Value: []byte("Sent from Test-Application")},
		},
	}

	if err := r.client.ProduceSync(context.Background(), kafkaRecord).FirstErr(); err != nil {
		fmt.Printf("Failed to produce record: %v\n", err)
		return err
	}

	fmt.Printf("Published the message to topic %v\n", topic)

	return nil
}
