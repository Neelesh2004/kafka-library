package kafkarepo

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)


type KafkaRepository struct {
	client *kgo.Client
}


// Initialize a new KafkaRepository
func NewKafkaRepository() (*KafkaRepository, error) {
	brokers := "localhost:9092"

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("kafka broker ping failed: %w", err)
	}

	return &KafkaRepository{client: client}, nil
}


// //Produces a message using an existing Kafka client
// func Produce(client *kgo.Client, data interface{}, topic string, key string) error {
// 	recordData, err := json.Marshal(data)
// 	if err != nil {
// 		fmt.Printf("Failed to marshal request to JSON: %v\n", err)
// 		return err
// 	}

// 	kafkaRecord := &kgo.Record{
// 		Topic:     topic,
// 		Key:       []byte(key),
// 		Value:     recordData,
// 		Timestamp: time.Now().UTC(),
// 		Headers: []kgo.RecordHeader{
// 			{Key: "Test-Header", Value: []byte("Sent from Test-Application")},
// 		},
// 	}

// 	if err := client.ProduceSync(context.Background(), kafkaRecord).FirstErr(); err != nil {
// 		fmt.Printf("Failed to produce record: %v\n", err)
// 		return err
// 	}

// 	fmt.Printf("Published the message to topic %v\n", topic)

// 	return nil
// }
