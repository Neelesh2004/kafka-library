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

	fmt.Println("Creating new kafka repository")
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
