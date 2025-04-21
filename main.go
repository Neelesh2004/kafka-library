package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Neelesh2004/kafka-library/config"
	"github.com/Neelesh2004/kafka-library/handlers"
	kafkalibrary "github.com/Neelesh2004/kafka-library/kafka-library"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Tenant struct {
	Timestamp         time.Time        `bson:"timestamp" json:"timestamp" validate:"required" example:"2021-07-01T00:00:00Z"`
	Id                string           `bson:"id" json:"id" validate:"required" example:"f6f4c9d8-fc85-4c86-b1bb-decfd27ee9e7"`
}


func main() {

	config := config.KafkaProviderConfig{
		KafkaConfig: config.KafkaConfig{
			ConnectionString: "localhost:9092",
			Username:         "test-user",
			Password:         "test-pass",
			ConsumerGroup:    "my-consumer-group",
		},
	}

	// Initialize Kafka repository
	kafkaRepo, err := kafkalibrary.NewKafkaProvider(&config, 
		kafkalibrary.WithPrometheusMetrics(),
		kafkalibrary.WithRetries(3),
		kafkalibrary.WithProducerTracking(false))

	if err != nil {
		log.Fatalf("Kafka client init error: %v", err)
	}

	defer kafkaRepo.Dispose()

	// Producing(kafkaRepo)
	Consuming(kafkaRepo)

}

func Producing(kafkaRepo *kafkalibrary.KafkaProvider){
	// Create a sample message
	tenant := Tenant{
		Timestamp: time.Now().UTC(),
		Id:        "tenant-31",
	}

	// Produce the message
	if err := kafkaRepo.Produce(context.Background(), tenant, "test-topic-consuming-2", "key-2"); err != nil {
		log.Fatalf("Kafka send error: %v", err)
	}

	fmt.Println("Message sent to Kafka successfully")
}



func Consuming(kafkaRepo *kafkalibrary.KafkaProvider) {
	// Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		fmt.Println("\nShutdown signal received")
		cancel()
	}()

	// Define handlers for each topic
	topicHandlers := map[string]func(record *kgo.Record){
		"test-topic-consuming-1": handlers.HandleTenantEvents,
		"test-topic-consuming-2": handlers.HandleBillingEvents,
	}

	// Start consuming
	go func() {
		err := kafkaRepo.Consume(ctx, topicHandlers)
		if err != nil {
			log.Fatalf("Kafka consume error: %v", err)
		}
	}()

	// Block until context is canceled
	<-ctx.Done()

	// Clean up
	kafkaRepo.Dispose()
	fmt.Println("Kafka client closed")
}
