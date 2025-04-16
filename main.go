package main

import (
	"log"
	"time"

	kafkarepo "github.com/Neelesh2004/kafka-library/kafka-repo"
)

type Tenant struct {
	Timestamp         time.Time        `bson:"timestamp" json:"timestamp" validate:"required" example:"2021-07-01T00:00:00Z"`
	Id                string           `bson:"id" json:"id" validate:"required" example:"f6f4c9d8-fc85-4c86-b1bb-decfd27ee9e7"`
   }

func main() {
	// Initialize Kafka repository
	kafkaRepo, err := kafkarepo.NewKafkaRepository()
	if err != nil {
		log.Fatalf("Kafka client init error: %v", err)
	}

	// Create a sample message
	tenant := Tenant{
		Timestamp: time.Now().UTC(),
		Id:        "tenant-128",
	}

	// Produce the message
	if err := kafkaRepo.Produce(tenant, "test-topic", "key-1"); err != nil {
		log.Fatalf("Kafka send error: %v", err)
	}

	log.Println("Message sent to Kafka successfully")
}