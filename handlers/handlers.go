package handlers

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func HandleTenantEvents(record *kgo.Record) {
	fmt.Println("Tenant Event:")
	fmt.Printf("Topic: %s | Partition: %d | Offset: %d\n", record.Topic, record.Partition, record.Offset)
	fmt.Printf("Key: %s\n", string(record.Key))
	fmt.Printf("Value: %s\n\n", string(record.Value))
}

func HandleBillingEvents(record *kgo.Record) {
	fmt.Println("Billing Event:")
	fmt.Printf("Topic: %s | Partition: %d | Offset: %d\n", record.Topic, record.Partition, record.Offset)
	fmt.Printf("Key: %s\n", string(record.Key))
	fmt.Printf("Value: %s\n\n", string(record.Value))
}