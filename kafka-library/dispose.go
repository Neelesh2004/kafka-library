package kafkalibrary

import "fmt"

func (r *KafkaProvider) Dispose() {
	r.client.Close()
	fmt.Println("Disposed the Kafka Client")
}