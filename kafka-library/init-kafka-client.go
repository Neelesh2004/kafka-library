package kafkalibrary

import (
	"context"
	"fmt"

	"github.com/Neelesh2004/kafka-library/config"
	"github.com/twmb/franz-go/pkg/kgo"
)


type KafkaProvider struct {
	client *kgo.Client
	configurations *config.KafkaProviderConfig
	retries int
	appName string
	producerTracking  bool
}

type KafkaOption func(*KafkaProvider)


// Initialize a new KafkaRepository
func NewKafkaProvider(configurations *config.KafkaProviderConfig, opts ...KafkaOption) (*KafkaProvider, error) {

	brokers := configurations.KafkaConfig.ConnectionString
	// tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		// kgo.SASL(plain.Auth{
		// 	User: configurations.MessageBroker.Username,
		// 	Pass: configurations.MessageBroker.Password,
		// }.AsMechanism()),
		// kgo.Dialer(tlsDialer.DialContext),
	}

	// Add ConsumerGroup option only if provided
	if configurations.KafkaConfig.ConsumerGroup != "" {
		kgoOpts = append(kgoOpts, kgo.ConsumerGroup(configurations.KafkaConfig.ConsumerGroup))
	}

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("kafka broker ping failed: %w", err)
	}

	provider := &KafkaProvider{
		client:        client,
		configurations: configurations,
		producerTracking: true,
	}

	applyOptions(provider, opts...)


	return provider, nil
}


// applyOptions applies all the functional options to the repo
func applyOptions(provider *KafkaProvider, opts ...KafkaOption) {
	for _, opt := range opts {
		opt(provider)
	}
}


func WithPrometheusMetrics() KafkaOption {
	return func(r *KafkaProvider) {
		fmt.Println("Prometheus metrics initialized")

	}
}

func WithRetries(retries int) KafkaOption {
	return func(r *KafkaProvider) {
		r.retries = retries
		fmt.Printf("Retry count set to %d\n", retries)
	}
}

func WithProducerTracking(enabled bool) KafkaOption {
	return func(r *KafkaProvider) {
		r.producerTracking = enabled
		fmt.Printf("Producer tracking enabled: %v\n", enabled)
	}
}

