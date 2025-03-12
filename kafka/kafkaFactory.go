package kafka

import (
	"fmt"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaFactory creates and manages Kafka producers and consumers
type KafkaFactory struct {
	config *kafka.ConfigMap
}

var (
	factory *KafkaFactory
	once    sync.Once
)

// GetKafkaFactory returns the singleton factory instance
func GetKafkaFactory() (*KafkaFactory, error) {
	var initErr error
	once.Do(func() {
		factory, initErr = initFactory()
	})
	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize kafka factory: %v", initErr)
	}
	return factory, nil
}

// initFactory creates and configures a new factory instance
func initFactory() (*KafkaFactory, error) {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "kafka:29092" // fallback default
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",    // Strongest delivery guarantee
		"retries":           3,        // Retry a few times before giving up
		"retry.backoff.ms":  100,      // Wait 100ms between retries
		"linger.ms":         5,        // Wait up to 5ms for batching
		"compression.type":  "snappy", // Use Snappy compression
	}

	return &KafkaFactory{
		config: config,
	}, nil
}

// CreateConsumer creates a new KafkaConsumer instance
func (f *KafkaFactory) CreateConsumer(groupId string) (*KafkaConsumer, error) {
	consumerConfig := *f.config
	consumerConfig["group.id"] = groupId
	consumerConfig["auto.offset.reset"] = "earliest"
	consumerConfig["enable.auto.commit"] = true

	consumer, err := NewKafkaConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %s", err)
	}

	return consumer, nil
}

// CreateProducer creates a new KafkaProducer instance
func (f *KafkaFactory) CreateProducer() (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %s", err)
	}

	return NewKafkaProducer(producer), nil
}

// CreateProducerWithDeliveryChannel creates a new producer with a delivery channel
func (f *KafkaFactory) CreateProducerWithDeliveryChannel() (*kafka.Producer, chan kafka.Event, error) {
	producer, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create producer: %s", err)
	}

	deliveryChan := make(chan kafka.Event, 100)
	return producer, deliveryChan, nil
}
