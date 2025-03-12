package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaProducer handles Kafka message production
type KafkaProducer struct {
	producer     *kafka.Producer
	deliveryChan chan kafka.Event
}

// NewKafkaProducer creates a new KafkaProducer with a producer and delivery channel
func NewKafkaProducer(producer *kafka.Producer) *KafkaProducer {
	return &KafkaProducer{
		producer:     producer,
		deliveryChan: make(chan kafka.Event, 100),
	}
}

// Close cleans up the Kafka producer resources
func (k *KafkaProducer) Close() {
	close(k.deliveryChan)
	k.producer.Close()
}

// ProduceMessage sends a message to the specified Kafka topic synchronously
func (k *KafkaProducer) ProduceMessage(topic string, message []byte) error {
	// Send the message
	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, k.deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to produce message: %s", err)
	}

	// Wait for delivery report
	e := <-k.deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	return nil
}
