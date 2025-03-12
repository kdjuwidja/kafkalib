package kafka

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	running bool
	client  *kafka.Consumer
}

type RedisConfig struct {
	Host string
	Port string
}

func NewKafkaConsumer(kafkaConfig *kafka.ConfigMap) (*KafkaConsumer, error) {
	if kafkaConfig == nil {
		return nil, fmt.Errorf("consumer config cannot be nil")
	}

	client, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}

	return &KafkaConsumer{
		running: false,
		client:  client,
	}, nil
}

func (c *KafkaConsumer) Start(topic string, handler func(map[string]interface{}) error) {
	if err := c.client.SubscribeTopics([]string{topic}, nil); err != nil {
		fmt.Printf("Error subscribing to topics: %v\n", err)
		os.Exit(1)
	}
	defer c.client.Close()

	c.running = true
	for c.running {
		ev := c.client.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			go func(msg *kafka.Message) {
				var jsonMsg map[string]interface{}
				if err := json.Unmarshal(msg.Value, &jsonMsg); err != nil {
					fmt.Printf("Error parsing message as JSON: %v\n", err)
					return
				}
				requestId, ok := jsonMsg["requestId"].(string)
				if !ok {
					fmt.Printf("Error: requestId not found or not a string in message\n")
					return
				}

				content, ok := jsonMsg["content"].(map[string]interface{})
				if !ok {
					fmt.Printf("Error: content not found or invalid in message\n")
					return
				}

				fmt.Printf("Processing message with requestId: %s\n", requestId)

				if err := handler(content); err != nil {
					fmt.Printf("Error processing message: %v\n", err)
					// TODO: Need to resubmit the message to the same topic
				} else {
					fmt.Printf("Successfully processed message with requestId: %s\n", requestId)
					if _, err := c.client.CommitMessage(e); err != nil {
						fmt.Printf("Error committing message: %v\n", err)
					}
				}

			}(e)
		case kafka.Error:
			fmt.Printf("Error: %v\n", e)
			if e.Code() == kafka.ErrAllBrokersDown {
				c.running = false
			}
		default:
			fmt.Printf("Ignored event: %v\n", e)
		}
	}
}

func (c *KafkaConsumer) Stop() {
	c.running = false
}
