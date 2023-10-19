package producer

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"packet-aggregator/pkg/aggregator"
	"strings"
)

func PushMessage(producerConfig aggregator.ProducerConfig, msg []byte) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(producerConfig.BootstrapServers, ","), // Replace with your Kafka broker address.
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	defer p.Close()

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &producerConfig.TopicName, Partition: kafka.PartitionAny},
		Value:          msg,
	}

	// Produce the message to the Kafka topic.
	if err := p.Produce(message, nil); err != nil {
		fmt.Printf("Failed to produce message to Kafka: %s\n", err)
	}

	// Wait for any outstanding messages to be delivered and delivery reports to be received.
	p.Flush(15 * 1000) // 15 seconds timeout (adjust as needed).
}
