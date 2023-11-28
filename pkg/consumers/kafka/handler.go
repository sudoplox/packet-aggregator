package kafka

import (
	"errors"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"strings"
	"time"
)

// Split the below function to new and start
func (kafka *KafkaConsumerConfig) StartConsumer(logger *zap.Logger) error {
	c, err := kafka2.NewConsumer(&kafka2.ConfigMap{
		"bootstrap.servers":                  strings.Join(kafka.BootstrapServers, ","), // Kafka broker address
		"group.id":                           kafka.GroupId,                             // Consumer group ID
		"auto.offset.reset":                  kafka.AutoOffsetReset,                     // Start consuming from the beginning of topics
		"heartbeat.interval.ms":              kafka.HeartbeatIntervalMs,                 // For consumer to specify it's alive to the broker
		"session.timeout.ms":                 kafka.SessionTimeoutMs,                    // If the broker doesn't receive heartbeat request from consumer within this timeout, it will delete the consumer and trigger rebalance
		"topic.metadata.refresh.interval.ms": kafka.TopicMetadataRefreshIntervalMs,      // At this interval, topic and broker data are refreshed. (Any new leaders, new brokers were picked up)
		"partition.assignment.strategy":      kafka.PartitionAssignmentStrategy,         // Strategy for assigning partitions to consumers
		"enable.auto.commit":                 kafka.EnableAutoCommit,                    // Disable automatic offset committing
		"max.poll.interval.ms":               kafka.MaxPollIntervalMs,                   // Maximum time between polls (10 minutes)
	})

	if err != nil {
		logger.Error("Failed to create consumer, error : " + err.Error())
		return errors.New("Failed to create consumer, error : " + err.Error())
	}

	logger.Info("Created Consumer!, consumer : " + c.String())

	err = c.SubscribeTopics(kafka.TopicNames, nil)
	if err != nil {
		logger.Error("Error at subscribing to the topic, Topic : " + strings.Join(kafka.TopicNames, ",") + " Error : " + err.Error())
		return errors.New(fmt.Sprint("Error at subscribing to the topic, Topic : ", kafka.TopicNames, " Error : ", err.Error()))
	}

	kafka.consumer = c
	return nil
}

func (kafka *KafkaConsumerConfig) GetMessages(logger *zap.Logger, size int, time *time.Timer) (any, error) {
	messages := make([]*kafka2.Message, 0)
	for {
		select {
		case <-time.C:
			if len(messages) > 0 {
				return messages, nil
			} else {
				return nil, nil
			}
		default:
			event := kafka.consumer.Poll(kafka.PollTimeoutMs)
			if event == nil {
				if len(messages) > 0 {
					return messages, nil
				}
			} else {
				switch e := event.(type) {
				case *kafka2.Message:
					messages = append(messages, e)
					if size >= len(messages) {
						return messages, nil
					} else {
						continue
					}
				case kafka2.Error:
					logger.Error("kafka error : " + e.Error())
					return nil, errors.New("Kafka error : " + e.Error())
				case kafka2.PartitionEOF:
					logger.Error("%% Reached %v" + e.String())
					continue
				default:
					continue
				}
			}
		}
	}
}

func (kafka *KafkaConsumerConfig) CommitMessages(logger *zap.Logger) error {
	var errorString string
	topicPartitions, err := kafka.consumer.Commit()
	if err != nil {
		for _, topicPartition := range topicPartitions {
			logger.Error(fmt.Sprintf("Error committing offset, Topic : %s Offset : %s Partition : %d", *topicPartition.Topic, topicPartition.Offset.String(), topicPartition.Partition))
			errorString += fmt.Sprintf("Error committing offset, Topic : %s Offset : %s Partition : %d", *topicPartition.Topic, topicPartition.Offset.String(), topicPartition.Partition)
		}
		return errors.New(errorString)
	}
	return nil
}

func (kafka *KafkaConsumerConfig) StopConsumer(logger *zap.Logger) error {
	err := kafka.consumer.Close()
	if err != nil {
		logger.Error("error while closing the consumer, error : " + err.Error())
		return errors.New("error while closing the consumer, error : " + err.Error())
	}
	kafka.consumer = nil
	return err
}
