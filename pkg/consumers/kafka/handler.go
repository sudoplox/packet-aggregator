package kafka

import (
	"errors"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
)

func (kafka *KafkaConsumerConfig) StartConsumer() (any, error) {
	//logger, _ := zap.NewProduction()
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
		fmt.Println("Failed to create consumer, error : ", err.Error())
		return nil, errors.New(fmt.Sprintf("Failed to create consumer, error : ", err.Error()))
	}

	fmt.Println("Created Consumer!, consumer : ", c)

	err = c.SubscribeTopics(kafka.TopicNames, nil)
	if err != nil {
		fmt.Println("Error at subscribing to the topic, Topic : ", kafka.TopicNames, " Error : ", err.Error())
		return nil, errors.New(fmt.Sprint("Error at subscribing to the topic, Topic : ", kafka.TopicNames, " Error : ", err.Error()))
	}

	kafka.consumer = c
	return nil, nil
}

func (kafka *KafkaConsumerConfig) GetOneMessage() (any, error) {
	if kafka.consumer == nil {
		_, err := kafka.StartConsumer()
		if err != nil {
			return nil, err
		}
	}

	for {
		event := kafka.consumer.Poll(kafka.PollTimeoutMs)
		if event == nil {
			continue
		} else {
			switch e := event.(type) {
			case *kafka2.Message:
				return e, nil
			case kafka2.Error:
				return nil, errors.New("Kafka error : " + e.Error())
			case kafka2.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
				continue
			default:
				continue
			}
		}
	}
}

func (kafka *KafkaConsumerConfig) CommitMessages() error {

	//kafka.consumer.Commit()
	var errorString string
	topicPartitions, err := kafka.consumer.Commit()
	if err != nil {
		for _, topicPartition := range topicPartitions {
			errorString += fmt.Sprintf("Error committing offset, Topic : %s Offset : %s Partition : %d", *topicPartition.Topic, topicPartition.Offset.String(), topicPartition.Partition)
			//panic(topicPartition)
		}
		return errors.New(errorString)
	}
	return nil
}

func (kafka *KafkaConsumerConfig) StopConsumer() error {
	err := kafka.consumer.Close()
	if err != nil {
		return errors.New("error while cosing the consumer, error : " + err.Error())
	}
	kafka.consumer = nil
	return err
}
