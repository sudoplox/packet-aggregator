package kafka

import kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type KafkaConsumerConfig struct {
	TopicNames                     []string
	PollTimeoutMs                  int
	BootstrapServers               []string `json:"bootstrap.servers"`
	GroupId                        string   `json:"group.id"`
	AutoOffsetReset                string   `json:"auto.offset.reset"`
	HeartbeatIntervalMs            int      `json:"heartbeat.interval.ms"`
	SessionTimeoutMs               int      `json:"session.timeout.ms"`
	TopicMetadataRefreshIntervalMs int      `json:"topic.metadata.refresh.interval.ms"`
	PartitionAssignmentStrategy    string   `json:"partition.assignment.strategy"`
	EnableAutoCommit               bool     `json:"enable.auto.commit"`
	MaxPollIntervalMs              int      `json:"max.poll.interval.ms"`
	consumer                       *kafka2.Consumer
}
