package helpers

import (
	"packet-aggregator/pkg/consumers/kafka"
)

func FormKafkaConsumer(topicNames []string,
	bootStrapServers []string,
	pollTimeOutMs int,
	groupId string,
	autoOffsetReset string,
	heartBeatIntervalMs int,
	sessionTimeOutMs int,
	topicMetadataRefreshIntervalMs int,
	partitionAssignmentStrategy string,
	enableAutoCommit bool,
	maxPollIntervalMs int) *kafka.KafkaConsumerConfig {
	return &kafka.KafkaConsumerConfig{
		TopicNames:                     topicNames,
		PollTimeoutMs:                  pollTimeOutMs,
		BootstrapServers:               bootStrapServers,
		GroupId:                        groupId,
		AutoOffsetReset:                autoOffsetReset,
		HeartbeatIntervalMs:            heartBeatIntervalMs,
		SessionTimeoutMs:               sessionTimeOutMs,
		TopicMetadataRefreshIntervalMs: topicMetadataRefreshIntervalMs,
		PartitionAssignmentStrategy:    partitionAssignmentStrategy,
		EnableAutoCommit:               enableAutoCommit,
		MaxPollIntervalMs:              maxPollIntervalMs,
	}
}
