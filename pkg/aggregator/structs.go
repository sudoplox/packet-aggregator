package aggregator

import (
	"time"
)

type ConsumerConfig struct {
	TopicNames    []string
	PollTimeoutMs int

	BootstrapServers               []string `json:"bootstrap.servers"`
	GroupId                        string   `json:"group.id"`
	AutoOffsetReset                string   `json:"auto.offset.reset"`
	HeartbeatIntervalMs            int      `json:"heartbeat.interval.ms"`
	SessionTimeoutMs               int      `json:"session.timeout.ms"`
	TopicMetadataRefreshIntervalMs int      `json:"topic.metadata.refresh.interval.ms"`
	PartitionAssignmentStrategy    string   `json:"partition.assignment.strategy"`
	EnableAutoCommit               bool     `json:"enable.auto.commit"`
	MaxPollIntervalMs              int      `json:"max.poll.interval.ms"`
}

type ProducerConfig struct {
	BootstrapServers []string `json:"bootstrap.servers"`
	TopicName        string
}

type AggrConfig struct {
	TimeDuration time.Duration
	MessageCount int

	// Kafka Consumer Config
	ConsumerConfig ConsumerConfig
}

type AggrObject[K comparable, V any] struct {
	KeyValueExtractor[K, V]
	Delegator[K, V]
	RetryHandler[K, V]
	// DLQHandler : Dead Letter Queue implementation
	DLQHandler[K, V]
}

type KeyValueExtractor[K comparable, V any] interface {
	Extract(any) (K, V, error)
}
type Delegator[K comparable, V any] interface {
	Delegate(any) error
}
type RetryHandler[K comparable, V any] interface {
	RetryHandle(any) error
}
type DLQHandler[K comparable, V any] interface {
	DLQHandle(any) error
}
