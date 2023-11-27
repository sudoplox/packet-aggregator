package aggregator

import (
	"go.uber.org/zap"
	"time"
)

type RmqConsumerConfig struct {
	BootstrapServer string
	QueueName       string
	ConsumerAutoAck bool
}

type ProducerConfig struct {
	BootstrapServers []string `json:"bootstrap.servers"`
	TopicName        string
}

type AggregationConfig struct {
	TimeDuration time.Duration
	MessageCount int
	Logger       *zap.Logger
}

type AggrObject[K comparable, V any] struct {
	KeyValueExtractor[K, V]
	Delegator[K, V]
	RetryHandler[K, V]
	// DLQHandler : Dead Letter Queue implementation
	DLQHandler[K, V]
	AggregatorConfig
	Consumer
}

type Consumer interface {
	StartConsumer() (any, error)
	GetOneMessage() (any, error)
	GetMessages(int) (any, error)
	CommitMessages() error
	StopConsumer() error
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

type AggregatorConfig interface {
	GetAggregatorConfig() AggregationConfig
	CreateConsumerFromConfig() (Consumer, error)
	GetLogger() *zap.Logger
}

type Aggregator interface {
	Start() error
	Stop() error
}
