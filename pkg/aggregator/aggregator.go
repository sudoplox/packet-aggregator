package aggregator

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func CreateAggregator[K comparable, V any](extract KeyValueExtractor[K, V], delegate Delegator[K, V], retry RetryHandler[K, V], handle DLQHandler[K, V]) AggrObject[K, V] {
	return AggrObject[K, V]{
		extract,
		delegate,
		retry,
		handle,
	}
}

func (agg AggrObject[K, V]) StartKafkaConsumer(aggrConfig AggrConfig) {

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(aggrConfig.ConsumerConfig.BootstrapServers, ","), // Kafka broker address
		"group.id":                           aggrConfig.ConsumerConfig.GroupId,                             // Consumer group ID
		"auto.offset.reset":                  aggrConfig.ConsumerConfig.AutoOffsetReset,                     // Start consuming from the beginning of topics
		"heartbeat.interval.ms":              aggrConfig.ConsumerConfig.HeartbeatIntervalMs,                 // For consumer to specify it's alive to the broker
		"session.timeout.ms":                 aggrConfig.ConsumerConfig.SessionTimeoutMs,                    // If the broker doesn't receive heartbeat request from consumer within this timeout, it will delete the consumer and trigger rebalance
		"topic.metadata.refresh.interval.ms": aggrConfig.ConsumerConfig.TopicMetadataRefreshIntervalMs,      // At this interval, topic and broker data are refreshed. (Any new leaders, new brokers were picked up)
		"partition.assignment.strategy":      aggrConfig.ConsumerConfig.PartitionAssignmentStrategy,         // Strategy for assigning partitions to consumers
		"enable.auto.commit":                 aggrConfig.ConsumerConfig.EnableAutoCommit,                    // Disable automatic offset committing
		"max.poll.interval.ms":               aggrConfig.ConsumerConfig.MaxPollIntervalMs,                   // Maximum time between polls (10 minutes)
	})

	if err != nil {
		logger.Error("Failed to create consumer!",
			zap.String("Error", err.Error()),
		)
		return
	}

	logger.Info("Created Consumer!",
		zap.Any("Consumer", c),
	)

	err = c.SubscribeTopics(aggrConfig.ConsumerConfig.TopicNames, nil)
	if err != nil {
		logger.Error("Error at subscribing to the topic",
			zap.Any("Topic", aggrConfig.ConsumerConfig.TopicNames),
			zap.String("Error", err.Error()),
		)
		panic(err)
	}

	messagesMap := make(map[K][]V)
	timer := time.NewTimer(aggrConfig.TimeDuration)

	commitAndReset := func() {
		topicPartitions, err := c.Commit()
		if err != nil {
			for _, topicPartition := range topicPartitions {
				logger.Error("Error committing offset",
					zap.Any("Topic", topicPartition.Topic),
					zap.Any("Offset", topicPartition.Offset),
					zap.Any("Partition", topicPartition.Partition),
				)
			}
		}
		timer.Reset(aggrConfig.TimeDuration)
		messagesMap = make(map[K][]V)
	}

	executeAggr := func() {
		err := agg.Delegator.Delegate(messagesMap)
		if err != nil {
			logger.Error("Error calling Delegate",
				zap.String("Error", err.Error()),
				zap.Any("Message Map", messagesMap),
			)

			err := agg.RetryHandler.RetryHandle(messagesMap)
			if err != nil {
				logger.Error("Error calling RetryHandle",
					zap.String("Error", err.Error()),
					zap.Any("Message Map", messagesMap),
				)

				err := agg.DLQHandler.DLQHandle(messagesMap)
				if err != nil {
					logger.Error("Error calling DLQHandle",
						zap.String("Error", err.Error()),
						zap.Any("Message Map", messagesMap),
					)
				}
				commitAndReset()

			} else {
				commitAndReset()
			}
		} else {
			commitAndReset()
		}
	}

	run := true
	for run {
		select {
		case <-timer.C:
			// Time-based aggregation logic
			if len(messagesMap) > 0 {
				executeAggr()
			}
		case sig := <-sigchan:
			logger.Info("Terminating",
				zap.Any("Caught Signal", sig),
			)
			run = false
		default:
			ev := c.Poll(aggrConfig.ConsumerConfig.PollTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				key, value, err := agg.KeyValueExtractor.Extract(e)
				if err != nil {
					// TODO Handle key value extractor failure
					logger.Error("Error while running KeyValueExtractor",
						zap.String("Error", err.Error()),
					)
					continue
				}
				messagesMap[key] = append(messagesMap[key], value)

				if len(messagesMap) >= aggrConfig.MessageCount {
					executeAggr()
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				logger.Error("Kafka Error",
					zap.Any("Error", e.Error()),
					zap.Any("Error Code", e.Code()),
					zap.Any("Event", e),
				)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				logger.Warn("Ignored",
					zap.Any("Event", e),
				)
			}
		}
	}
	logger.Info("Closing Consumer!",
		zap.Any("Consumer", c),
	)
	err = c.Close()
	if err != nil {
		logger.Error("Error Closing Consumer!")
	}
}
