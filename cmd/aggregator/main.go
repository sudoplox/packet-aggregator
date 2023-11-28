package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"packet-aggregator/pkg/aggregator"
<<<<<<< HEAD
	kafka2 "packet-aggregator/pkg/consumers/kafka"
	"packet-aggregator/pkg/consumers/rabbitmq"
=======
	"packet-aggregator/pkg/consumers/helpers"
>>>>>>> 06bcdb0 (Minor fixes)
	avroHelpers "packet-aggregator/pkg/helpers/avro"
	"syscall"
	"time"
)

func main() {
	server := []string{"kafka:9092"}
	schemaFile := "/Users/mmt9761/Code/Go/src/awesomeAggregator/temp/packet-aggregator/template.avsc"
	codec, err := avroHelpers.GetCodec(schemaFile)
	if err != nil {
		return
	}

	z := zap.NewDevelopmentConfig()
	z.InitialFields = map[string]interface{}{
		"package": "aggregator",
	}
	logger, _ := z.Build()
	kafkaConsumer := helpers.FormKafkaConsumer([]string{"aggregator_test"}, server, 10, "aggregator-test-1", "earliest", 3000, 30000, 36000, "range", false, 600000)
	aggr, err := aggregator.CreateAggregator[string, string](
		KeyValueExtractorStruct1[string, string]{
			codec: codec,
			key:   "name1",
			value: "name2",
		},
		DelegatorStruct[string, string]{},
		RetryHandlerStruct[string, string]{},
		DLQHandlerStruct[string, string]{},
		AggregatorConfigStruct{
			TimeDuration: 4 * time.Minute,
			MessageCount: 10,
			Logger:       logger,

			ConsumerType: "kafka",
			Consumer:     kafkaConsumer,
		},
	)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	err = aggr.Start()
	if err != nil {

	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func(sigchan chan os.Signal) {
		for {
			select {
			case _ = <-sigchan:
				err := aggr.Stop()
				panic(err)
			}
		}
	}(sigchan)

}

type KeyValueExtractorStruct[K string, V string] struct {
	codec *goavro.Codec
	key   string
	value string
}

type KeyValueExtractorStruct1[K string, V string] struct {
	codec *goavro.Codec
	key   string
	value string
}

type DelegatorStruct[K string, V string] struct {
}
type DLQHandlerStruct[K string, V string] struct {
}
type RetryHandlerStruct[K string, V string] struct {
}
type RetryHandlerStructNew[K string, V string] struct {
}
type AggregatorConfigStruct struct {
	TimeDuration time.Duration
	MessageCount int
	Logger       *zap.Logger
<<<<<<< HEAD
	ConsumerType string // "Kafka" or "Rabbitmq" or anything else specify
	//Kafka
	KafkaTopicNames                     []string
	KafkaBootStrapServers               []string
	KafkaPollTimeOutMs                  int
	KafkaGroupId                        string
	KafkaAutoOffsetReset                string
	KafkaHeartBeatIntervalMs            int
	KafkaSessionTimeOutMs               int
	KafkaTopicMetadataRefreshIntervalMs int
	KafkaPartitionAssignmentStrategy    string
	KafkaEnableAutoCommit               bool
	KafkaMaxPollIntervalMs              int

	// RabbitMQ
	RabbitMQBootstrapServer string `json:"bootstrapServer"`
	RabbitMQQueueName       string `json:"queueName"`
	RabbitMQConsumerAutoAck bool   `json:"consumerAutoAck"`

	// Any Other Consumer
	Consumer any
=======
	ConsumerType string
	Consumer     any
>>>>>>> 06bcdb0 (Minor fixes)
}

func (abc KeyValueExtractorStruct1[K, V]) Extract(source any) (key K, value V, err error) {
	return "", "", nil
}

func (kve KeyValueExtractorStruct[K, V]) Extract(source any) (key K, value V, err error) {
	//V.Extract(source)
	codec := kve.codec
	if kafkaMsg, ok := source.(*kafka.Message); ok {
		decodedMsg, err := avroHelpers.TransformAvro(kafkaMsg.Value, codec, avroHelpers.NativeFromBinary)
		if err != nil {
			fmt.Println("Extract + TransformAvro: ", err.Error())
			return key, value, err
		}
		fmt.Println(decodedMsg)
	} else if rmqMessage, ok := source.(amqp.Delivery); ok {
		mp := make(map[string]interface{})
		err := json.Unmarshal(rmqMessage.Body, &mp)
		fmt.Println(err.Error())
	} else {
		err = errors.New(fmt.Sprintf("Error with source type assertion: %v", source))
	}

	return "", "", err
}

func (kve DelegatorStruct[K, V]) Delegate(source any) (err error) {
	fmt.Println("Aggregated Messages:")
	for key, element := range source.(map[K][]V) {
		fmt.Println("Key:", key, "=>", "Element:", element)
	}
	return nil
}
func (kve RetryHandlerStruct[K, V]) RetryHandle(source any) (err error) {
	return nil
}
func (kve RetryHandlerStructNew[K, V]) RetryHandle(source any) (err error) {
	return nil
}
func (kve DLQHandlerStruct[K, V]) DLQHandle(source any) (err error) {
	return nil
}

func (kve AggregatorConfigStruct) CreateConsumerFromConfig() (aggregator.Consumer, error) {
<<<<<<< HEAD
	if kve.ConsumerType == "kafka" {
		consumer := &kafka2.KafkaConsumerConfig{
			TopicNames:                     kve.KafkaTopicNames,
			PollTimeoutMs:                  kve.KafkaPollTimeOutMs,
			BootstrapServers:               kve.KafkaBootStrapServers,
			GroupId:                        kve.KafkaGroupId,
			AutoOffsetReset:                kve.KafkaAutoOffsetReset,
			HeartbeatIntervalMs:            kve.KafkaHeartBeatIntervalMs,
			SessionTimeoutMs:               kve.KafkaSessionTimeOutMs,
			TopicMetadataRefreshIntervalMs: kve.KafkaTopicMetadataRefreshIntervalMs,
			PartitionAssignmentStrategy:    kve.KafkaPartitionAssignmentStrategy,
			EnableAutoCommit:               kve.KafkaEnableAutoCommit,
			MaxPollIntervalMs:              kve.KafkaMaxPollIntervalMs,
		}
		return interface{}(consumer).(aggregator.Consumer), nil
	} else if kve.ConsumerType == "rabbitmq" {
		consumer := &rabbitmq.RmqConsumerConfig{
			BootstrapServer: kve.RabbitMQBootstrapServer,
			QueueName:       kve.RabbitMQQueueName,
			ConsumerAutoAck: kve.RabbitMQConsumerAutoAck,
		}
		return interface{}(consumer).(aggregator.Consumer), nil
	} else if kve.ConsumerType == "" {
		kve.Logger.Error("ConsumerType empty")
		return nil, errors.New("ConsumerType empty")
	}

	if val, ok := interface{}(kve.Consumer).(aggregator.Consumer); ok {
		kve.Logger.Info("Provided consumer " + kve.ConsumerType + " implements all the methods required for aggregator.Consumer interface{}")
		return val, nil
	} else {
		kve.Logger.Error("Provided consumer " + kve.ConsumerType + " doesn't implements all the methods required for aggregator.Consumer interface{}")
		return nil, errors.New("Provided consumer " + kve.ConsumerType + " doesn't implements all the methods required for aggregator.Consumer interface{}")
=======
	if kve.ConsumerType == "" {
		kve.Logger.Error("ConsumerType empty")
		return nil, errors.New("ConsumerType empty")
	} else {
		if val, ok := interface{}(kve.Consumer).(aggregator.Consumer); ok {
			kve.Logger.Info("Provided consumer " + kve.ConsumerType + " implements all the methods required for aggregator.Consumer interface{}")
			return val, nil
		} else {
			kve.Logger.Error("Provided consumer " + kve.ConsumerType + " doesn't implements all the methods required for aggregator.Consumer interface{}")
			return nil, errors.New("Provided consumer " + kve.ConsumerType + " doesn't implements all the methods required for aggregator.Consumer interface{}")
		}
>>>>>>> 06bcdb0 (Minor fixes)
	}
}
func (kve AggregatorConfigStruct) GetAggregatorConfig() aggregator.AggregationConfig {
	return aggregator.AggregationConfig{
		TimeDuration: kve.TimeDuration,
		MessageCount: kve.MessageCount,
		Logger:       kve.Logger,
	}
}

func (kve AggregatorConfigStruct) GetLogger() *zap.Logger {
	return kve.Logger
}
