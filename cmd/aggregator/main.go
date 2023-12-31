package main

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
	"packet-aggregator/pkg/aggregator"
	avroHelpers "packet-aggregator/pkg/helpers/avro"
	"time"
)

func main() {
	server := []string{"kafka:9092"}
	schemaFile := "template.avro"
	codec, err := avroHelpers.GetCodec(schemaFile)
	if err != nil {
		return
	}
	aggr := aggregator.CreateAggregator[string, string](
		KeyValueExtractorStruct[string, string]{
			codec: codec,
			key:   "name1",
			value: "name2",
		},
		DelegatorStruct[string, string]{},
		RetryHandlerStruct[string, string]{},
		DLQHandlerStruct[string, string]{},
	)

	config := aggregator.AggrConfig{
		TimeDuration: 2 * time.Minute,
		MessageCount: 2,

		ConsumerConfig: aggregator.ConsumerConfig{
			TopicNames:    []string{"aggregator_test"},
			PollTimeoutMs: 10,

			BootstrapServers:               server,
			GroupId:                        "aggregator-test-1",
			AutoOffsetReset:                "earliest",
			HeartbeatIntervalMs:            3000,
			SessionTimeoutMs:               30000,
			TopicMetadataRefreshIntervalMs: 36000,
			PartitionAssignmentStrategy:    "range",
			EnableAutoCommit:               false,
			MaxPollIntervalMs:              600000,
		},
	}
	aggr.StartKafkaConsumer(config)

}

type KeyValueExtractorStruct[K string, V string] struct {
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

func (kve KeyValueExtractorStruct[K, V]) Extract(source any) (key K, value V, err error) {
	codec := kve.codec
	if kafkaMsg, ok := source.(*kafka.Message); ok {
		decodedMsg, err := avroHelpers.TransformAvro(kafkaMsg.Value, codec, avroHelpers.NativeFromBinary)
		if err != nil {
			fmt.Println("Extract + TransformAvro: ", err.Error())
			return key, value, err
		}
		if decodedMsgMap, ok := decodedMsg.(map[string]any); ok {
			if key, ok = decodedMsgMap[kve.key].(K); ok {
				if value, ok = decodedMsgMap[kve.value].(V); ok {
					return key, value, nil
				} else {
					err = errors.New(fmt.Sprintf("Error with value: %v | MessageMap: %v", kve.value, decodedMsgMap))
				}
			} else {
				err = errors.New(fmt.Sprintf("Error with key: %v | MessageMap: %v", kve.key, decodedMsgMap))
			}
		}
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
