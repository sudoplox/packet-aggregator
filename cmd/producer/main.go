package main

import (
	"encoding/json"
	"fmt"
	"packet-aggregator/pkg/aggregator"
	avroHelpers "packet-aggregator/pkg/helpers/avro"
	producerHelpers "packet-aggregator/pkg/helpers/producer"
)

func main() {
	msg := getMsg()
	producerConfig := aggregator.ProducerConfig{
		BootstrapServers: []string{"kafka:9092"},
		TopicName:        "aggregator_test",
	}
	producerHelpers.PushMessage(producerConfig, msg)
}

func getMsg() []byte {
	schemaFile := "template.avro"
	codec, err := avroHelpers.GetCodec(schemaFile)
	if err != nil {
		fmt.Printf("Error GetCodec: %v\n", err)
		return nil
	}
	jsonData := []byte(`{
				"name1": "Sudhanshu1",
				"name2": "Mohan",
				"age": 24,
				"is_adult": true,
				"colleagues": [
					"Prasanna",
					"Hari",
					"Arpit"
				]
			 }`)

	var jsonDataMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &jsonDataMap); err != nil {
		fmt.Printf("Error unmarshaling JSON data: %v\n", err)
		return nil
	}

	encodedMessage, err := codec.BinaryFromNative(nil, jsonDataMap)
	return encodedMessage
}
