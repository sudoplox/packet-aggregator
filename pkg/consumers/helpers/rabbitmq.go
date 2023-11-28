package helpers

import "packet-aggregator/pkg/consumers/rabbitmq"

func FormRabbitMqConsumer(bootstrapServer string,
	queueName string,
	consumerAutoAck bool) *rabbitmq.RmqConsumerConfig {
	return &rabbitmq.RmqConsumerConfig{
		BootstrapServer: bootstrapServer,
		QueueName:       queueName,
		ConsumerAutoAck: consumerAutoAck,
	}
}
