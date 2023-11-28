package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type RmqConsumerConfig struct {
	BootstrapServer string `json:"bootstrapServer"`
	QueueName       string `json:"queueName"`
	ConsumerAutoAck bool   `json:"consumerAutoAck"`
	connection      *amqp.Connection
	channel         *amqp.Channel
	queue           *amqp.Queue
	messages        *<-chan amqp.Delivery
	currentMessage  *amqp.Delivery
}
