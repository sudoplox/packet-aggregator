package rabbitmq

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

func (rmq *RmqConsumerConfig) StartConsumer(logger *zap.Logger) error {
	connection, err := amqp.Dial(rmq.BootstrapServer)
	if err != nil {
		logger.Error("Error while connecting to rmq server, err : " + err.Error())
		return errors.New("Error while connecting to rmq server, err : " + err.Error())
	}

	logger.Info("Successfully established connection to server " + rmq.BootstrapServer)
	rmq.connection = connection

	channel, err := connection.Channel()
	if err != nil {
		logger.Info("Error while forming the channel connection to rmq server, err : " + err.Error())
		return errors.New("Error while forming the channel connection to rmq server, err : " + err.Error())
	}
	rmq.channel = channel

	queue, err := channel.QueueDeclare(
		rmq.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logger.Error("Error while forming queue, err : " + err.Error())
		return errors.New("Error while forming queue, err : " + err.Error())
	}

	rmq.queue = &queue

	//consume messages from the queue

	messages, err := channel.Consume(
		queue.Name,
		"",
		rmq.ConsumerAutoAck,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logger.Error(fmt.Sprintf("Error while forming consumer channel to rmq for server : %s queue : %s err : %s", rmq.BootstrapServer, queue.Name, err.Error()))
		return errors.New(fmt.Sprintf("Error while forming consumer channel to rmq for server : %s queue : %s err : %s", rmq.BootstrapServer, queue.Name, err.Error()))
	}

	rmq.messages = &messages

	return nil
}

func (rmq *RmqConsumerConfig) GetMessages(logger *zap.Logger, size int, timer *time.Timer) (any, error) {
	messagesChan := *(rmq.messages)
	messages := make([]amqp.Delivery, 0)
	timeOut := false
	go func(timerCheck *bool) {
		for {
			select {
			case <-timer.C:
				*timerCheck = true
			}
		}
	}(&timeOut)
	for message := range messagesChan {
		if !timeOut {
			rmq.currentMessage = &message
			messages = append(messages, message)
			if len(messages) >= size {
				return messages, nil
			}
		} else {
			return messages, nil
		}
	}
	return messages, nil
}

func (rmq *RmqConsumerConfig) CommitMessages(logger *zap.Logger) error {
	if !rmq.ConsumerAutoAck {
		err := (*rmq.currentMessage).Ack(true)
		if err != nil {
			logger.Info(fmt.Sprintf("Error while acknowledge the messages,  message data : %s,  error : %s ", (*rmq.currentMessage).Body, err.Error()))
			return errors.New(fmt.Sprintf("Error while acknowledge the messages,  message data : %s,  error : %s ", (*rmq.currentMessage).Body, err.Error()))
		}
	}
	return nil
}
func (rmq *RmqConsumerConfig) StopConsumer(logger *zap.Logger) error {
	err := rmq.channel.Close()
	if err != nil {
		logger.Error("Error while closing the connection channel , err : " + err.Error())
		return errors.New("Error while closing the connection channel  , err : " + err.Error())
	}
	err = rmq.connection.Close()
	if err != nil {
		logger.Error("Error while closing the connection, err : " + err.Error())
		return errors.New("Error while closing the connection, err : " + err.Error())
	}
	logger.Info("Successfully closed the rmq consumer")
	return nil
}
