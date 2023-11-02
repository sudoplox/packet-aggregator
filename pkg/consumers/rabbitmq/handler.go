package rabbitmq

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

//func (rmq RmqConsumerConfig)StartConsumer()

func (rmq *RmqConsumerConfig) StartConsumer() (any, error) {
	connection, err := amqp.Dial(rmq.BootstrapServer)
	if err != nil {
		fmt.Println("Error while connecting to rmq server, err : ", err.Error())
		return nil, errors.New("Error while connecting to rmq server, err : " + err.Error())
	}

	fmt.Println("Successfully established connection to server " + rmq.BootstrapServer)
	rmq.connection = connection
	//defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Println("Error while forming the channel connection to rmq server, err : ", err.Error())
		return nil, errors.New("Error while forming the channel connection to rmq server, err : " + err.Error())
	}
	rmq.channel = channel
	//defer channel.Close()

	// queue declaration

	queue, err := channel.QueueDeclare(
		rmq.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		fmt.Println("Error while forming queue, err : ", err.Error())
		return nil, errors.New("Error while forming queue, err : " + err.Error())
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
		fmt.Printf("Error while forming consumer channel to rmq for server : %s queue : %s err : %s", rmq.BootstrapServer, queue.Name, err.Error())
		return nil, errors.New(fmt.Sprintf("Error while forming consumer channel to rmq for server : %s queue : %s err : %s", rmq.BootstrapServer, queue.Name, err.Error()))
	}

	rmq.messages = &messages

	return nil, nil
}
func (rmq *RmqConsumerConfig) GetOneMessage() (any, error) {
	messagesChan := *(rmq.messages)
	for message := range messagesChan {
		rmq.currentMessage = &message
		return message, nil
	}
	return nil, nil
}
func (rmq *RmqConsumerConfig) CommitMessages() error {
	if !rmq.ConsumerAutoAck {
		err := (*rmq.currentMessage).Ack(true)
		if err != nil {
			fmt.Printf("Error while acknowledge the messages,  message data : %s,  error : %s ", (*rmq.currentMessage).Body, err.Error())
			return errors.New(fmt.Sprintf("Error while acknowledge the messages,  message data : %s,  error : %s ", (*rmq.currentMessage).Body, err.Error()))
		}
	}
	return nil
}
func (rmq *RmqConsumerConfig) StopConsumer() error {
	err := rmq.channel.Close()
	if err != nil {
		fmt.Println("Error while closing the connection channel , err : ", err.Error())
		return errors.New("Error while closing the connection channel  , err : " + err.Error())
	}
	err = rmq.connection.Close()
	if err != nil {
		fmt.Println("Error while closing the connection, err : ", err.Error())
		return errors.New("Error while closing the connection, err : " + err.Error())
	}
	return nil
}
