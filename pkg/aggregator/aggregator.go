package aggregator

import (
	"go.uber.org/zap"
	"time"
)

func CreateAggregator[K comparable, V any](extract KeyValueExtractor[K, V], delegate Delegator[K, V], retry RetryHandler[K, V], handle DLQHandler[K, V], config AggregatorConfig, consumerConfig ConsumerConfig) AggrObject[K, V] {
	return AggrObject[K, V]{
		extract,
		delegate,
		retry,
		handle,
		config,
		consumerConfig,
	}
}

// type rmqMessage []byte
//type timeOut string
//type interrupt string
//
//func (agg AggrObject[K, V]) StartConsumer(config AggrConfig) {
//	if _, validType := config.ConsumerConfig.(KafkaConsumerConfig); validType {
//		agg.startKafkaConsumer(config)
//	} else if _, validType = config.ConsumerConfig.(RmqConsumerConfig); validType {
//		agg.startRmqConsumer(config)
//	}
//}
//
//func (agg AggrObject[K, V]) startRmqConsumer(config AggrConfig) {
//	logger, _ := zap.NewProduction()
//	defer logger.Sync()
//	consumerChannel := make(chan any)
//	//agg.consumeMessagesAndAggregate(consumerChannel)
//
//	sigchan := make(chan os.Signal, 1)
//	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
//
//	//timer := time.NewTimer(aggrConfig.TimeDuration)
//
//	consumerConfig := config.ConsumerConfig.(RmqConsumerConfig)
//
//	connection, err := amqp.Dial(consumerConfig.BootstrapServer)
//	if err != nil {
//		logger.Error("Failed to connect to RMQ server ",
//			zap.String("Error", err.Error()),
//		)
//		return
//	}
//	defer connection.Close()
//
//	channel, err := connection.Channel()
//	if err != nil {
//		logger.Error("Failed to create a channel to the RMQ server  ",
//			zap.String("Error", err.Error()),
//		)
//		return
//	}
//	defer channel.Close()
//
//	// queue declaration
//
//	queue, err := channel.QueueDeclare(
//		consumerConfig.QueueName,
//		false,
//		false,
//		false,
//		false,
//		nil,
//	)
//
//	if err != nil {
//		panic(err)
//	}
//
//	//consumer messages from the queue
//
//	messages, err := channel.Consume(
//		queue.Name,
//		"",
//		consumerConfig.ConsumerAutoAck,
//		false,
//		false,
//		false,
//		nil,
//	)
//
//	if err != nil {
//		panic(err)
//	}
//
//	messagesMap := make(map[K][]V)
//	timer := time.NewTimer(config.TimeDuration)
//
//	go func() {
//		for d := range messages {
//			consumerChannel <- d
//		}
//	}()
//
//	go func() {
//		runForInterrupts := true
//		for runForInterrupts {
//			select {
//			case <-timer.C:
//				// Time-based aggregation logic
//				var v timeOut
//				consumerChannel <- v
//			case _ = <-sigchan:
//				var v interrupt
//				consumerChannel <- v
//				runForInterrupts = false
//			}
//		}
//	}()
//
//	var forever chan struct{}
//
//	commitAndReset := func(delivery amqp.Delivery) {
//
//		if err := delivery.Ack(true); err != nil {
//			logger.Error("Error acknowledging the messages",
//				zap.Any("Message", delivery.Body),
//				zap.Any("Message Count", delivery.MessageCount))
//		}
//		timer.Reset(config.TimeDuration)
//		messagesMap = make(map[K][]V)
//	}
//
//	executeAggr := func(d interface{}) {
//		err := agg.Delegator.Delegate(messagesMap)
//		if err != nil {
//			logger.Error("Error calling Delegate",
//				zap.String("Error", err.Error()),
//				zap.Any("Message Map", messagesMap),
//			)
//
//			err := agg.RetryHandler.RetryHandle(messagesMap)
//			if err != nil {
//				logger.Error("Error calling RetryHandle",
//					zap.String("Error", err.Error()),
//					zap.Any("Message Map", messagesMap),
//				)
//
//				err := agg.DLQHandler.DLQHandle(messagesMap)
//				if err != nil {
//					logger.Error("Error calling DLQHandle",
//						zap.String("Error", err.Error()),
//						zap.Any("Message Map", messagesMap),
//					)
//				}
//				if delivery, validType := d.(amqp.Delivery); validType {
//					commitAndReset(delivery)
//				}
//			} else {
//				if delivery, validType := d.(amqp.Delivery); validType {
//					commitAndReset(delivery)
//				}
//			}
//		} else {
//			if delivery, validType := d.(amqp.Delivery); validType {
//				commitAndReset(delivery)
//			}
//		}
//	}
//
//	run := true
//	for run {
//		for events := range consumerChannel {
//			switch event := events.(type) {
//			case timeOut:
//				if len(messagesMap) > 0 {
//					executeAggr(nil)
//				}
//			case interrupt:
//				logger.Info("Terminating")
//				run = false
//			case amqp.Delivery:
//				key, value, err := agg.KeyValueExtractor[K, V].Extract(event)
//				if err != nil {
//					// TODO Handle key value extractor failure
//					logger.Error("Error while running KeyValueExtractor",
//						zap.String("Error", err.Error()),
//					)
//					continue
//				}
//				messagesMap[key] = append(messagesMap[key], value)
//
//				if len(messagesMap) >= config.MessageCount {
//					executeAggr(event)
//				}
//
//			}
//		}
//	}
//
//	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
//	<-forever
//}
//
//func (agg AggrObject[K, V]) startKafkaConsumer(aggrConfig AggrConfig) {
//
//	logger, _ := zap.NewProduction()
//	defer logger.Sync()
//
//	sigchan := make(chan os.Signal, 1)
//	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
//
//	consumerConfig := aggrConfig.ConsumerConfig.(KafkaConsumerConfig)
//	c, err := kafka.NewConsumer(&kafka.ConfigMap{
//		"bootstrap.servers":                  strings.Join(consumerConfig.BootstrapServers, ","), // Kafka broker address
//		"group.id":                           consumerConfig.GroupId,                             // Consumer group ID
//		"auto.offset.reset":                  consumerConfig.AutoOffsetReset,                     // Start consuming from the beginning of topics
//		"heartbeat.interval.ms":              consumerConfig.HeartbeatIntervalMs,                 // For consumer to specify it's alive to the broker
//		"session.timeout.ms":                 consumerConfig.SessionTimeoutMs,                    // If the broker doesn't receive heartbeat request from consumer within this timeout, it will delete the consumer and trigger rebalance
//		"topic.metadata.refresh.interval.ms": consumerConfig.TopicMetadataRefreshIntervalMs,      // At this interval, topic and broker data are refreshed. (Any new leaders, new brokers were picked up)
//		"partition.assignment.strategy":      consumerConfig.PartitionAssignmentStrategy,         // Strategy for assigning partitions to consumers
//		"enable.auto.commit":                 consumerConfig.EnableAutoCommit,                    // Disable automatic offset committing
//		"max.poll.interval.ms":               consumerConfig.MaxPollIntervalMs,                   // Maximum time between polls (10 minutes)
//	})
//
//	if err != nil {
//		logger.Error("Failed to create consumer!",
//			zap.String("Error", err.Error()),
//		)
//		return
//	}
//
//	logger.Info("Created Consumer!",
//		zap.Any("Consumer", c),
//	)
//
//	err = c.SubscribeTopics(consumerConfig.TopicNames, nil)
//	if err != nil {
//		logger.Error("Error at subscribing to the topic",
//			zap.Any("Topic", consumerConfig.TopicNames),
//			zap.String("Error", err.Error()),
//		)
//		panic(err)
//	}
//
//	messagesMap := make(map[K][]V)
//	timer := time.NewTimer(aggrConfig.TimeDuration)
//
//	commitAndReset := func() {
//		topicPartitions, err := c.Commit()
//		if err != nil {
//			for _, topicPartition := range topicPartitions {
//				logger.Error("Error committing offset",
//					zap.Any("Topic", topicPartition.Topic),
//					zap.Any("Offset", topicPartition.Offset),
//					zap.Any("Partition", topicPartition.Partition),
//				)
//			}
//		}
//		timer.Reset(aggrConfig.TimeDuration)
//		messagesMap = make(map[K][]V)
//	}
//
//	executeAggr := func() {
//		err := agg.Delegator.Delegate(messagesMap)
//		if err != nil {
//			logger.Error("Error calling Delegate",
//				zap.String("Error", err.Error()),
//				zap.Any("Message Map", messagesMap),
//			)
//
//			err := agg.RetryHandler.RetryHandle(messagesMap)
//			if err != nil {
//				logger.Error("Error calling RetryHandle",
//					zap.String("Error", err.Error()),
//					zap.Any("Message Map", messagesMap),
//				)
//
//				err := agg.DLQHandler.DLQHandle(messagesMap)
//				if err != nil {
//					logger.Error("Error calling DLQHandle",
//						zap.String("Error", err.Error()),
//						zap.Any("Message Map", messagesMap),
//					)
//				}
//				commitAndReset()
//
//			} else {
//				commitAndReset()
//			}
//		} else {
//			commitAndReset()
//		}
//	}
//
//	run := true
//	for run {
//		select {
//		case <-timer.C:
//			// Time-based aggregation logic
//			if len(messagesMap) > 0 {
//				executeAggr()
//			}
//		case sig := <-sigchan:
//			logger.Info("Terminating",
//				zap.Any("Caught Signal", sig),
//			)
//			run = false
//		default:
//			ev := c.Poll(consumerConfig.PollTimeoutMs)
//			if ev == nil {
//				continue
//			}
//			switch e := ev.(type) {
//			case *kafka.Message:
//				key, value, err := agg.KeyValueExtractor[K, V].Extract(e)
//				if err != nil {
//					// TODO Handle key value extractor failure
//					logger.Error("Error while running KeyValueExtractor",
//						zap.String("Error", err.Error()),
//					)
//					continue
//				}
//				messagesMap[key] = append(messagesMap[key], value)
//
//				if len(messagesMap) >= aggrConfig.MessageCount {
//					executeAggr()
//				}
//			case kafka.Error:
//				// Errors should generally be considered
//				// informational, the client will try to
//				// automatically recover.
//				// But in this example we choose to terminate
//				// the application if all brokers are down.
//				logger.Error("Kafka Error",
//					zap.Any("Error", e.Error()),
//					zap.Any("Error Code", e.Code()),
//					zap.Any("Event", e),
//				)
//				if e.Code() == kafka.ErrAllBrokersDown {
//					run = false
//				}
//			default:
//				logger.Warn("Ignored",
//					zap.Any("Event", e),
//				)
//			}
//		}
//	}
//	logger.Info("Closing Consumer!",
//		zap.Any("Consumer", c),
//	)
//	err = c.Close()
//	if err != nil {
//		logger.Error("Error Closing Consumer!")
//	}
//}

func (aggr AggrObject[K, V]) Start() error {
	logger := aggr.AggregatorConfig.GetLogger()
	logger.Info("Staring Aggregator")
	_, err := aggr.ConsumerConfig.StartConsumer()
	if err != nil {
		logger.Error("Error while starting the Aggregator, err : " + err.Error())
		return err
	}

	messageCount, timeOutDuration := aggr.AggregatorConfig.GetAggregatorConfig()
	messagesMap := make(map[K][]V)

	timer := time.NewTimer(timeOutDuration)

	for {
		select {
		case <-timer.C:
			// Time-based aggregation logic
			if len(messagesMap) > 0 {
				err = aggr.Delegator.Delegate(messagesMap)
				if err != nil {
					err = aggr.RetryHandler.RetryHandle(messagesMap)
					if err != nil {
						err = aggr.DLQHandler.DLQHandle(messagesMap)
						if err != nil {
							panic(err)
						}
					}
				}
				err = aggr.ConsumerConfig.CommitMessages()
			}
			timer.Reset(timeOutDuration)
			messagesMap = make(map[K][]V)
		default:
			msg, err := aggr.ConsumerConfig.GetOneMessage()
			if err != nil {
				logger.Error("Error while getting the message from consumer ", zap.Any("message", msg),
					zap.Any("error", err))
				return err
			}
			key, val, err := aggr.KeyValueExtractor.Extract(msg)
			if err != nil {
				logger.Error("Key Value extractor error", zap.Any("message", msg),
					zap.Any("error", err))
				return err
			}
			messagesMap[key] = append(messagesMap[key], val)

			if len(messagesMap) >= messageCount {
				err = aggr.Delegator.Delegate(messagesMap)
				if err != nil {
					err = aggr.RetryHandler.RetryHandle(messagesMap)
					if err != nil {
						err = aggr.DLQHandler.DLQHandle(messagesMap)
						if err != nil {
							panic(err)
						}
					}
				}
				err = aggr.ConsumerConfig.CommitMessages()
				if err != nil {
					logger.Error("Error while commit the messages", zap.Any("error", err))
					return err
				}
				timer.Reset(timeOutDuration)
				messagesMap = make(map[K][]V)
			}
		}
	}
	return nil
}

func (aggr AggrObject[K, V]) Stop() error {
	err := aggr.ConsumerConfig.StopConsumer()
	if err != nil {
		aggr.GetLogger().Error("Error stopping consumer ", zap.Any("error", err))
		return err
	}
	return nil
}

//func (c KafkaConsumerConfig) Start() error {
//	return nil
//}
//
//func (c KafkaConsumerConfig) Stop() error {
//	return nil
//}

//func (agg AggrObject[K, V]) consumeMessagesAndAggregate(receiverChannel chan any, aggrConfig AggrConfig) {
//	logger, _ := zap.NewProduction()
//	messagesMap := make(map[K][]V)
//	timer := time.NewTimer(aggrConfig.TimeDuration)
//	sigchan := make(chan os.Signal, 1)
//	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
//
//	go func() {
//		runTimer := true
//		for runTimer {
//			select {
//			case <-timer.C:
//				var v timeOut
//				receiverChannel <- v
//			case _ = <-sigchan:
//				var v interrupt
//				receiverChannel <- v
//			}
//		}
//	}()
//
//	//commitAndReset := func() {
//	//	topicPartitions, err := c.Commit()
//	//	if err != nil {
//	//		for _, topicPartition := range topicPartitions {
//	//			logger.Error("Error committing offset",
//	//				zap.Any("Topic", topicPartition.Topic),
//	//				zap.Any("Offset", topicPartition.Offset),
//	//				zap.Any("Partition", topicPartition.Partition),
//	//			)
//	//		}
//	//	}
//	//	timer.Reset(aggrConfig.TimeDuration)
//	//	messagesMap = make(map[K][]V)
//	//}
//
//	executeAggr := func() {
//		err := agg.Delegator.Delegate(messagesMap)
//		if err != nil {
//			logger.Error("Error calling Delegate",
//				zap.String("Error", err.Error()),
//				zap.Any("Message Map", messagesMap),
//			)
//
//			err := agg.RetryHandler.RetryHandle(messagesMap)
//			if err != nil {
//				logger.Error("Error calling RetryHandle",
//					zap.String("Error", err.Error()),
//					zap.Any("Message Map", messagesMap),
//				)
//
//				err := agg.DLQHandler.DLQHandle(messagesMap)
//				if err != nil {
//					logger.Error("Error calling DLQHandle",
//						zap.String("Error", err.Error()),
//						zap.Any("Message Map", messagesMap),
//					)
//				}
//				//commitAndReset()
//
//			} else {
//				//commitAndReset()
//			}
//		} else {
//			//commitAndReset()
//		}
//	}
//
//	continueRunning := true
//	select {
//	case msg := <-receiverChannel:
//		if continueRunning {
//			fmt.Println(msg)
//			switch event := msg.(type) {
//			case interrupt:
//				logger.Info("Terminating")
//				continueRunning = false
//				break
//			case timeOut:
//				// Time-based aggregation logic
//				if len(messagesMap) > 0 {
//					executeAggr()
//				}
//			case *kafka.Message, rmqMessage:
//				key, value, err := agg.KeyValueExtractor[K, V].Extract(event)
//				if err != nil {
//					// TODO Handle key value extractor failure
//					logger.Error("Error while running KeyValueExtractor",
//						zap.String("Error", err.Error()),
//					)
//				}
//				messagesMap[key] = append(messagesMap[key], value)
//
//				if len(messagesMap) >= aggrConfig.MessageCount {
//					executeAggr()
//				}
//			case *kafka.Error:
//				logger.Error("Kafka Error",
//					zap.Any("Error", event.Error()),
//					zap.Any("Error Code", event.Code()),
//					zap.Any("Event", event),
//				)
//				if event.Code() == kafka.ErrAllBrokersDown {
//					continueRunning = false
//					break
//				}
//			default:
//				logger.Warn("Ignored")
//			}
//		}
//	default:
//		logger.Warn("Ignored")
//	}
//
//	for event := range receiverChannel {
//		select {}
//	}
//
//	run := true
//	for run {
//		select {
//		case <-timer.C:
//			// Time-based aggregation logic
//			if len(messagesMap) > 0 {
//				executeAggr()
//			}
//		case sig := <-sigchan:
//			logger.Info("Terminating",
//				zap.Any("Caught Signal", sig),
//			)
//			run = false
//		default:
//			for event := range receiverChannel {
//				switch e := event.(type) {
//				case *kafka.Message:
//				case *kafka.Error:
//				case rmqMessage:
//				default:
//				}
//			}
//
//			ev := c.Poll(consumerConfig.PollTimeoutMs)
//			if ev == nil {
//				continue
//			}
//			switch e := ev.(type) {
//			case *kafka.Message:
//				key, value, err := agg.KeyValueExtractor[K, V].Extract(e)
//				if err != nil {
//					// TODO Handle key value extractor failure
//					logger.Error("Error while running KeyValueExtractor",
//						zap.String("Error", err.Error()),
//					)
//					continue
//				}
//				messagesMap[key] = append(messagesMap[key], value)
//
//				if len(messagesMap) >= aggrConfig.MessageCount {
//					executeAggr()
//				}
//			case kafka.Error:
//				// Errors should generally be considered
//				// informational, the client will try to
//				// automatically recover.
//				// But in this example we choose to terminate
//				// the application if all brokers are down.
//				logger.Error("Kafka Error",
//					zap.Any("Error", e.Error()),
//					zap.Any("Error Code", e.Code()),
//					zap.Any("Event", e),
//				)
//				if e.Code() == kafka.ErrAllBrokersDown {
//					run = false
//				}
//			default:
//				logger.Warn("Ignored",
//					zap.Any("Event", e),
//				)
//			}
//		}
//	}
//	logger.Info("Closing Consumer!",
//		zap.Any("Consumer", c),
//	)
//	err = c.Close()
//	if err != nil {
//		logger.Error("Error Closing Consumer!")
//	}
//
//	//fmt.Println(receiverChannel)
//	//return nil
//}
