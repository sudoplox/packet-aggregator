package aggregator

import (
	"go.uber.org/zap"
	"time"
)

func CreateAggregator[K comparable, V any](extract KeyValueExtractor[K, V], delegate Delegator[K, V], retry RetryHandler[K, V], handle DLQHandler[K, V], config AggregatorConfig) (AggrObject[K, V], error) {

	aggr := AggrObject[K, V]{
		extract,
		delegate,
		retry,
		handle,
		config,
		nil,
	}
	consumer, err := aggr.AggregatorConfig.CreateConsumerFromConfig()
	if err != nil {
		aggr.AggregatorConfig.GetLogger().Error(err.Error())
		return AggrObject[K, V]{}, err
	}
	aggr.Consumer = consumer
	return aggr, nil
}

func (aggr AggrObject[K, V]) Start() error {
	logger := aggr.AggregatorConfig.GetLogger()
	logger.Info("Staring Aggregator")
	err := aggr.Consumer.StartConsumer(aggr.GetLogger())
	if err != nil {
		logger.Error("Error while starting the Aggregator, err : " + err.Error())
		return err
	}

	aggregationConfig := aggr.AggregatorConfig.GetAggregatorConfig()
	messagesMap := make(map[K][]V)

	timer := time.NewTimer(aggregationConfig.TimeDuration)

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
				err = aggr.Consumer.CommitMessages(aggr.GetLogger())
			}
			timer.Reset(aggr.AggregatorConfig.GetAggregatorConfig().TimeDuration)
			messagesMap = make(map[K][]V)
		default:
			msg, err := aggr.Consumer.GetMessages(aggr.GetLogger(), 1, timer)
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

			if len(messagesMap) >= aggr.AggregatorConfig.GetAggregatorConfig().MessageCount {
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
				err = aggr.Consumer.CommitMessages(aggr.GetLogger())
				if err != nil {
					logger.Error("Error while commit the messages", zap.Any("error", err))
					return err
				}
				timer.Reset(aggr.AggregatorConfig.GetAggregatorConfig().TimeDuration)
				messagesMap = make(map[K][]V)
			}
		}
	}
	return nil
}

func (aggr AggrObject[K, V]) Stop() error {
	err := aggr.Consumer.StopConsumer(aggr.GetLogger())
	if err != nil {
		aggr.GetLogger().Error("Error stopping consumer ", zap.Any("error", err))
		return err
	}
	return nil
}
