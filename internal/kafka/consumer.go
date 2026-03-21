package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

type MessageHandler func(msg *sarama.ConsumerMessage) error

type Consumer struct {
	consumer sarama.Consumer
}

func NewConsumer(cfg *Config) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer: consumer}, nil
}

func (c *Consumer) Consume(topic string, partition int32, handler MessageHandler) error {
	partitionConsumer, err := c.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	for {
		select {
		case err := <-partitionConsumer.Errors():
			log.Printf("Consumer error: %v", err)
		case msg := <-partitionConsumer.Messages():
			if err := handler(msg); err != nil {
				log.Printf("Handler error: %v", err)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
