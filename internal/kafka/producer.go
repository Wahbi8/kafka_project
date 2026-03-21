package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(cfg *Config) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	return &Producer{producer: producer}, nil
}

func (p *Producer) Send(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Message sent to partition %d at offset %d", partition, offset)
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
