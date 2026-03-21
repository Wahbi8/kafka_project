package kafka

import (
	"os"
)

type Config struct {
	Brokers []string
}

func NewConfig() *Config {
	brokerAddr := os.Getenv("KAFKA_BROKER")
	if brokerAddr == "" {
		brokerAddr = "localhost:9092"
	}
	return &Config{
		Brokers: []string{brokerAddr},
	}
}
