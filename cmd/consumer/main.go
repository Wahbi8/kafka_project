package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/Wahbi8/kafka_project/internal/kafka"
)

func main() {
	cfg := kafka.NewConfig()

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	msgCount := 0

	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		log.Printf("Processed %d messages", msgCount)
		consumer.Close()
		os.Exit(0)
	}()

	log.Println("Consumer started, listening for orders...")

	err = consumer.Consume("orders", 0, func(msg *sarama.ConsumerMessage) error {
		msgCount++
		log.Printf("Received order %d: %s", msgCount, string(msg.Value))
		return nil
	})
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}
