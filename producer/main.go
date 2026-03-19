package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	worker, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer worker.Close()

	// Using OffsetOldest helps during testing to see previous messages
	partitionConsumer, err := worker.ConsumePartition("orders", 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Worker started...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCnt := 0
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-partitionConsumer.Errors():
				fmt.Println("Error:", err)
			case msg := <-partitionConsumer.Messages():
				msgCnt++
				fmt.Printf("Received order %d: %s\n", msgCnt, string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCnt, "messages")
}

func ConnectConsumer(broker []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	return sarama.NewConsumer(broker, config)
}