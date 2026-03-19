package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type order struct {
	Name  string `json:"name"`
	Price int    `json:"price"`
}

// Global producer for reuse
var producer sarama.SyncProducer

func main() {
	var err error
	broker := []string{"localhost:9092"}
	
	// Initialize producer ONCE
	producer, err = ConnectToKafka(broker)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer producer.Close()

	http.HandleFunc("/order", handleOrder)

	log.Println("Server is running on port 8080")
	// CRITICAL: You must listen and serve
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ord := new(order)
	if err := json.NewDecoder(r.Body).Decode(ord); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	orderBytes, _ := json.Marshal(ord)

	// Use the global producer
	err := PushOrderToQueue("orders", orderBytes)
	if err != nil {
		http.Error(w, "Failed to push to Kafka", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted) // Must be called BEFORE Encode
	json.NewEncoder(w).Encode(map[string]string{"message": "Order received"})
}

func ConnectToKafka(broker []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	return sarama.NewSyncProducer(broker, config)
}

func PushOrderToQueue(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err == nil {
		log.Printf("Message sent to partition %d at offset %d \n", partition, offset)
	}
	return err
}