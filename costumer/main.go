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

func main() {
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		order := new(order)
		if err := json.NewDecoder(r.Body).Decode(order); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		orderBytes, err := json.Marshal(order)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		err = PushOrderToQueue(orderBytes)
		if err != nil {
			http.Error(w, "Failed to push order to queue", http.StatusInternalServerError)
			return
		}

		if err := json.NewEncoder(w).Encode(map[string]string{"message": "Order received"}); err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	log.Println("Server is running on port 8080")
}

func ConnectToKafka(broker []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(broker, config)
	return producer, err
}

func PushOrderToQueue(message []byte) error {
	broker := []string{"localhost:9092"}
	producer, err := ConnectToKafka(broker)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: "orders",
		Value: sarama.ByteEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	
	log.Printf("Message sent to partition %d at offset %d \n", partition, offset)
	return nil
}