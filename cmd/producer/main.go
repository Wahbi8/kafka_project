package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Wahbi8/kafka_project/internal/kafka"
)

type order struct {
	Name  string `json:"name"`
	Price int    `json:"price"`
}

var producer *kafka.Producer

func main() {
	cfg := kafka.NewConfig()

	var err error
	producer, err = kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer producer.Close()

	http.HandleFunc("/order", handleOrder)

	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		log.Println("Shutting down...")
		producer.Close()
		os.Exit(0)
	}()

	log.Println("Producer server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
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

	orderBytes, err := json.Marshal(ord)
	if err != nil {
		http.Error(w, "Failed to marshal order", http.StatusInternalServerError)
		return
	}

	if err := producer.Send("orders", orderBytes); err != nil {
		http.Error(w, "Failed to push to Kafka", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"message": "Order received"})
}
