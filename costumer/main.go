package main

import (
	"net/http"
	"log"
)

type order struct {
	Name  string `json:"name"`
	Price int    `json:"price"`
}

main() {
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		order := new(order)
		if err := json.NewDecoder(r.boby).Decode(order); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		orderBytes, err := json.Marshal(order)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		PushOrderToQueue(orderBytes)
		w.WriteHeader(http.StatusAccepted)
	})
}

func PushOrderToQueue(message []byte) {
	// This function would contain the logic to push the order to Kafka
	// For example, you could use a Kafka producer library to send the orderBytes to a Kafka topic
}