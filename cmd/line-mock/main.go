package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type request struct {
	To   string `json:"to"`
	Text string `json:"text"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("line-mock: to=%s text=%q", req.To, req.Text)
		w.WriteHeader(http.StatusAccepted)
	})

	log.Println("line-mock: listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
