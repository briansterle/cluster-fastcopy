package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/copy", handleCopy)
	log.Println("Listening on :8080...")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("failed to start http server: %s", err)
	}
}

func handleCopy(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		http.Error(w, "'from' and 'to' query params must be supplied.'", http.StatusBadRequest)
		return
	}

}
