package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/colinmarc/hdfs"
)

func main() {
	http.HandleFunc("/copy", handleCopy)
	http.HandleFunc("/upload", handleUpload)
	log.Println("Listening on :8080...")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("failed to start http server: %s", err)
	}
}

// Uploads the incoming byte[] to the hdfs path provided by
// query param 'to'
func handleUpload(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")

	if to == "" {
		http.Error(w, "'to' query param must be provided.", http.StatusBadRequest)
		return
	}
	client, err := hdfs.New("localhost:9000")

	if err != nil {
		http.Error(w, fmt.Sprintf("failed to start hdfs client: %s", err), http.StatusInternalServerError)
		return
	}

	buf := make([]byte, 59)
	r.Body.Read(buf)
	fmt.Println(buf)
	client.MkdirAll(to, os.FileMode(0755))
	w.WriteHeader(http.StatusOK)
}

// Reads all files in a given directory provided by 'from'
// and uploads them to the user provided path 'to'
func handleCopy(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		http.Error(w, "'from' and 'to' query params must be provided.'", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}
