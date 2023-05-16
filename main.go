package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/colinmarc/hdfs/v2"
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
	fileName := r.URL.Query().Get("fileName")
	to := r.URL.Query().Get("to")

	if to == "" || fileName == "" {
		http.Error(w, "'to', 'fileName', 'dir' query params must be provided.", http.StatusBadRequest)
		return
	}

	// create an hdfs client
	client, err := hdfs.New("localhost:9000")
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to start hdfs client: %s", err), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	// create target dir
	client.MkdirAll(to, os.FileMode(0755))

	// create file
	file, err := client.Create(filepath.Join(to, fileName))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating file in hdfs %s", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// write data from request body into the file
	_, err = io.Copy(file, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error copying request body into file %s %s", fileName, err), http.StatusInternalServerError)
		return
	}

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

	// find all the files in the dir

	w.WriteHeader(http.StatusOK)
}
