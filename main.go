package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/colinmarc/hdfs/v2"
)

var hdfsClient *hdfs.Client

// lazy load the hdfs client
func getHdfsClient() *hdfs.Client {
	if hdfsClient == nil {
		client, err := hdfs.New("localhost:9000")
		if err != nil {
			log.Fatalf("failed to create hdfs client: %s", err)
		}
		hdfsClient = client
	}
	return hdfsClient
}

func main() {
	// create an hdfs client
	defer hdfsClient.Close()

	// bind functions to routes
	http.HandleFunc("/copy", copy)
	http.HandleFunc("/upload", upload)
	log.Println("Listening on :8080...")

	// start the http server
	e := http.ListenAndServe(":8080", nil)
	if e != nil {
		log.Fatalf("failed to start http server: %s", e)
	}
}

// Uploads the incoming byte[] to the hdfs path provided by
// query param 'to'
func upload(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")
	to := r.URL.Query().Get("to")

	log.Printf("Writing %s to target: %s\n", fileName, to)

	if to == "" || fileName == "" {
		http.Error(w, "'to', 'fileName', 'dir' query params must be provided.", http.StatusBadRequest)
		return
	}
	client := getHdfsClient()

	// create target dir
	client.MkdirAll(to, os.FileMode(0755))

	// create file
	path := filepath.Join(to, fileName)
	// write data from request body into the file
	client.Remove(path) // Truncate the file to 0 bytes

	file, err := client.Create(path)
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
func copy(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		http.Error(w, "'from' and 'to' query params must be provided.'", http.StatusBadRequest)
		return
	}
	client := getHdfsClient()
	fileInfos, err := client.ReadDir(from)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list the hdfs dir %s", err), http.StatusInternalServerError)
		return
	}

	// Wait group to synchronize goroutines
	var wg sync.WaitGroup

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		// filePath := filepath.Join(from, fileInfo.Name())
		wg.Add(1)

		go func(file string) {
			defer wg.Done()

			path := filepath.Join(from, file)
			log.Printf("Reading from path: %s\n", path)
			reader, err := client.Open(path)
			if err != nil {
				log.Printf("Failed to read file %s\n", file)
				return
			}
			defer reader.Close()

			// Create a buffer to store the file contents
			var buf bytes.Buffer
			_, err = io.Copy(&buf, reader)
			if err != nil {
				log.Printf("Failed to read file '%s': %s\n", file, err)
				return
			}

			// Define the URL of Service B to send the file to
			uploadUrl := "http://localhost:8080/upload?fileName=" + file + "&to=" + to

			// Create an HTTP request
			req, err := http.NewRequest(http.MethodPost, uploadUrl, &buf)
			if err != nil {
				log.Printf("Failed to create request for file '%s': %s", reader, err)
				return
			}

			// Set the Content-Type header
			req.Header.Set("Content-Type", "application/octet-stream")

			// Send the request to Service B
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to send file '%s' to Service B: %s", file, err)
				return
			}
			defer resp.Body.Close()

			// Check the response from Service B
			if resp.StatusCode != http.StatusOK {
				log.Printf("Service B returned non-OK status for file '%s': %d", file, resp.StatusCode)
				return
			}

			log.Printf("File '%s' sent successfully to Service B!", file)

		}(fileInfo.Name())
	}

	wg.Wait()
	w.WriteHeader(http.StatusOK)
}
