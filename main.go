package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/colinmarc/hdfs/v2"
)

var hdfsClient *hdfs.Client

type UploadResponse struct {
	Path    string `json:"path"`
	Written int64  `json:"written"`
}

type CopyResponse struct {
	From           string        `json:"from"`
	To             string        `json:"to"`
	Written        int64         `json:"written"`
	FilesRequested int64         `json:"filesRequested"`
	FilesCopied    int64         `json:"filesCopied"`
	CopyFailures   []CopyFailure `json:"copyFailures"`
	Throughput     float64       `json:"throughputMbps"`
	ElapsedSecs    float64       `json:"elapsedSecs"`
}

type CopyFailure struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func WriteHDFS(to string, fileName string, data io.ReadCloser) (UploadResponse, error) {
	var msg string

	client := getHdfsClient()

	// create target dir
	client.MkdirAll(to, os.FileMode(0755))

	// create file
	path := filepath.Join(to, fileName)
	// write data from request body into the file
	client.Remove(path) // Truncate the file to 0 bytes

	file, err := client.Create(path)
	if err != nil {
		msg = fmt.Sprintf("Error creating file in hdfs %s", err)
		return UploadResponse{}, errors.New(msg)

	}
	defer file.Close()

	// write data from request body into the file
	written, err := io.Copy(file, data)

	if err != nil {
		msg = fmt.Sprintf("Error copying request body into file %s %s", fileName, err)
		return UploadResponse{}, errors.New(msg)
	}

	return UploadResponse{
		Path:    path,
		Written: written,
	}, nil
}

// Uploads the incoming byte[] to the hdfs path provided by
// query param 'to'
func upload(w http.ResponseWriter, r *http.Request) {
	// parse params
	fileName := r.URL.Query().Get("fileName")
	to := r.URL.Query().Get("to")
	if to == "" || fileName == "" {
		http.Error(w, "'to', 'fileName', 'dir' query params must be provided.", http.StatusBadRequest)
		return
	}
	log.Printf("Writing %s to target: %s\n", fileName, to)

	// write data from request body into the file
	data := r.Body
	res, err := WriteHDFS(to, fileName, data)
	defer data.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json, _ := json.Marshal(res)
	w.Write(json)
}

// Reads all files in a given directory provided by 'from'
// and uploads them to the user provided path 'to'
func copy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// get params
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
	var (
		writtenBytes int64
		copyFailures = make([]CopyFailure, 0)
		mutex        sync.Mutex
		wg           sync.WaitGroup
	)

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		// filePath := filepath.Join(from, fileInfo.Name())
		wg.Add(1)

		go func(file string) {
			var failure CopyFailure
			defer func() {
				if failure != (CopyFailure{}) {
					mutex.Lock()
					copyFailures = append(copyFailures, failure)
					mutex.Unlock()
				}
			}()
			defer wg.Done()

			path := filepath.Join(from, file)
			log.Printf("Reading from path: %s\n", path)
			reader, err := client.Open(path)
			if err != nil {
				log.Printf("Failed to read file %s\n", file)
				failure = CopyFailure{path, err.Error()}
				return
			}
			defer reader.Close()

			// Create a buffer to store the file contents
			var buf bytes.Buffer
			written, err := io.Copy(&buf, reader)
			if err != nil {
				log.Printf("Failed to read file '%s': %s\n", file, err)
				failure = CopyFailure{path, err.Error()}
				return
			}

			mutex.Lock()
			writtenBytes += written
			mutex.Unlock()

			// Define the URL of Service B to send the file to
			uploadUrl := "http://localhost:8080/upload?fileName=" + file + "&to=" + to

			// Create an HTTP request
			req, err := http.NewRequest(http.MethodPost, uploadUrl, &buf)
			if err != nil {
				log.Printf("Failed to create request for file '%s': %s", file, err)
				failure = CopyFailure{path, err.Error()}
				return
			}

			// Set the Content-Type header
			req.Header.Set("Content-Type", "application/octet-stream")

			// Send the request to /upload
			httpClient := &http.Client{
				Timeout: 15 * time.Minute,
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Printf("Failed to send file '%s' to /upload: %s", file, err)
				failure = CopyFailure{path, err.Error()}
				return
			}
			defer resp.Body.Close()

			// Check the response from Service B
			if resp.StatusCode != http.StatusOK {
				msg := fmt.Sprintf("Service returned non-OK status for file '%s': %d", file, resp.StatusCode)
				log.Println(msg)
				failure = CopyFailure{path, msg}
				return
			}

			log.Printf("File '%s' sent successfully to /upload!", file)

		}(fileInfo.Name())
	}

	wg.Wait()

	elapsed := time.Since(start).Seconds()
	resp := CopyResponse{
		From:           from,
		To:             to,
		Written:        writtenBytes,
		FilesRequested: int64(len(fileInfos)),
		FilesCopied:    int64(len(fileInfos) - len(copyFailures)),
		CopyFailures:   copyFailures,
		Throughput:     (float64(writtenBytes) * 8 / elapsed) / 1000000, // conversion to mbps
		ElapsedSecs:    elapsed,
	}
	json, _ := json.MarshalIndent(resp, "", "  ")
	log.Println(string(json))
	if len(copyFailures) > 0 {
		http.Error(w, string(json), http.StatusInternalServerError)
		return
	}
	log.Printf("Copied %d files successfully.", resp.FilesCopied)
	w.Write(json)
}

// lazy load the hdfs client
func getHdfsClient() *hdfs.Client {
	// namenode := os.Getenv("HDFS_HOST")
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
	// close the hdfs client (this is lazily loaded by the endpoints)
	defer hdfsClient.Close()

	// bind functions to routes
	http.HandleFunc("/copy", copy)
	http.HandleFunc("/upload", upload)
	log.Println("Listening on :8080...")

	// configure server
	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  90 * time.Second,
		WriteTimeout: 15 * time.Minute,
	}

	// start server
	err := srv.ListenAndServe()
	if err != nil {
		log.Fatalf("failed to start http server: %s", err)
	}
}
