package main

import (
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

var httpClient = &http.Client{
	Timeout: 15 * time.Minute,
}

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
	Size   int64  `json:"size"`
}

type CopyArgs struct {
	From string
	File string
	Path string
	To   string
}

func WriteHDFS(to string, fileName string, data io.ReadCloser) (UploadResponse, error) {
	var msg string
	client := GetHdfsClient()
	client.MkdirAll(to, os.FileMode(0755))

	path := filepath.Join(to, fileName)
	client.Remove(path) // Truncate the file to 0 bytes

	file, err := client.Create(path)
	if err != nil {
		msg = fmt.Sprintf("Error creating file in hdfs %s", err)
		return UploadResponse{}, errors.New(msg)
	}
	defer file.Close()

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

func sendToUpload(reader *hdfs.FileReader, targetURL string, args CopyArgs, wg *sync.WaitGroup, ch chan CopyFailure) {
	defer wg.Done()
	uploadUrl := targetURL + "?fileName=" + args.File + "&to=" + args.To

	req, err := http.NewRequest(http.MethodPost, uploadUrl, reader)
	if err != nil {
		log.Printf("Failed to create request for file '%s': %s", args.File, err)
		ch <- CopyFailure{args.Path, err.Error(), reader.Stat().Size()}
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Connection", "keep-alive")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to send file '%s' to /upload: %s", args.File, err)
		ch <- CopyFailure{args.Path, err.Error(), reader.Stat().Size()}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("/upload returned non-OK status for file '%s': %d", args.File, resp.StatusCode)
		log.Println(msg)
		ch <- CopyFailure{args.Path, msg, reader.Stat().Size()}
	}
	log.Printf("File '%s' successfully to copied to target!", args.File)
}

// Uploads the incoming []byte to the hdfs path provided by
// query param 'to' and file provided by param 'fileName'
func handleUpload(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")
	to := r.URL.Query().Get("to")
	if to == "" || fileName == "" {
		http.Error(w, "'to', 'fileName', 'dir' query params must be provided.", http.StatusBadRequest)
		return
	}
	log.Printf("Writing %s to target: %s\n", fileName, to)

	data := r.Body
	res, err := WriteHDFS(to, fileName, data)
	defer data.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error occurred writing to HDFS: %s", err)
		return
	}
	json, _ := json.Marshal(res)
	w.Write(json)
}

// Reads all files in a given directory provided by 'from'
// and uploads them to the user provided path 'to'
func handleCopy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	targetURL := r.URL.Query().Get("targetURL")
	if from == "" || to == "" {
		http.Error(w, "'from', 'to', and 'targetURL' query params must be provided.'", http.StatusBadRequest)
		return
	}

	client := GetHdfsClient()
	fileInfos, err := client.ReadDir(from)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list the hdfs dir %s", err), http.StatusInternalServerError)
		return
	}

	var (
		totalBytesWritten int64
		copyFailuresCh    = make(chan CopyFailure)
		wg                sync.WaitGroup
	)

	copyFailures := make([]CopyFailure, 0)
	go func() {
		for failure := range copyFailuresCh {
			copyFailures = append(copyFailures, failure)
		}
	}()

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		args := CopyArgs{from, fileInfo.Name(), filepath.Join(from, fileInfo.Name()), to}
		totalBytesWritten += fileInfo.Size()
		log.Printf("Reading from path: %s\n", args.Path)
		reader, err := client.Open(args.Path)
		if err != nil {
			log.Printf("Failed to read file %s\n", args.File)
			copyFailuresCh <- CopyFailure{args.Path, err.Error(), fileInfo.Size()}
			return
		}
		defer reader.Close()
		wg.Add(1)
		go sendToUpload(reader, targetURL, args, &wg, copyFailuresCh)
	}
	wg.Wait()

	for _, f := range copyFailures {
		totalBytesWritten -= f.Size
	}

	elapsed := time.Since(start).Seconds()
	resp := CopyResponse{
		From:           from,
		To:             to,
		Written:        totalBytesWritten,
		FilesRequested: int64(len(fileInfos)),
		FilesCopied:    int64(len(fileInfos) - len(copyFailures)),
		CopyFailures:   copyFailures,
		Throughput:     (float64(totalBytesWritten) * 8 / elapsed) / 1000000, // conversion to mbps
		ElapsedSecs:    elapsed,
	}
	json, _ := json.MarshalIndent(resp, "", "  ")
	log.Println(string(json))
	if len(copyFailuresCh) > 0 {
		http.Error(w, string(json), http.StatusInternalServerError)
		return
	}
	log.Printf("Copied %d files successfully.", resp.FilesCopied)
	w.Write(json)
}

func main() {
	defer HdfsClient.Close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("{\"status\":\"200 OK\"}")) })
	http.HandleFunc("/copy", handleCopy)
	http.HandleFunc("/upload", handleUpload)
	log.Println("fastcopy server listening on :8080...")

	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 15 * time.Minute,
		IdleTimeout:  5 * time.Minute,
	}

	err := srv.ListenAndServe()
	if err != nil {
		log.Fatalf("failed to start http server: %s", err)
	}
}
