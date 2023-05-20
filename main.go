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

	client := getHdfsClient(os.Getenv("HDFS_NAMENODE"))

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
func handleUpload(w http.ResponseWriter, r *http.Request) {
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

func sendToUpload(wg *sync.WaitGroup, targetURL string, buf *bytes.Buffer, args CopyArgs, ch chan CopyFailure, size int64) {
	defer wg.Done()
	uploadUrl := targetURL + "?fileName=" + args.File + "&to=" + args.To

	// Create an HTTP request
	req, err := http.NewRequest(http.MethodPost, uploadUrl, buf)
	if err != nil {
		log.Printf("Failed to create request for file '%s': %s", args.File, err)
		ch <- CopyFailure{args.Path, err.Error(), size}
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Connection", "keep-alive")

	// Send the request to /upload
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to send file '%s' to /upload: %s", args.File, err)
		ch <- CopyFailure{args.Path, err.Error(), size}
	}
	defer resp.Body.Close()

	// Check the response /upload
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("/upload returned non-OK status for file '%s': %d", args.File, resp.StatusCode)
		log.Println(msg)
		ch <- CopyFailure{args.Path, msg, size}
	}
	log.Printf("File '%s' sent successfully to /upload!", args.File)
}

// Reads all files in a given directory provided by 'from'
// and uploads them to the user provided path 'to'
func handleCopy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// get query params
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	targetURL := r.URL.Query().Get("targetURL")
	if from == "" || to == "" {
		http.Error(w, "'from', 'to', and 'targetURL' query params must be provided.'", http.StatusBadRequest)
		return
	}

	client := getHdfsClient(os.Getenv("HDFS_NAMENODE"))
	fileInfos, err := client.ReadDir(from)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list the hdfs dir %s", err), http.StatusInternalServerError)
		return
	}

	var (
		totalBytesWritten int64
		copyFailuresCh    = make(chan CopyFailure)
		wg                sync.WaitGroup // Wait group to synchronize goroutines
	)

	// collect all copy failures
	copyFailures := make([]CopyFailure, 0)
	go func() {
		for failure := range copyFailuresCh {
			copyFailures = append(copyFailures, failure)
		}
	}()

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() { // skip dirs for now
			continue
		}
		file, size := fileInfo.Name(), fileInfo.Size()
		totalBytesWritten += size // if any writes fail, we subtract at the end

		args := CopyArgs{from, file, filepath.Join(from, file), to}

		log.Printf("Reading from path: %s\n", args.Path)
		reader, err := client.Open(args.Path)
		if err != nil {
			log.Printf("Failed to read file %s\n", file)

			copyFailuresCh <- CopyFailure{args.Path, err.Error(), size}
			return
		}
		defer reader.Close()

		// profiling-tuned bufSize eliminates growSlices
		bufSize := int(float64(size) * 1.0000765)
		buf := bytes.NewBuffer(make([]byte, 0, bufSize))

		_, err = io.Copy(buf, reader)
		if err != nil {
			log.Printf("Failed to read file '%s': %s\n", file, err)
			copyFailuresCh <- CopyFailure{args.Path, err.Error(), size}
			return
		}

		wg.Add(1)
		// send the buffer to the /upload endpoint
		go sendToUpload(&wg, targetURL, buf, args, copyFailuresCh, size)
	}
	wg.Wait() // wait for all goroutines to complete

	for _, f := range copyFailures {
		totalBytesWritten -= f.Size // subtract bytes from any failed copy
	}

	elapsed := time.Since(start).Seconds()
	resp := CopyResponse{
		From:           from,
		To:             to,
		Written:        totalBytesWritten, // todo update this by subtracting bytes of copy failures
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

// lazy load the hdfs client
func getHdfsClient(namenode string) *hdfs.Client {
	if namenode == "" {
		namenode = "localhost:9000"
	}
	if hdfsClient == nil {
		client, err := hdfs.New(namenode)
		if err != nil {
			log.Fatalf("failed to create hdfs client: %s", err)
		}
		hdfsClient = client
	}
	return hdfsClient
}

func main() {
	// defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.TraceProfile, profile.ProfilePath(".")).Stop()

	// close the hdfs client (this is lazily loaded by the endpoints)
	defer hdfsClient.Close()

	// bind functions to routes
	http.HandleFunc("/copy", handleCopy)
	http.HandleFunc("/upload", handleUpload)
	log.Println("Listening on :8080...")

	// configure server
	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 15 * time.Minute,
		IdleTimeout:  5 * time.Minute, // Set the idle timeout for keep-alive connections
	}

	// start server
	err := srv.ListenAndServe()
	if err != nil {
		log.Fatalf("failed to start http server: %s", err)
	}
}
