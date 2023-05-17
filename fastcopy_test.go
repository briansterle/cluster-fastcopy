package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// random test data generator
type RandomReadCloser struct {
	size     int64
	position int64
}

func (r *RandomReadCloser) Read(p []byte) (n int, err error) {
	if r.position >= r.size {
		return 0, io.EOF
	}
	remaining := r.size - r.position
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err = rand.Read(p)
	if err != nil {
		return n, err
	}
	r.position += int64(n)
	return n, nil
}

func (r *RandomReadCloser) Close() error {
	// Perform any cleanup if necessary
	return nil
}

func BenchmarkCopy(b *testing.B) {
	ans := make([]int, 0)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 69; j++ {
			size := int64(j * 1024 * 1024)
			data := &RandomReadCloser{
				size:     size,
				position: 0,
			}
			WriteHDFS("/tmp/bench/", fmt.Sprint(j, "rand.txt"), data)
		}

	}
	fmt.Println(len(ans))
}

func TestUpload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(upload))
	defer server.Close()
	route := "/upload?to=%2Ftmp%2Fin%2F&fileName=hello6.txt"
	req, err := http.NewRequest("POST", server.URL+route, strings.NewReader("hello, world!"))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var data UploadResponse

	json.NewDecoder(resp.Body).Decode(&data)
	var expected int64 = 13
	if data.Written != expected {
		t.Errorf("unexpected bytes written %d, got %d", expected, data.Written)
	}

}
