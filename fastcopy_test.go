package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
