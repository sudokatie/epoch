package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sudokatie/epoch/internal/storage"
)

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		{"data.json", "json"},
		{"data.JSON", "json"},
		{"data.csv", "csv"},
		{"data.CSV", "csv"},
		{"data.txt", "line"},
		{"data.lp", "line"},
		{"data", "line"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := detectFormat(tt.filename)
			if got != tt.want {
				t.Errorf("detectFormat(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

func TestImportLineProtocol(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	content := `cpu,host=server01 value=0.64 1609459200000000000
cpu,host=server02 value=0.72 1609459200000000000
memory,host=server01 value=65.4 1609459200000000000`

	reader := strings.NewReader(content)
	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	points, bytesRead, err := importLineProtocol(reader, client, writeURL, 100)
	if err != nil {
		t.Fatalf("importLineProtocol error: %v", err)
	}
	if points != 3 {
		t.Errorf("points = %d, want 3", points)
	}
	if bytesRead == 0 {
		t.Error("bytesRead should be > 0")
	}
}

func TestImportJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	content := `[
		{"measurement": "cpu", "tags": {"host": "server01"}, "fields": {"value": 0.64}, "timestamp": 1609459200000000000},
		{"measurement": "cpu", "tags": {"host": "server02"}, "fields": {"value": 0.72}, "timestamp": 1609459200000000000}
	]`

	reader := strings.NewReader(content)
	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	points, bytesRead, err := importJSON(reader, client, writeURL, 100)
	if err != nil {
		t.Fatalf("importJSON error: %v", err)
	}
	if points != 2 {
		t.Errorf("points = %d, want 2", points)
	}
	if bytesRead == 0 {
		t.Error("bytesRead should be > 0")
	}
}

func TestImportCSV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	content := `time,host,value
1609459200000000000,server01,0.64
1609459200000000000,server02,0.72`

	reader := strings.NewReader(content)
	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	cfg := ImportConfig{
		Measurement: "cpu",
		TagColumns:  "host",
		BatchSize:   100,
	}

	points, bytesRead, err := importCSV(reader, client, writeURL, cfg)
	if err != nil {
		t.Fatalf("importCSV error: %v", err)
	}
	if points != 2 {
		t.Errorf("points = %d, want 2", points)
	}
	if bytesRead == 0 {
		t.Error("bytesRead should be > 0")
	}
}

func TestSendBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	batch := []byte("cpu,host=server1 value=1\ncpu,host=server2 value=2")

	err := sendBatch(client, writeURL, batch)
	if err != nil {
		t.Errorf("sendBatch error: %v", err)
	}
}

func TestSendBatchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	batch := []byte("cpu value=1")

	err := sendBatch(client, writeURL, batch)
	if err == nil {
		t.Error("expected error from sendBatch")
	}
}

func TestFormatLineProtocol(t *testing.T) {
	point := &storage.DataPoint{
		Measurement: "cpu",
		Tags:        storage.Tags{"host": "server1", "region": "us-west"},
		Fields:      storage.Fields{"value": storage.NewFloatField(42.5), "count": storage.NewIntField(10)},
		Timestamp:   1609459200000000000,
	}

	line := formatLineProtocol(point)
	if !strings.HasPrefix(line, "cpu,") {
		t.Errorf("line should start with 'cpu,': %s", line)
	}
	if !strings.Contains(line, "host=server1") {
		t.Errorf("line should contain host tag: %s", line)
	}
	if !strings.Contains(line, "value=42.5") {
		t.Errorf("line should contain value field: %s", line)
	}
	if !strings.HasSuffix(line, "1609459200000000000") {
		t.Errorf("line should end with timestamp: %s", line)
	}
}

func TestFormatImportFieldValue(t *testing.T) {
	tests := []struct {
		name  string
		input storage.FieldValue
		want  string
	}{
		{"float64", storage.NewFloatField(3.14), "3.14"},
		{"int64", storage.NewIntField(42), "42i"},
		{"string", storage.NewStringField("hello"), `"hello"`},
		{"bool_true", storage.NewBoolField(true), "true"},
		{"bool_false", storage.NewBoolField(false), "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatImportFieldValue(tt.input)
			if got != tt.want {
				t.Errorf("formatImportFieldValue(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRunImportFileNotFound(t *testing.T) {
	cfg := ImportConfig{
		Host:      "localhost:8086",
		File:      "/nonexistent/file.csv",
		Database:  "testdb",
		Format:    "auto",
		BatchSize: 100,
	}

	err := runImport(cfg)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestRunImportWithMockServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	// Create temp file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.lp")
	content := `cpu,host=server01 value=0.64 1609459200000000000
cpu,host=server02 value=0.72 1609459200000000000`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Extract host from server URL
	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ImportConfig{
		Host:        host,
		File:        testFile,
		Database:    "testdb",
		Measurement: "cpu",
		Format:      "line",
		BatchSize:   100,
	}

	err := runImport(cfg)
	if err != nil {
		t.Errorf("runImport error: %v", err)
	}
}

func TestRunImportJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.json")
	content := `[{"measurement":"cpu","tags":{"host":"server01"},"fields":{"value":0.64},"timestamp":1609459200000000000}]`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ImportConfig{
		Host:      host,
		File:      testFile,
		Database:  "testdb",
		Format:    "json",
		BatchSize: 100,
	}

	err := runImport(cfg)
	if err != nil {
		t.Errorf("runImport JSON error: %v", err)
	}
}

func TestRunImportCSV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := `time,host,value
1609459200000000000,server01,0.64
1609459200000000000,server02,0.72`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ImportConfig{
		Host:        host,
		File:        testFile,
		Database:    "testdb",
		Measurement: "cpu",
		TagColumns:  "host",
		Format:      "csv",
		BatchSize:   100,
	}

	err := runImport(cfg)
	if err != nil {
		t.Errorf("runImport CSV error: %v", err)
	}
}

func TestRunImportUnsupportedFormat(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg := ImportConfig{
		Host:      "localhost:8086",
		File:      testFile,
		Database:  "testdb",
		Format:    "invalid",
		BatchSize: 100,
	}

	err := runImport(cfg)
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}

func TestImportCSVWithTagColumns(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	content := `time,host,region,value
1609459200000000000,server01,us-west,0.64
1609459200000000000,server02,us-east,0.72`

	reader := strings.NewReader(content)
	client := server.Client()
	writeURL := server.URL + "/write?db=testdb"

	cfg := ImportConfig{
		Measurement: "cpu",
		TagColumns:  "host,region",
		BatchSize:   100,
	}

	_, _, err := importCSV(reader, client, writeURL, cfg)
	if err != nil {
		t.Errorf("importCSV error: %v", err)
	}
}

func TestFormatLineProtocolWithStringField(t *testing.T) {
	point := &storage.DataPoint{
		Measurement: "logs",
		Tags:        storage.Tags{"level": "error"},
		Fields:      storage.Fields{"message": storage.NewStringField("test message")},
		Timestamp:   1609459200000000000,
	}

	line := formatLineProtocol(point)
	if !strings.Contains(line, `message="test message"`) {
		t.Errorf("line should contain string field: %s", line)
	}
}

func TestFormatLineProtocolWithBoolField(t *testing.T) {
	point := &storage.DataPoint{
		Measurement: "status",
		Tags:        storage.Tags{"service": "api"},
		Fields:      storage.Fields{"healthy": storage.NewBoolField(true)},
		Timestamp:   1609459200000000000,
	}

	line := formatLineProtocol(point)
	if !strings.Contains(line, "healthy=true") {
		t.Errorf("line should contain bool field: %s", line)
	}
}

func TestImportConfig(t *testing.T) {
	cfg := ImportConfig{
		Host:        "localhost:8086",
		File:        "test.csv",
		Database:    "testdb",
		Measurement: "cpu",
		Format:      "auto",
		BatchSize:   5000,
		TagColumns:  "host,region",
	}

	if cfg.Host != "localhost:8086" {
		t.Errorf("unexpected host: %s", cfg.Host)
	}
	if cfg.BatchSize != 5000 {
		t.Errorf("unexpected batch size: %d", cfg.BatchSize)
	}
	if cfg.TagColumns != "host,region" {
		t.Errorf("unexpected tag columns: %s", cfg.TagColumns)
	}
}
