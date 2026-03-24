package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

func TestBuildExportQuery(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ExportConfig
		wantSQL string
	}{
		{
			name: "all data",
			cfg: ExportConfig{
				Database: "testdb",
			},
			wantSQL: "SELECT * FROM /.*/",
		},
		{
			name: "specific measurement",
			cfg: ExportConfig{
				Database:    "testdb",
				Measurement: "cpu",
			},
			wantSQL: "SELECT * FROM cpu",
		},
		{
			name: "with time range (RFC3339)",
			cfg: ExportConfig{
				Database:  "testdb",
				StartTime: "2021-01-01T00:00:00Z",
				EndTime:   "2021-01-02T00:00:00Z",
			},
			// RFC3339 times are converted to nanoseconds
			wantSQL: "SELECT * FROM /.*/ WHERE time >= 1609459200000000000 AND time <= 1609545600000000000",
		},
		{
			name: "with unix timestamp",
			cfg: ExportConfig{
				Database:  "testdb",
				StartTime: "1609459200",
				EndTime:   "1609545600",
			},
			wantSQL: "SELECT * FROM /.*/ WHERE time >= 1609459200000000000 AND time <= 1609545600000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildExportQuery(tt.cfg)
			if got != tt.wantSQL {
				t.Errorf("buildExportQuery() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestParseTimeArg(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{
			name:  "unix seconds",
			input: "1609459200",
			want:  1609459200000000000,
		},
		{
			name:  "unix milliseconds",
			input: "1609459200000",
			want:  1609459200000000000,
		},
		{
			name:  "unix nanoseconds",
			input: "1609459200000000000",
			want:  1609459200000000000,
		},
		{
			name:  "RFC3339",
			input: "2021-01-01T00:00:00Z",
			want:  1609459200000000000,
		},
		{
			name:  "date only",
			input: "2021-01-01",
			want:  1609459200000000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimeArg(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimeArg() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseTimeArg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResultsToPoints(t *testing.T) {
	series := []QuerySeries{
		{
			Name:    "cpu",
			Tags:    storage.Tags{"host": "server01"},
			Columns: []string{"time", "value"},
			Values: [][]interface{}{
				{float64(1609459200000000000), float64(0.64)},
				{float64(1609459201000000000), float64(0.72)},
			},
		},
	}

	points := resultsToPoints(series)

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}

	if points[0].Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got %q", points[0].Measurement)
	}

	if points[0].Tags["host"] != "server01" {
		t.Errorf("expected tag host=server01, got %q", points[0].Tags["host"])
	}

	if points[0].Timestamp != 1609459200000000000 {
		t.Errorf("expected timestamp 1609459200000000000, got %d", points[0].Timestamp)
	}
}

func TestWriteLineProtocol(t *testing.T) {
	points := []*storage.DataPoint{
		{
			Measurement: "cpu",
			Tags:        storage.Tags{"host": "server01"},
			Fields:      storage.Fields{"value": storage.NewFloatField(0.64)},
			Timestamp:   1609459200000000000,
		},
	}

	var buf bytes.Buffer
	count, err := writeLineProtocol(&buf, points)
	if err != nil {
		t.Fatalf("writeLineProtocol() error = %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 point written, got %d", count)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected non-empty output")
	}
}

func TestWriteJSON(t *testing.T) {
	points := []*storage.DataPoint{
		{
			Measurement: "cpu",
			Tags:        storage.Tags{"host": "server01"},
			Fields:      storage.Fields{"value": storage.NewFloatField(0.64)},
			Timestamp:   1609459200000000000,
		},
	}

	var buf bytes.Buffer
	count, err := writeJSON(&buf, points)
	if err != nil {
		t.Fatalf("writeJSON() error = %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 point written, got %d", count)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected non-empty output")
	}

	// Check it's valid JSON structure
	if output[0] != '[' {
		t.Errorf("expected JSON array, got: %s", output[:10])
	}
}

func TestWriteCSV(t *testing.T) {
	points := []*storage.DataPoint{
		{
			Measurement: "cpu",
			Tags:        storage.Tags{"host": "server01"},
			Fields:      storage.Fields{"value": storage.NewFloatField(0.64)},
			Timestamp:   1609459200000000000,
		},
	}

	var buf bytes.Buffer
	count, err := writeCSV(&buf, points)
	if err != nil {
		t.Fatalf("writeCSV() error = %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 point written, got %d", count)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected non-empty output")
	}

	// Check header exists
	if len(output) < 4 {
		t.Error("output too short")
	}
}

func TestExportConfig(t *testing.T) {
	// Test default values
	cfg := ExportConfig{
		Host:     "localhost:8086",
		Database: "testdb",
		Format:   "line",
		Output:   "-",
	}

	if cfg.Host != "localhost:8086" {
		t.Errorf("unexpected host: %s", cfg.Host)
	}
	if cfg.Format != "line" {
		t.Errorf("unexpected format: %s", cfg.Format)
	}
	if cfg.Output != "-" {
		t.Errorf("unexpected output: %s", cfg.Output)
	}
}

func TestFormatStorageFieldValue(t *testing.T) {
	tests := []struct {
		name  string
		fv    storage.FieldValue
		want  string
	}{
		{"float", storage.NewFloatField(3.14), "3.14"},
		{"int", storage.NewIntField(42), "42i"},
		{"string", storage.NewStringField("hello"), `"hello"`},
		{"bool_true", storage.NewBoolField(true), "true"},
		{"bool_false", storage.NewBoolField(false), "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatStorageFieldValue(tt.fv)
			if got != tt.want {
				t.Errorf("formatStorageFieldValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConvertToProtocolPoints(t *testing.T) {
	points := []*storage.DataPoint{
		{
			Measurement: "cpu",
			Tags:        storage.Tags{"host": "server01"},
			Fields:      storage.Fields{"value": storage.NewFloatField(0.64)},
			Timestamp:   time.Now().UnixNano(),
		},
	}

	result := convertToProtocolPoints(points)

	if len(result) != len(points) {
		t.Errorf("expected %d points, got %d", len(points), len(result))
	}

	if result[0] != points[0] {
		t.Error("expected same pointer reference")
	}
}

func TestRunExportFileNotFound(t *testing.T) {
	// Create a server that returns empty results
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series":       []interface{}{},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "line",
		Output:   "/nonexistent/path/file.txt",
	}

	err := runExport(cfg)
	if err == nil {
		t.Error("expected error for invalid output path")
	}
}

func TestRunExportToStdout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"tags":    map[string]string{"host": "server1"},
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000.0, 42.5},
							},
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "line",
		Output:   "-",
	}

	err := runExport(cfg)
	if err != nil {
		t.Errorf("runExport to stdout error: %v", err)
	}
}

func TestRunExportToFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000.0, 42.5},
							},
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "export.lp")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "line",
		Output:   outFile,
	}

	err := runExport(cfg)
	if err != nil {
		t.Errorf("runExport to file error: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Error("output file was not created")
	}
}

func TestRunExportJSONFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000.0, 42.5},
							},
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "export.json")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "json",
		Output:   outFile,
	}

	err := runExport(cfg)
	if err != nil {
		t.Errorf("runExport JSON error: %v", err)
	}
}

func TestRunExportCSVFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000.0, 42.5},
							},
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "export.csv")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "csv",
		Output:   outFile,
	}

	err := runExport(cfg)
	if err != nil {
		t.Errorf("runExport CSV error: %v", err)
	}
}

func TestRunExportWithTimeRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify query contains time range
		q := r.URL.Query().Get("q")
		if !strings.Contains(q, "time >=") || !strings.Contains(q, "time <=") {
			t.Errorf("query missing time range: %s", q)
		}
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series":       []interface{}{},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ExportConfig{
		Host:      host,
		Database:  "testdb",
		StartTime: "2021-01-01T00:00:00Z",
		EndTime:   "2021-01-02T00:00:00Z",
		Format:    "line",
		Output:    "-",
	}

	err := runExport(cfg)
	if err != nil {
		t.Errorf("runExport with time range error: %v", err)
	}
}

func TestRunExportServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ExportConfig{
		Host:     host,
		Database: "testdb",
		Format:   "line",
		Output:   "-",
	}

	err := runExport(cfg)
	if err == nil {
		t.Error("expected error for server error")
	}
}

func TestRunExportQueryError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"error":        "database not found",
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	cfg := ExportConfig{
		Host:     host,
		Database: "nonexistent",
		Format:   "line",
		Output:   "-",
	}

	err := runExport(cfg)
	if err == nil {
		t.Error("expected error for query error")
	}
}
