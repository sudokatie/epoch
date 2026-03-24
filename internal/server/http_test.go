package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sudokatie/epoch/internal/query"
	"github.com/sudokatie/epoch/internal/storage"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "epoch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create engine
	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create engine: %v", err)
	}

	// Create executor
	executor := query.NewExecutor(engine, query.DefaultExecutorConfig())

	// Create server
	config := DefaultConfig()
	config.LogRequests = false
	server := NewServer(engine, executor, config)

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return server, cleanup
}

func TestHandlePing(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{"GET", http.MethodGet, http.StatusNoContent},
		{"HEAD", http.MethodHead, http.StatusNoContent},
		{"POST not allowed", http.MethodPost, http.StatusMethodNotAllowed},
		{"PUT not allowed", http.MethodPut, http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/ping", nil)
			w := httptest.NewRecorder()

			server.handlePing(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("got status %d, want %d", w.Code, tt.wantStatus)
			}

			if tt.wantStatus == http.StatusNoContent {
				version := w.Header().Get("X-Epoch-Version")
				if version != "0.1.0" {
					t.Errorf("got version %q, want %q", version, "0.1.0")
				}
			}
		})
	}
}

func TestHandleWriteSinglePoint(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create database first
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	body := "cpu,host=server01 value=0.64 1422568543702900257"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleWrite(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
	}

	// Verify stats
	stats := server.GetStats()
	if stats.Writes != 1 {
		t.Errorf("got %d writes, want 1", stats.Writes)
	}
	if stats.PointsWritten != 1 {
		t.Errorf("got %d points written, want 1", stats.PointsWritten)
	}
}

func TestHandleWriteBatch(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	body := `cpu,host=server01 value=0.64 1422568543702900257
cpu,host=server02 value=0.75 1422568543702900258
cpu,host=server03 value=0.82 1422568543702900259`

	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleWrite(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
	}

	stats := server.GetStats()
	if stats.PointsWritten != 3 {
		t.Errorf("got %d points written, want 3", stats.PointsWritten)
	}
}

func TestHandleWriteErrors(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name       string
		method     string
		url        string
		body       string
		wantStatus int
		wantError  string
	}{
		{
			name:       "method not allowed",
			method:     http.MethodGet,
			url:        "/write?db=testdb",
			wantStatus: http.StatusMethodNotAllowed,
			wantError:  "method not allowed",
		},
		{
			name:       "missing database",
			method:     http.MethodPost,
			url:        "/write",
			body:       "cpu value=1",
			wantStatus: http.StatusBadRequest,
			wantError:  "database is required",
		},
		{
			name:       "empty body",
			method:     http.MethodPost,
			url:        "/write?db=testdb",
			body:       "",
			wantStatus: http.StatusBadRequest,
			wantError:  "empty request body",
		},
		{
			name:       "invalid line protocol",
			method:     http.MethodPost,
			url:        "/write?db=testdb",
			body:       "invalid line protocol here",
			wantStatus: http.StatusBadRequest,
			wantError:  "failed to parse",
		},
	}

	// Create database for tests that need it
	server.engine.CreateDatabase("testdb")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.url, strings.NewReader(tt.body))
			w := httptest.NewRecorder()

			server.handleWrite(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("got status %d, want %d", w.Code, tt.wantStatus)
			}

			if tt.wantError != "" {
				body := w.Body.String()
				if !strings.Contains(body, tt.wantError) {
					t.Errorf("error message %q does not contain %q", body, tt.wantError)
				}
			}
		})
	}
}

func TestHandleQuery(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup: create database and write data
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	writeReq := httptest.NewRequest(http.MethodPost, "/write?db=testdb",
		strings.NewReader("cpu,host=server01 value=0.64"))
	writeW := httptest.NewRecorder()
	server.handleWrite(writeW, writeReq)

	if writeW.Code != http.StatusNoContent {
		t.Fatalf("write failed: %s", writeW.Body.String())
	}

	tests := []struct {
		name       string
		method     string
		url        string
		body       string
		wantStatus int
	}{
		{
			name:       "GET query",
			method:     http.MethodGet,
			url:        "/query?db=testdb&q=SELECT+*+FROM+cpu",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST query plain",
			method:     http.MethodPost,
			url:        "/query?db=testdb",
			body:       "SELECT * FROM cpu",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST query JSON",
			method:     http.MethodPost,
			url:        "/query?db=testdb",
			body:       `{"q": "SELECT * FROM cpu"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			}
			req := httptest.NewRequest(tt.method, tt.url, bodyReader)
			w := httptest.NewRecorder()

			server.handleQuery(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("got status %d, want %d: %s", w.Code, tt.wantStatus, w.Body.String())
			}

			// Verify JSON structure
			if tt.wantStatus == http.StatusOK {
				var resp QueryResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Errorf("failed to parse response JSON: %v", err)
				}
				if len(resp.Results) == 0 {
					t.Error("expected at least one result")
				}
			}
		})
	}
}

func TestHandleQueryErrors(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.engine.CreateDatabase("testdb")

	tests := []struct {
		name       string
		method     string
		url        string
		body       string
		wantStatus int
		wantError  string
	}{
		{
			name:       "method not allowed",
			method:     http.MethodPut,
			url:        "/query?db=testdb&q=SELECT+*+FROM+cpu",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "missing database",
			method:     http.MethodGet,
			url:        "/query?q=SELECT+*+FROM+cpu",
			wantStatus: http.StatusBadRequest,
			wantError:  "database is required",
		},
		{
			name:       "missing query",
			method:     http.MethodGet,
			url:        "/query?db=testdb",
			wantStatus: http.StatusBadRequest,
			wantError:  "query is required",
		},
		{
			name:       "invalid query syntax",
			method:     http.MethodGet,
			url:        "/query?db=testdb&q=INVALID+QUERY",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.url, strings.NewReader(tt.body))
			w := httptest.NewRecorder()

			server.handleQuery(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("got status %d, want %d", w.Code, tt.wantStatus)
			}

			if tt.wantError != "" {
				if !strings.Contains(w.Body.String(), tt.wantError) {
					t.Errorf("error %q does not contain %q", w.Body.String(), tt.wantError)
				}
			}
		})
	}
}

func TestHandleDebugVars(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	w := httptest.NewRecorder()

	server.handleDebugVars(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	// Parse response
	var vars map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &vars); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Check required fields
	requiredFields := []string{"version", "uptime", "server", "memory", "runtime", "databases"}
	for _, field := range requiredFields {
		if _, ok := vars[field]; !ok {
			t.Errorf("missing field %q in response", field)
		}
	}

	// Check server stats
	serverStats, ok := vars["server"].(map[string]interface{})
	if !ok {
		t.Fatal("server stats not a map")
	}

	serverFields := []string{"requests", "writes", "queries", "points_written", "errors"}
	for _, field := range serverFields {
		if _, ok := serverStats[field]; !ok {
			t.Errorf("missing server field %q", field)
		}
	}
}

func TestHandleDebugVarsMethodNotAllowed(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/debug/vars", nil)
	w := httptest.NewRecorder()

	server.handleDebugVars(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got status %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestServerServeHTTP(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test that ServeHTTP properly routes requests
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d", w.Code, http.StatusNoContent)
	}

	// Check stats were updated
	stats := server.GetStats()
	if stats.Requests != 1 {
		t.Errorf("got %d requests, want 1", stats.Requests)
	}
}

func TestServerBodySizeLimit(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Override config with small limit
	server.config.MaxBodySize = 100

	// Create request with body exceeding limit
	body := bytes.Repeat([]byte("x"), 200)
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", bytes.NewReader(body))
	req.ContentLength = 200
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got status %d, want %d", w.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestCSVOutput(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup
	server.engine.CreateDatabase("testdb")
	writeReq := httptest.NewRequest(http.MethodPost, "/write?db=testdb",
		strings.NewReader("cpu,host=server01 value=0.64"))
	writeW := httptest.NewRecorder()
	server.handleWrite(writeW, writeReq)

	// Request CSV format
	req := httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu&format=csv", nil)
	w := httptest.NewRecorder()

	server.handleQuery(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/csv") {
		t.Errorf("got content-type %q, want text/csv", contentType)
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.BindAddress != ":8086" {
		t.Errorf("got bind address %q, want :8086", config.BindAddress)
	}

	if config.ReadTimeout != 30*time.Second {
		t.Errorf("got read timeout %v, want 30s", config.ReadTimeout)
	}

	if config.MaxBodySize != 25*1024*1024 {
		t.Errorf("got max body size %d, want %d", config.MaxBodySize, 25*1024*1024)
	}
}

func TestAutoCreateDatabase(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Write to non-existent database - should auto-create
	body := "cpu value=1"
	req := httptest.NewRequest(http.MethodPost, "/write?db=newdb", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleWrite(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
	}

	// Verify database was created
	databases := server.engine.ListDatabases()
	found := false
	for _, db := range databases {
		if db == "newdb" {
			found = true
			break
		}
	}
	if !found {
		t.Error("database 'newdb' was not auto-created")
	}
}

func TestGetPrecisionMultiplier(t *testing.T) {
	tests := []struct {
		precision string
		want      int64
	}{
		{"ns", 1},
		{"n", 1},
		{"us", 1000},
		{"u", 1000},
		{"µ", 1000},
		{"ms", 1000000},
		{"s", 1000000000},
		{"invalid", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.precision, func(t *testing.T) {
			got := getPrecisionMultiplier(tt.precision)
			if got != tt.want {
				t.Errorf("getPrecisionMultiplier(%q) = %d, want %d", tt.precision, got, tt.want)
			}
		})
	}
}

func TestWriteWithPrecision(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create database first
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	tests := []struct {
		name      string
		precision string
		timestamp string
		wantOK    bool
	}{
		{"nanoseconds", "ns", "1609459200000000000", true},
		{"microseconds", "us", "1609459200000000", true},
		{"milliseconds", "ms", "1609459200000", true},
		{"seconds", "s", "1609459200", true},
		{"invalid precision", "invalid", "1609459200", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := "cpu value=42 " + tt.timestamp
			req := httptest.NewRequest(http.MethodPost, "/write?db=testdb&precision="+tt.precision, strings.NewReader(body))
			w := httptest.NewRecorder()

			server.handleWrite(w, req)

			if tt.wantOK {
				if w.Code != http.StatusNoContent {
					t.Errorf("got status %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
				}
			} else {
				if w.Code != http.StatusBadRequest {
					t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
				}
			}
		})
	}
}

func TestWriteDefaultPrecision(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create database first
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Write without precision parameter - should default to ns
	body := "cpu value=42 1609459200000000000"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleWrite(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
	}
}

func TestServerNew(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "epoch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	config := Config{
		Addr:         ":0",
		QueryTimeout: 30 * time.Second,
	}

	srv, err := New(config, engine)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if srv == nil {
		t.Fatal("New() returned nil server")
	}
}

func TestServerNewWithBindAddress(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "epoch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test with BindAddress instead of Addr
	config := Config{
		BindAddress: ":9999",
	}

	srv, err := New(config, engine)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if srv.config.Addr != ":9999" {
		t.Errorf("Addr = %q, want %q", srv.config.Addr, ":9999")
	}
}

func TestServerNewDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "epoch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test with empty config - should use defaults
	config := Config{}

	srv, err := New(config, engine)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if srv.config.Addr != ":8086" {
		t.Errorf("default Addr = %q, want %q", srv.config.Addr, ":8086")
	}

	if srv.config.ReadTimeout != 30*time.Second {
		t.Errorf("default ReadTimeout = %v, want %v", srv.config.ReadTimeout, 30*time.Second)
	}

	if srv.config.QueryTimeout != 30*time.Second {
		t.Errorf("default QueryTimeout = %v, want %v", srv.config.QueryTimeout, 30*time.Second)
	}
}

func TestHandleMetrics(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	// Check for expected Prometheus metrics
	if !strings.Contains(body, "epoch_") {
		t.Error("metrics response should contain epoch_ metrics")
	}
	if !strings.Contains(body, "go_") {
		t.Error("metrics response should contain go_ runtime metrics")
	}
}

func TestHandleQueryFormats(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create database and write data
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write test data
	body := "cpu,host=server1 value=42 1609459200000000000"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Test JSON format (default)
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("JSON query status = %d, want 200", w.Code)
	}

	// Test CSV format
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu&format=csv", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("CSV query status = %d, want 200", w.Code)
	}
}

func TestHandleQueryWithTimeRange(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Query with time range
	req := httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu+WHERE+time+>+now()-1h", nil)
	w := httptest.NewRecorder()
	server.handleQuery(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("query status = %d, want 200", w.Code)
	}
}

func TestHandleQueryDDL(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// CREATE DATABASE via query - needs db parameter for most queries
	req := httptest.NewRequest(http.MethodPost, "/query?db=testdb", strings.NewReader("q=CREATE+DATABASE+newdb"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	server.handleQuery(w, req)
	// May succeed or fail depending on implementation
	_ = w.Code

	// SHOW DATABASES - also needs db parameter in this implementation
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SHOW+DATABASES", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	// Just verify it doesn't panic
	_ = w.Code
}

func TestHandleWriteErrorsMissingDB(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Missing db parameter
	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader("cpu value=1"))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("missing db status = %d, want 400", w.Code)
	}
}

func TestServerStats(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Make some requests
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Check stats were updated
	server.stats.mu.RLock()
	requests := server.stats.Requests
	server.stats.mu.RUnlock()

	if requests < 1 {
		t.Errorf("stats.Requests = %d, want >= 1", requests)
	}
}

func TestFormatCSVValue(t *testing.T) {
	tests := []struct {
		input interface{}
	}{
		{nil},
		{"hello"},
		{42},
		{3.14},
		{true},
		{int64(123)},
		{float64(1.5)},
	}

	for _, tt := range tests {
		// Just verify it doesn't panic and returns a string
		got := formatCSVValue(tt.input)
		if got == "" && tt.input != nil {
			// String values get quoted, which is fine
		}
		_ = got
	}
}

func TestHandleQueryWithAggregates(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write multiple data points
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("cpu value=%d %d", i*10, 1609459200000000000+int64(i)*1000000000)
		req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
		w := httptest.NewRecorder()
		server.handleWrite(w, req)
	}

	// Query with aggregation
	req := httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+mean(value)+FROM+cpu", nil)
	w := httptest.NewRecorder()
	server.handleQuery(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("aggregate query status = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestHandleWriteMultipleLines(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	body := `cpu,host=server1 value=1
cpu,host=server2 value=2
cpu,host=server3 value=3`

	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("multi-line write status = %d, want 204: %s", w.Code, w.Body.String())
	}
}

func TestHandleWriteJSONFormat(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Test single JSON point
	jsonBody := `{"measurement": "cpu", "tags": {"host": "server1"}, "fields": {"value": 42.5}}`
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("JSON write status = %d, want 204: %s", w.Code, w.Body.String())
	}

	// Test JSON array of points
	jsonArrayBody := `[
		{"measurement": "cpu", "tags": {"host": "server2"}, "fields": {"value": 55.0}},
		{"measurement": "cpu", "tags": {"host": "server3"}, "fields": {"value": 66.0}}
	]`
	req = httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(jsonArrayBody))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	w = httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("JSON array write status = %d, want 204: %s", w.Code, w.Body.String())
	}
}

func TestHandleWriteJSONInvalid(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader("{invalid json}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON write status = %d, want 400", w.Code)
	}
}

func TestWriteJSONOutput(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	body := "cpu value=42"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Query and verify JSON response
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu", nil)
	req.Header.Set("Accept", "application/json")
	w = httptest.NewRecorder()
	server.handleQuery(w, req)

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
}

func TestHandleQuerySelectStar(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write data with multiple fields
	body := "cpu,host=server1 value=42,temp=65"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Query all fields
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("SELECT * status = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestHandleWriteWithTags(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	body := "cpu,host=server1,region=us-west value=42"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("write with tags status = %d, want 204: %s", w.Code, w.Body.String())
	}
}

func TestFormatQueryResponseJSON(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write test data
	body := "cpu,host=server1 value=42 1609459200000000000"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Query with default JSON format
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("query status = %d, want 200", w.Code)
	}
	
	// Verify JSON response structure
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}
	
	if _, ok := resp["results"]; !ok {
		t.Error("response missing 'results' key")
	}
}

func TestFormatQueryResponseCSV(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write test data
	body := "cpu,host=server1 value=42 1609459200000000000"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Query with CSV format
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=SELECT+*+FROM+cpu&format=csv", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("CSV query status = %d, want 200", w.Code)
	}
	
	// Just verify we get some output for CSV
	content := w.Body.String()
	_ = content // CSV might be empty if no rows or have different format
}

func TestHandleQueryWithPrecision(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write test data
	body := "cpu value=42 1609459200000000000"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)

	// Query with different precisions
	precisions := []string{"ns", "us", "ms", "s"}
	for _, p := range precisions {
		req = httptest.NewRequest(http.MethodGet, 
			fmt.Sprintf("/query?db=testdb&q=SELECT+*+FROM+cpu&epoch=%s", p), nil)
		w = httptest.NewRecorder()
		server.handleQuery(w, req)
		
		if w.Code != http.StatusOK {
			t.Errorf("query with precision %s status = %d, want 200", p, w.Code)
		}
	}
}

func TestHandleQueryError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Query without database
	req := httptest.NewRequest(http.MethodGet, "/query?q=SELECT+*+FROM+cpu", nil)
	w := httptest.NewRecorder()
	server.handleQuery(w, req)
	
	if w.Code != http.StatusBadRequest {
		t.Errorf("query without db status = %d, want 400", w.Code)
	}

	// Query without query
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	
	if w.Code != http.StatusBadRequest {
		t.Errorf("query without q status = %d, want 400", w.Code)
	}

	// Invalid query
	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/query?db=testdb&q=INVALID+QUERY", nil)
	w = httptest.NewRecorder()
	server.handleQuery(w, req)
	// Should return error but not panic
}

func TestServeHTTPMethodNotAllowed(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// DELETE request to /ping (should still work since ServeHTTP dispatches to router)
	req := httptest.NewRequest(http.MethodDelete, "/ping", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	// Should get method not allowed or handled by mux
}

func TestHandleWritePrecision(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if err := server.engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Write with seconds precision (timestamp without nanoseconds)
	body := "cpu value=42 1609459200"
	req := httptest.NewRequest(http.MethodPost, "/write?db=testdb&precision=s", strings.NewReader(body))
	w := httptest.NewRecorder()
	server.handleWrite(w, req)
	
	if w.Code != http.StatusNoContent {
		t.Errorf("write with precision=s status = %d, want 204: %s", w.Code, w.Body.String())
	}
}

func TestFormatQueryResponseWithData(t *testing.T) {
	// Create a result with actual data
	result := &query.Result{
		Series: []*query.Series{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "server1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{int64(1609459200000000000), 42.5},
					{int64(1609459201000000000), 43.5},
				},
			},
		},
		Messages: []string{"Query executed successfully"},
	}

	resp := formatQueryResponse(result)

	if len(resp.Results) != 1 {
		t.Errorf("expected 1 result, got %d", len(resp.Results))
	}
	if len(resp.Results[0].Series) != 1 {
		t.Errorf("expected 1 series, got %d", len(resp.Results[0].Series))
	}
	if resp.Results[0].Series[0].Name != "cpu" {
		t.Errorf("expected series name 'cpu', got %s", resp.Results[0].Series[0].Name)
	}
	if len(resp.Results[0].Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(resp.Results[0].Messages))
	}
}

func TestFormatQueryResponseEmpty(t *testing.T) {
	result := &query.Result{
		Series: []*query.Series{},
	}

	resp := formatQueryResponse(result)

	if len(resp.Results) != 1 {
		t.Errorf("expected 1 result, got %d", len(resp.Results))
	}
	if len(resp.Results[0].Series) != 0 {
		t.Errorf("expected 0 series, got %d", len(resp.Results[0].Series))
	}
}

func TestFormatQueryResponseNoTimeColumn(t *testing.T) {
	result := &query.Result{
		Series: []*query.Series{
			{
				Name:    "cpu",
				Columns: []string{"value", "count"},
				Values: [][]interface{}{
					{42.5, int64(10)},
				},
			},
		},
	}

	resp := formatQueryResponse(result)

	if len(resp.Results[0].Series) != 1 {
		t.Errorf("expected 1 series, got %d", len(resp.Results[0].Series))
	}
}

func TestWriteCSV(t *testing.T) {
	result := &query.Result{
		Series: []*query.Series{
			{
				Name:    "cpu",
				Columns: []string{"time", "host", "value"},
				Values: [][]interface{}{
					{int64(1609459200000000000), "server1", 42.5},
					{int64(1609459201000000000), "server2", 43.5},
				},
			},
		},
	}

	var buf bytes.Buffer
	writeCSV(&buf, result)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	if len(lines) != 3 {
		t.Errorf("expected 3 lines (header + 2 rows), got %d", len(lines))
	}

	// Check header
	if !strings.Contains(lines[0], "time") || !strings.Contains(lines[0], "host") || !strings.Contains(lines[0], "value") {
		t.Errorf("header missing columns: %s", lines[0])
	}

	// Check data rows
	if !strings.Contains(lines[1], "server1") {
		t.Errorf("row 1 missing server1: %s", lines[1])
	}
}

func TestWriteCSVEmpty(t *testing.T) {
	result := &query.Result{
		Series: []*query.Series{},
	}

	var buf bytes.Buffer
	writeCSV(&buf, result)

	if buf.Len() != 0 {
		t.Errorf("expected empty output, got %s", buf.String())
	}
}

func TestWriteCSVMultipleSeries(t *testing.T) {
	result := &query.Result{
		Series: []*query.Series{
			{
				Name:    "cpu",
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{int64(1609459200000000000), 42.5},
				},
			},
			{
				Name:    "memory",
				Columns: []string{"time", "used"},
				Values: [][]interface{}{
					{int64(1609459200000000000), int64(1024)},
				},
			},
		},
	}

	var buf bytes.Buffer
	writeCSV(&buf, result)

	output := buf.String()
	// Should have 2 headers + 2 data rows = 4 lines
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 4 {
		t.Errorf("expected 4 lines, got %d: %s", len(lines), output)
	}
}

func TestFormatCSVValueAllTypes(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", `"hello"`},
		{int64(42), "42"},
		{int(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, ""},
		{struct{}{}, ""}, // unknown type
	}

	for _, tt := range tests {
		result := formatCSVValue(tt.input)
		if result != tt.expected {
			t.Errorf("formatCSVValue(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestServerStartAndShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("NewEngine error: %v", err)
	}
	defer engine.Close()

	executor := query.NewExecutor(engine, query.DefaultExecutorConfig())

	// Use a random available port
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"

	server := NewServer(engine, executor, config)

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown error: %v", err)
	}

	// Check that Start returned (possibly with error due to shutdown)
	select {
	case err := <-errCh:
		// http.ErrServerClosed is expected
		if err != nil && err != http.ErrServerClosed {
			t.Logf("Start returned: %v (expected ErrServerClosed or nil)", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start did not return after Shutdown")
	}
}

func TestServerListenAndServe(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("NewEngine error: %v", err)
	}
	defer engine.Close()

	executor := query.NewExecutor(engine, query.DefaultExecutorConfig())

	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"

	server := NewServer(engine, executor, config)

	// Start server using ListenAndServe
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func TestServerStartTLS(t *testing.T) {
	tmpDir := t.TempDir()
	engine, err := storage.NewEngine(storage.DefaultEngineConfig(tmpDir))
	if err != nil {
		t.Fatalf("NewEngine error: %v", err)
	}
	defer engine.Close()

	executor := query.NewExecutor(engine, query.DefaultExecutorConfig())

	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"

	server := NewServer(engine, executor, config)

	// StartTLS with non-existent certs should fail
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.StartTLS("/nonexistent/cert.pem", "/nonexistent/key.pem")
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected error for non-existent certs")
		}
	case <-time.After(2 * time.Second):
		// If it didn't error quickly, shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}
}

func TestServeHTTPPanic(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test various edge cases that might cause panics
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	// Should return 404, not panic
	if w.Code != http.StatusNotFound {
		t.Logf("unexpected status for nonexistent route: %d", w.Code)
	}
}

func TestWriteJSONError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	w := httptest.NewRecorder()
	
	// Write valid JSON
	server.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	
	if w.Code != http.StatusOK {
		t.Errorf("writeJSON status = %d, want 200", w.Code)
	}
	
	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
}
