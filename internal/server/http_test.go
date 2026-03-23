package server

import (
	"bytes"
	"encoding/json"
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
