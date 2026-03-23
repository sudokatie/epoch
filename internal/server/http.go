package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sudokatie/epoch/internal/query"
	"github.com/sudokatie/epoch/internal/storage"
)

// Server is the HTTP API server for epoch
type Server struct {
	mu sync.RWMutex

	config   Config
	engine   *storage.Engine
	executor *query.Executor
	server   *http.Server
	mux      *http.ServeMux

	// Metrics
	stats Stats
}

// Config holds server configuration
type Config struct {
	// BindAddress is the address to listen on (default ":8086")
	BindAddress string
	// ReadTimeout is the maximum duration for reading the entire request
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration before timing out writes
	WriteTimeout time.Duration
	// MaxBodySize is the maximum allowed request body size in bytes
	MaxBodySize int64
	// AuthEnabled enables authentication
	AuthEnabled bool
	// LogRequests enables request logging
	LogRequests bool
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		BindAddress:  ":8086",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxBodySize:  25 * 1024 * 1024, // 25MB
		AuthEnabled:  false,
		LogRequests:  true,
	}
}

// Stats holds server metrics
type Stats struct {
	mu sync.RWMutex

	StartTime    time.Time
	Requests     int64
	Writes       int64
	Queries      int64
	PointsWritten int64
	Errors       int64
}

// NewServer creates a new HTTP server
func NewServer(engine *storage.Engine, executor *query.Executor, config Config) *Server {
	s := &Server{
		config:   config,
		engine:   engine,
		executor: executor,
		mux:      http.NewServeMux(),
		stats: Stats{
			StartTime: time.Now(),
		},
	}

	s.registerRoutes()

	s.server = &http.Server{
		Addr:         config.BindAddress,
		Handler:      s,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return s
}

// registerRoutes sets up the HTTP routes
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/ping", s.handlePing)
	s.mux.HandleFunc("/write", s.handleWrite)
	s.mux.HandleFunc("/query", s.handleQuery)
	s.mux.HandleFunc("/debug/vars", s.handleDebugVars)
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Track request count
	s.stats.mu.Lock()
	s.stats.Requests++
	s.stats.mu.Unlock()

	// Log request if enabled
	if s.config.LogRequests {
		start := time.Now()
		defer func() {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
		}()
	}

	// Limit body size
	if r.ContentLength > s.config.MaxBodySize {
		s.writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxBodySize)

	// Route the request
	s.mux.ServeHTTP(w, r)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	log.Printf("Starting HTTP server on %s", s.config.BindAddress)
	return s.server.ListenAndServe()
}

// StartTLS starts the HTTP server with TLS
func (s *Server) StartTLS(certFile, keyFile string) error {
	log.Printf("Starting HTTPS server on %s", s.config.BindAddress)
	return s.server.ListenAndServeTLS(certFile, keyFile)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	log.Printf("Shutting down HTTP server")
	return s.server.Shutdown(ctx)
}

// GetStats returns current server statistics
func (s *Server) GetStats() Stats {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	return Stats{
		StartTime:     s.stats.StartTime,
		Requests:      s.stats.Requests,
		Writes:        s.stats.Writes,
		Queries:       s.stats.Queries,
		PointsWritten: s.stats.PointsWritten,
		Errors:        s.stats.Errors,
	}
}

// Helper functions

func (s *Server) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Epoch-Version", "0.1.0")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("error encoding JSON: %v", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.stats.mu.Lock()
	s.stats.Errors++
	s.stats.mu.Unlock()

	s.writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) writeErrorf(w http.ResponseWriter, status int, format string, args ...interface{}) {
	s.writeError(w, status, fmt.Sprintf(format, args...))
}
