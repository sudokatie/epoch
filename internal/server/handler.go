package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/sudokatie/epoch/internal/query"
	"github.com/sudokatie/epoch/pkg/protocol"
)

// handlePing handles GET /ping - health check endpoint
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Epoch-Version", "0.1.0")
	w.WriteHeader(http.StatusNoContent)
}

// handleWrite handles POST /write - data ingestion endpoint
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get database from query parameter
	database := r.URL.Query().Get("db")
	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	// Check database exists, create if requested
	if _, ok := s.engine.GetDatabase(database); !ok {
		// Try to create if it doesn't exist
		if err := s.engine.CreateDatabase(database); err != nil {
			s.writeErrorf(w, http.StatusNotFound, "database not found: %s", database)
			return
		}
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorf(w, http.StatusBadRequest, "failed to read body: %v", err)
		return
	}

	if len(body) == 0 {
		s.writeError(w, http.StatusBadRequest, "empty request body")
		return
	}

	// Parse line protocol
	points, err := protocol.ParseLineProtocol(string(body))
	if err != nil {
		s.writeErrorf(w, http.StatusBadRequest, "failed to parse line protocol: %v", err)
		return
	}

	if len(points) == 0 {
		s.writeError(w, http.StatusBadRequest, "no valid points in request")
		return
	}

	// Write to engine
	if err := s.engine.WriteBatch(database, points); err != nil {
		s.writeErrorf(w, http.StatusInternalServerError, "failed to write: %v", err)
		return
	}

	// Update stats
	s.stats.mu.Lock()
	s.stats.Writes++
	s.stats.PointsWritten += int64(len(points))
	s.stats.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// handleQuery handles GET/POST /query - query execution endpoint
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get database from query parameter
	database := r.URL.Query().Get("db")
	if database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}

	// Get query string
	var queryStr string
	if r.Method == http.MethodGet {
		queryStr = r.URL.Query().Get("q")
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeErrorf(w, http.StatusBadRequest, "failed to read body: %v", err)
			return
		}
		// Check if body is JSON or plain query
		if len(body) > 0 && body[0] == '{' {
			var req struct {
				Query string `json:"q"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				s.writeErrorf(w, http.StatusBadRequest, "invalid JSON: %v", err)
				return
			}
			queryStr = req.Query
		} else {
			queryStr = string(body)
		}
	}

	if queryStr == "" {
		s.writeError(w, http.StatusBadRequest, "query is required")
		return
	}

	// Parse the query
	parser := query.NewParser(queryStr)
	stmt, err := parser.Parse()
	if err != nil {
		s.writeErrorf(w, http.StatusBadRequest, "invalid query: %v", err)
		return
	}

	// Type assert to SelectStatement
	selectStmt, ok := stmt.(*query.SelectStatement)
	if !ok {
		s.writeError(w, http.StatusBadRequest, "only SELECT statements are supported")
		return
	}

	// Build execution plan
	planner := query.NewPlanner()
	plan, err := planner.Plan(selectStmt)
	if err != nil {
		s.writeErrorf(w, http.StatusBadRequest, "failed to plan query: %v", err)
		return
	}

	// Execute the query
	ctx := r.Context()
	result, err := s.executor.Execute(ctx, database, plan)
	if err != nil {
		if err == context.DeadlineExceeded {
			s.writeError(w, http.StatusRequestTimeout, "query timeout")
			return
		}
		s.writeErrorf(w, http.StatusInternalServerError, "query failed: %v", err)
		return
	}

	// Update stats
	s.stats.mu.Lock()
	s.stats.Queries++
	s.stats.mu.Unlock()

	// Format response
	response := formatQueryResponse(result)

	// Get output format from Accept header or query param
	format := r.URL.Query().Get("format")
	if format == "" {
		accept := r.Header.Get("Accept")
		if accept == "text/csv" {
			format = "csv"
		}
	}

	switch format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		writeCSV(w, result)
	default:
		s.writeJSON(w, http.StatusOK, response)
	}
}

// handleDebugVars handles GET /debug/vars - metrics endpoint
func (s *Server) handleDebugVars(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := s.GetStats()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	vars := map[string]interface{}{
		"version": "0.1.0",
		"uptime":  time.Since(stats.StartTime).String(),
		"server": map[string]interface{}{
			"requests":       stats.Requests,
			"writes":         stats.Writes,
			"queries":        stats.Queries,
			"points_written": stats.PointsWritten,
			"errors":         stats.Errors,
		},
		"memory": map[string]interface{}{
			"alloc":       mem.Alloc,
			"total_alloc": mem.TotalAlloc,
			"sys":         mem.Sys,
			"heap_alloc":  mem.HeapAlloc,
			"heap_sys":    mem.HeapSys,
			"gc_runs":     mem.NumGC,
		},
		"runtime": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"cpus":       runtime.NumCPU(),
			"go_version": runtime.Version(),
		},
		"databases": s.engine.ListDatabases(),
	}

	s.writeJSON(w, http.StatusOK, vars)
}

// QueryResponse is the JSON response format for queries
type QueryResponse struct {
	Results []QueryResult `json:"results"`
}

// QueryResult is a single result in the response
type QueryResult struct {
	StatementID int              `json:"statement_id"`
	Series      []ResultSeries   `json:"series,omitempty"`
	Messages    []ResultMessage  `json:"messages,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// ResultSeries is a single series in the result
type ResultSeries struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

// ResultMessage is a warning or info message
type ResultMessage struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

// formatQueryResponse converts executor result to API response format
func formatQueryResponse(result *query.Result) QueryResponse {
	var series []ResultSeries

	for _, s := range result.Series {
		rs := ResultSeries{
			Name:    s.Name,
			Tags:    s.Tags,
			Columns: s.Columns,
			Values:  s.Values,
		}

		// Convert timestamps to RFC3339 format
		timeIdx := -1
		for i, col := range rs.Columns {
			if col == "time" {
				timeIdx = i
				break
			}
		}

		if timeIdx >= 0 {
			for _, row := range rs.Values {
				if ts, ok := row[timeIdx].(int64); ok {
					row[timeIdx] = time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
				}
			}
		}

		series = append(series, rs)
	}

	qr := QueryResult{
		StatementID: 0,
		Series:      series,
	}

	// Add messages if any
	for _, msg := range result.Messages {
		qr.Messages = append(qr.Messages, ResultMessage{
			Level: "info",
			Text:  msg,
		})
	}

	return QueryResponse{
		Results: []QueryResult{qr},
	}
}

// writeCSV writes query results as CSV
func writeCSV(w io.Writer, result *query.Result) {
	for _, s := range result.Series {
		// Write header
		for i, col := range s.Columns {
			if i > 0 {
				w.Write([]byte(","))
			}
			w.Write([]byte(col))
		}
		w.Write([]byte("\n"))

		// Write rows
		for _, row := range s.Values {
			for i, val := range row {
				if i > 0 {
					w.Write([]byte(","))
				}
				w.Write([]byte(formatCSVValue(val)))
			}
			w.Write([]byte("\n"))
		}
	}
}

// formatCSVValue formats a value for CSV output
func formatCSVValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return strconv.Quote(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case nil:
		return ""
	default:
		return ""
	}
}
