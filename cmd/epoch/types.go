package main

import (
	"github.com/sudokatie/epoch/internal/storage"
)

// QueryResponse is the HTTP API response format
type QueryResponse struct {
	Results []QueryResult `json:"results"`
}

// QueryResult represents a single query result
type QueryResult struct {
	StatementID int           `json:"statement_id"`
	Series      []QuerySeries `json:"series,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// QuerySeries represents a single series in a query result
type QuerySeries struct {
	Name    string          `json:"name"`
	Tags    storage.Tags    `json:"tags,omitempty"`
	Columns []string        `json:"columns"`
	Values  [][]interface{} `json:"values"`
}
