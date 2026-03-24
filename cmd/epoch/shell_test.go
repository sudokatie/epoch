package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewShell(t *testing.T) {
	config := ShellConfig{
		Host:      "localhost:8086",
		Database:  "testdb",
		Format:    "table",
		Precision: "ns",
	}

	shell := NewShell(config)
	if shell == nil {
		t.Fatal("NewShell returned nil")
	}
	if shell.config.Host != "localhost:8086" {
		t.Errorf("Host = %q, want %q", shell.config.Host, "localhost:8086")
	}
	if shell.config.Database != "testdb" {
		t.Errorf("Database = %q, want %q", shell.config.Database, "testdb")
	}
}

func TestShellHandleCommand(t *testing.T) {
	config := ShellConfig{
		Host:      "localhost:8086",
		Format:    "table",
		Precision: "ns",
	}
	shell := NewShell(config)

	// Test USE command
	handled := shell.handleCommand("USE testdb")
	if !handled {
		t.Error("USE command should be handled")
	}
	if shell.config.Database != "testdb" {
		t.Errorf("Database = %q, want %q", shell.config.Database, "testdb")
	}

	// Test PRECISION command
	handled = shell.handleCommand("PRECISION ms")
	if !handled {
		t.Error("PRECISION command should be handled")
	}
	if shell.config.Precision != "ms" {
		t.Errorf("Precision = %q, want %q", shell.config.Precision, "ms")
	}

	// Test FORMAT command
	handled = shell.handleCommand("FORMAT json")
	if !handled {
		t.Error("FORMAT command should be handled")
	}
	if shell.config.Format != "json" {
		t.Errorf("Format = %q, want %q", shell.config.Format, "json")
	}

	// Test CONNECT command
	handled = shell.handleCommand("CONNECT newhost:8086")
	if !handled {
		t.Error("CONNECT command should be handled")
	}
	if shell.config.Host != "newhost:8086" {
		t.Errorf("Host = %q, want %q", shell.config.Host, "newhost:8086")
	}

	// Test HISTORY command
	handled = shell.handleCommand("HISTORY")
	if !handled {
		t.Error("HISTORY command should be handled")
	}

	// Test CLEAR command
	handled = shell.handleCommand("CLEAR")
	if !handled {
		t.Error("CLEAR command should be handled")
	}

	// Test HELP command
	handled = shell.handleCommand("HELP")
	if !handled {
		t.Error("HELP command should be handled")
	}

	// Test unknown command
	handled = shell.handleCommand("SELECT * FROM cpu")
	if handled {
		t.Error("SELECT should not be handled as a built-in command")
	}
}

func TestShellHandleCommandEdgeCases(t *testing.T) {
	shell := NewShell(ShellConfig{})

	// Empty command
	handled := shell.handleCommand("")
	if !handled {
		t.Error("empty command should be handled")
	}

	// USE without database
	handled = shell.handleCommand("USE")
	if !handled {
		t.Error("USE without arg should be handled")
	}

	// PRECISION without value
	handled = shell.handleCommand("PRECISION")
	if !handled {
		t.Error("PRECISION without arg should be handled")
	}

	// Invalid precision
	handled = shell.handleCommand("PRECISION invalid")
	if !handled {
		t.Error("PRECISION with invalid value should be handled")
	}

	// FORMAT without value
	handled = shell.handleCommand("FORMAT")
	if !handled {
		t.Error("FORMAT without arg should be handled")
	}

	// Invalid format
	handled = shell.handleCommand("FORMAT invalid")
	if !handled {
		t.Error("FORMAT with invalid value should be handled")
	}

	// CONNECT without value
	handled = shell.handleCommand("CONNECT")
	if !handled {
		t.Error("CONNECT without arg should be handled")
	}
}

func TestShellHandleShow(t *testing.T) {
	shell := NewShell(ShellConfig{Database: "testdb"})

	// SHOW without target
	handled := shell.handleShow([]string{"SHOW"})
	if !handled {
		t.Error("SHOW without target should be handled")
	}

	// SHOW DATABASES handled internally
	handled = shell.handleShow([]string{"SHOW", "DATABASES"})
	// This will try to execute a query, but since no server is running, it will fail silently
	if !handled {
		t.Error("SHOW DATABASES should be handled")
	}
}

func TestShellExecuteQuery(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000, 42.5},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	shell := NewShell(ShellConfig{
		Host:      host,
		Database:  "testdb",
		Format:    "table",
		Precision: "ns",
	})

	// Execute query
	shell.executeQuery("SELECT * FROM cpu")
	// No error expected, just verify it doesn't panic
}

func TestShellExecuteQueryNoDatabase(t *testing.T) {
	shell := NewShell(ShellConfig{
		Host:      "localhost:8086",
		Format:    "table",
		Precision: "ns",
	})

	// Execute query without database
	shell.executeQuery("SELECT * FROM cpu")
	// Should print an error message about no database selected
}

func TestShellExecuteQueryJSONFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000, 42.5},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	shell := NewShell(ShellConfig{
		Host:      host,
		Database:  "testdb",
		Format:    "json",
		Precision: "ns",
	})

	shell.executeQuery("SELECT * FROM cpu")
}

func TestShellExecuteQueryCSVFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "cpu",
							"columns": []string{"time", "value"},
							"values": [][]interface{}{
								{1609459200000000000, 42.5},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	shell := NewShell(ShellConfig{
		Host:      host,
		Database:  "testdb",
		Format:    "csv",
		Precision: "ns",
	})

	shell.executeQuery("SELECT * FROM cpu")
}

func TestShellExecuteQueryError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"error":        "database not found",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	shell := NewShell(ShellConfig{
		Host:      host,
		Database:  "testdb",
		Format:    "table",
		Precision: "ns",
	})

	shell.executeQuery("SELECT * FROM cpu")
}

func TestShellPrintTable(t *testing.T) {
	shell := NewShell(ShellConfig{})
	
	resp := ShellQueryResponse{
		Results: []ShellQueryResult{
			{
				StatementID: 0,
				Series: []ShellResultSeries{
					{
						Name:    "cpu",
						Columns: []string{"time", "host", "value"},
						Values: [][]interface{}{
							{1609459200000000000.0, "server1", 42.5},
							{1609459201000000000.0, "server2", 43.5},
						},
					},
				},
			},
		},
	}

	// Just verify it doesn't panic
	shell.printTable(resp)
}

func TestShellPrintTableEmpty(t *testing.T) {
	shell := NewShell(ShellConfig{})
	
	resp := ShellQueryResponse{
		Results: []ShellQueryResult{
			{
				StatementID: 0,
				Series: []ShellResultSeries{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  [][]interface{}{},
					},
				},
			},
		},
	}

	shell.printTable(resp)
}

func TestShellPrintTableWithError(t *testing.T) {
	shell := NewShell(ShellConfig{})
	
	resp := ShellQueryResponse{
		Results: []ShellQueryResult{
			{
				StatementID: 0,
				Error:       "database not found",
			},
		},
	}

	shell.printTable(resp)
}

func TestShellPrintJSON(t *testing.T) {
	shell := NewShell(ShellConfig{})
	
	resp := ShellQueryResponse{
		Results: []ShellQueryResult{
			{
				StatementID: 0,
				Series: []ShellResultSeries{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{1609459200000000000.0, 42.5},
						},
					},
				},
			},
		},
	}

	shell.printJSON(resp)
}

func TestShellPrintCSV(t *testing.T) {
	shell := NewShell(ShellConfig{})
	
	resp := ShellQueryResponse{
		Results: []ShellQueryResult{
			{
				StatementID: 0,
				Series: []ShellResultSeries{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{1609459200000000000.0, 42.5},
						},
					},
				},
			},
		},
	}

	shell.printCSV(resp)
}

func TestShellPrintHelp(t *testing.T) {
	shell := NewShell(ShellConfig{})
	shell.printHelp()
}

func TestShellShowDatabases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]interface{}{
			"results": []map[string]interface{}{
				{
					"statement_id": 0,
					"series": []map[string]interface{}{
						{
							"name":    "databases",
							"columns": []string{"name"},
							"values": [][]interface{}{
								{"db1"},
								{"db2"},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	shell := NewShell(ShellConfig{
		Host:      host,
		Format:    "table",
		Precision: "ns",
	})

	handled := shell.handleShow([]string{"SHOW", "DATABASES"})
	if !handled {
		t.Error("SHOW DATABASES should be handled")
	}
}

func TestShellShowMeasurementsNoDatabase(t *testing.T) {
	shell := NewShell(ShellConfig{
		Host:      "localhost:8086",
		Format:    "table",
		Precision: "ns",
	})

	handled := shell.handleShow([]string{"SHOW", "MEASUREMENTS"})
	if !handled {
		t.Error("SHOW MEASUREMENTS should be handled")
	}
}

func TestShellShowTagKeys(t *testing.T) {
	shell := NewShell(ShellConfig{
		Host:      "localhost:8086",
		Database:  "testdb",
		Format:    "table",
		Precision: "ns",
	})

	handled := shell.handleShow([]string{"SHOW", "TAG", "KEYS"})
	if !handled {
		t.Error("SHOW TAG KEYS should be handled")
	}
}

func TestShellShowFieldKeys(t *testing.T) {
	shell := NewShell(ShellConfig{
		Host:      "localhost:8086",
		Database:  "testdb",
		Format:    "table",
		Precision: "ns",
	})

	handled := shell.handleShow([]string{"SHOW", "FIELD", "KEYS"})
	if !handled {
		t.Error("SHOW FIELD KEYS should be handled")
	}
}

func TestShellShowUnknown(t *testing.T) {
	shell := NewShell(ShellConfig{
		Host:     "localhost:8086",
		Database: "testdb",
	})

	handled := shell.handleShow([]string{"SHOW", "UNKNOWN"})
	if handled {
		t.Error("SHOW UNKNOWN should not be handled")
	}
}
