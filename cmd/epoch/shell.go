package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ShellConfig holds shell configuration
type ShellConfig struct {
	Host      string
	Database  string
	Format    string
	Precision string
}

// Shell is an interactive query shell
type Shell struct {
	config  ShellConfig
	client  *http.Client
	history []string
}

// NewShell creates a new shell instance
func NewShell(config ShellConfig) *Shell {
	return &Shell{
		config: config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		history: make([]string, 0),
	}
}

// Run starts the interactive shell
func (s *Shell) Run() {
	fmt.Printf("epoch shell v%s\n", version)
	fmt.Printf("Connected to %s\n", s.config.Host)
	if s.config.Database != "" {
		fmt.Printf("Using database: %s\n", s.config.Database)
	}
	fmt.Println("Type 'help' for available commands, 'exit' to quit.")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	for {
		// Print prompt
		prompt := "> "
		if s.config.Database != "" {
			prompt = fmt.Sprintf("[%s]> ", s.config.Database)
		}
		fmt.Print(prompt)

		// Read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				return
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		// Trim and process
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Add to history
		s.history = append(s.history, line)

		// Handle command
		if s.handleCommand(line) {
			continue
		}

		// Execute as query
		s.executeQuery(line)
	}
}

// handleCommand handles built-in commands, returns true if handled
func (s *Shell) handleCommand(line string) bool {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return true
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "EXIT", "QUIT", "\\Q":
		fmt.Println("Goodbye!")
		os.Exit(0)
		return true

	case "HELP", "\\?":
		s.printHelp()
		return true

	case "USE":
		if len(parts) < 2 {
			fmt.Println("Usage: USE <database>")
			return true
		}
		s.config.Database = parts[1]
		fmt.Printf("Using database: %s\n", s.config.Database)
		return true

	case "PRECISION":
		if len(parts) < 2 {
			fmt.Printf("Current precision: %s\n", s.config.Precision)
			return true
		}
		precision := strings.ToLower(parts[1])
		if precision != "ns" && precision != "us" && precision != "ms" && precision != "s" {
			fmt.Println("Invalid precision. Use: ns, us, ms, s")
			return true
		}
		s.config.Precision = precision
		fmt.Printf("Precision set to: %s\n", s.config.Precision)
		return true

	case "FORMAT":
		if len(parts) < 2 {
			fmt.Printf("Current format: %s\n", s.config.Format)
			return true
		}
		format := strings.ToLower(parts[1])
		if format != "table" && format != "json" && format != "csv" {
			fmt.Println("Invalid format. Use: table, json, csv")
			return true
		}
		s.config.Format = format
		fmt.Printf("Format set to: %s\n", s.config.Format)
		return true

	case "SHOW":
		return s.handleShow(parts)

	case "HISTORY":
		for i, h := range s.history {
			fmt.Printf("%3d  %s\n", i+1, h)
		}
		return true

	case "CLEAR":
		fmt.Print("\033[H\033[2J")
		return true

	case "CONNECT":
		if len(parts) < 2 {
			fmt.Printf("Current host: %s\n", s.config.Host)
			return true
		}
		s.config.Host = parts[1]
		fmt.Printf("Connected to: %s\n", s.config.Host)
		return true

	default:
		return false
	}
}

// handleShow handles SHOW commands
func (s *Shell) handleShow(parts []string) bool {
	if len(parts) < 2 {
		fmt.Println("Usage: SHOW DATABASES | MEASUREMENTS | TAG KEYS | FIELD KEYS")
		return true
	}

	target := strings.ToUpper(parts[1])

	switch target {
	case "DATABASES":
		s.executeQuery("SHOW DATABASES")
		return true

	case "MEASUREMENTS":
		if s.config.Database == "" {
			fmt.Println("No database selected. Use: USE <database>")
			return true
		}
		s.executeQuery("SHOW MEASUREMENTS")
		return true

	case "TAG":
		if len(parts) >= 3 && strings.ToUpper(parts[2]) == "KEYS" {
			if s.config.Database == "" {
				fmt.Println("No database selected. Use: USE <database>")
				return true
			}
			s.executeQuery("SHOW TAG KEYS")
			return true
		}

	case "FIELD":
		if len(parts) >= 3 && strings.ToUpper(parts[2]) == "KEYS" {
			if s.config.Database == "" {
				fmt.Println("No database selected. Use: USE <database>")
				return true
			}
			s.executeQuery("SHOW FIELD KEYS")
			return true
		}
	}

	return false
}

// executeQuery executes a query against the server
func (s *Shell) executeQuery(query string) {
	if s.config.Database == "" && !strings.HasPrefix(strings.ToUpper(query), "SHOW DATABASES") {
		fmt.Println("No database selected. Use: USE <database>")
		return
	}

	// Build URL
	u := fmt.Sprintf("http://%s/query", s.config.Host)
	params := url.Values{}
	if s.config.Database != "" {
		params.Set("db", s.config.Database)
	}
	params.Set("q", query)
	params.Set("precision", s.config.Precision)

	// Make request
	resp, err := s.client.Get(u + "?" + params.Encode())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
		return
	}

	// Check for error status
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		return
	}

	// Parse and format response
	var result QueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
		return
	}

	// Format output
	switch s.config.Format {
	case "json":
		s.printJSON(result)
	case "csv":
		s.printCSV(result)
	default:
		s.printTable(result)
	}
}

// printHelp prints help information
func (s *Shell) printHelp() {
	fmt.Print(`
Commands:
  USE <database>       Select a database
  SHOW DATABASES       List all databases
  SHOW MEASUREMENTS    List measurements in current database
  SHOW TAG KEYS        List tag keys
  SHOW FIELD KEYS      List field keys
  PRECISION <unit>     Set timestamp precision (ns, us, ms, s)
  FORMAT <type>        Set output format (table, json, csv)
  CONNECT <host:port>  Connect to a different server
  HISTORY              Show command history
  CLEAR                Clear screen
  HELP                 Show this help
  EXIT                 Exit the shell

Queries:
  SELECT * FROM <measurement>
  SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)
`)
}

// QueryResponse is the response from the server
type QueryResponse struct {
	Results []QueryResult `json:"results"`
}

// QueryResult is a single result
type QueryResult struct {
	StatementID int            `json:"statement_id"`
	Series      []ResultSeries `json:"series"`
	Error       string         `json:"error,omitempty"`
}

// ResultSeries is a single series
type ResultSeries struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

// printTable prints results as a table
func (s *Shell) printTable(resp QueryResponse) {
	for _, result := range resp.Results {
		if result.Error != "" {
			fmt.Fprintf(os.Stderr, "Error: %s\n", result.Error)
			continue
		}

		for _, series := range result.Series {
			// Print series header
			if series.Name != "" {
				fmt.Printf("name: %s\n", series.Name)
			}
			if len(series.Tags) > 0 {
				fmt.Printf("tags: ")
				first := true
				for k, v := range series.Tags {
					if !first {
						fmt.Print(", ")
					}
					fmt.Printf("%s=%s", k, v)
					first = false
				}
				fmt.Println()
			}

			// Calculate column widths
			widths := make([]int, len(series.Columns))
			for i, col := range series.Columns {
				widths[i] = len(col)
			}
			for _, row := range series.Values {
				for i, val := range row {
					if i < len(widths) {
						str := formatValue(val)
						if len(str) > widths[i] {
							widths[i] = len(str)
						}
					}
				}
			}

			// Print header
			var header bytes.Buffer
			var separator bytes.Buffer
			for i, col := range series.Columns {
				if i > 0 {
					header.WriteString(" | ")
					separator.WriteString("-+-")
				}
				header.WriteString(fmt.Sprintf("%-*s", widths[i], col))
				separator.WriteString(strings.Repeat("-", widths[i]))
			}
			fmt.Println(header.String())
			fmt.Println(separator.String())

			// Print rows
			for _, row := range series.Values {
				var line bytes.Buffer
				for i, val := range row {
					if i > 0 {
						line.WriteString(" | ")
					}
					if i < len(widths) {
						line.WriteString(fmt.Sprintf("%-*s", widths[i], formatValue(val)))
					}
				}
				fmt.Println(line.String())
			}

			fmt.Printf("\n%d rows returned\n\n", len(series.Values))
		}
	}
}

// printJSON prints results as JSON
func (s *Shell) printJSON(resp QueryResponse) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(resp)
}

// printCSV prints results as CSV
func (s *Shell) printCSV(resp QueryResponse) {
	for _, result := range resp.Results {
		for _, series := range result.Series {
			// Print header
			fmt.Println(strings.Join(series.Columns, ","))

			// Print rows
			for _, row := range series.Values {
				values := make([]string, len(row))
				for i, val := range row {
					values[i] = formatCSVValue(val)
				}
				fmt.Println(strings.Join(values, ","))
			}
		}
	}
}

// formatValue formats a value for display
func formatValue(v interface{}) string {
	if v == nil {
		return "<null>"
	}
	switch val := v.(type) {
	case float64:
		// Check if it's a whole number
		if val == float64(int64(val)) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%.6g", val)
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatCSVValue formats a value for CSV output
func formatCSVValue(v interface{}) string {
	if v == nil {
		return ""
	}
	str := formatValue(v)
	// Quote if contains comma, quote, or newline
	if strings.ContainsAny(str, ",\"\n") {
		return fmt.Sprintf("%q", str)
	}
	return str
}
