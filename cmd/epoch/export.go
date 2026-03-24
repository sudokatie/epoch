package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
	"github.com/sudokatie/epoch/pkg/protocol"
)

// ExportConfig holds export configuration
type ExportConfig struct {
	Host        string
	Database    string
	Measurement string
	StartTime   string
	EndTime     string
	Format      string
	Output      string
}

// runExport exports data to a file
func runExport(cfg ExportConfig) error {
	// Build query
	query := buildExportQuery(cfg)

	fmt.Fprintf(os.Stderr, "Exporting from %s: %s\n", cfg.Database, query)

	// Execute query
	client := &http.Client{Timeout: 5 * time.Minute}
	queryURL := fmt.Sprintf("http://%s/query?db=%s&q=%s",
		cfg.Host,
		url.QueryEscape(cfg.Database),
		url.QueryEscape(query))

	resp, err := client.Get(queryURL)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("query failed (%d): %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(result.Results) == 0 || result.Results[0].Error != "" {
		if len(result.Results) > 0 && result.Results[0].Error != "" {
			return fmt.Errorf("query error: %s", result.Results[0].Error)
		}
		return fmt.Errorf("no results")
	}

	// Convert to data points
	points := resultsToPoints(result.Results[0].Series)

	// Open output
	var out io.Writer
	if cfg.Output == "-" {
		out = os.Stdout
	} else {
		f, err := os.Create(cfg.Output)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		out = f
	}

	// Write in requested format
	var written int
	switch cfg.Format {
	case "line":
		written, err = writeLineProtocol(out, points)
	case "json":
		written, err = writeJSON(out, points)
	case "csv":
		written, err = writeCSV(out, points)
	default:
		return fmt.Errorf("unsupported format: %s", cfg.Format)
	}

	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Exported %d points\n", written)
	return nil
}

func buildExportQuery(cfg ExportConfig) string {
	var sb strings.Builder

	sb.WriteString("SELECT * FROM ")

	if cfg.Measurement != "" {
		sb.WriteString(cfg.Measurement)
	} else {
		sb.WriteString("/.*/")
	}

	var conditions []string

	if cfg.StartTime != "" {
		ts, err := parseTimeArg(cfg.StartTime)
		if err == nil {
			conditions = append(conditions, fmt.Sprintf("time >= %d", ts))
		} else {
			conditions = append(conditions, fmt.Sprintf("time >= '%s'", cfg.StartTime))
		}
	}

	if cfg.EndTime != "" {
		ts, err := parseTimeArg(cfg.EndTime)
		if err == nil {
			conditions = append(conditions, fmt.Sprintf("time <= %d", ts))
		} else {
			conditions = append(conditions, fmt.Sprintf("time <= '%s'", cfg.EndTime))
		}
	}

	if len(conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conditions, " AND "))
	}

	return sb.String()
}

func parseTimeArg(s string) (int64, error) {
	// Try as Unix timestamp
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Detect precision
		if ts > 1e18 {
			return ts, nil // nanoseconds
		} else if ts > 1e15 {
			return ts * 1000, nil // microseconds
		} else if ts > 1e12 {
			return ts * 1000000, nil // milliseconds
		} else {
			return ts * 1000000000, nil // seconds
		}
	}

	// Try as RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano(), nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UnixNano(), nil
	}

	return 0, fmt.Errorf("unable to parse time: %s", s)
}

func resultsToPoints(series []QuerySeries) []*storage.DataPoint {
	var points []*storage.DataPoint

	for _, s := range series {
		// Find column indices
		timeIdx := -1
		fieldCols := make(map[int]string)

		for i, col := range s.Columns {
			if col == "time" {
				timeIdx = i
			} else {
				fieldCols[i] = col
			}
		}

		for _, row := range s.Values {
			p := &storage.DataPoint{
				Measurement: s.Name,
				Tags:        s.Tags,
				Fields:      make(storage.Fields),
			}

			// Get timestamp
			if timeIdx >= 0 && timeIdx < len(row) {
				switch v := row[timeIdx].(type) {
				case float64:
					p.Timestamp = int64(v)
				case int64:
					p.Timestamp = v
				case string:
					if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
						p.Timestamp = t.UnixNano()
					}
				}
			}

			// Get fields
			for idx, name := range fieldCols {
				if idx >= len(row) || row[idx] == nil {
					continue
				}
				switch v := row[idx].(type) {
				case float64:
					p.Fields[name] = storage.NewFloatField(v)
				case int64:
					p.Fields[name] = storage.NewIntField(v)
				case string:
					p.Fields[name] = storage.NewStringField(v)
				case bool:
					p.Fields[name] = storage.NewBoolField(v)
				}
			}

			if len(p.Fields) > 0 {
				points = append(points, p)
			}
		}
	}

	return points
}

func writeLineProtocol(w io.Writer, points []*storage.DataPoint) (int, error) {
	for _, p := range points {
		line := formatStorageLineProtocol(p)
		if _, err := fmt.Fprintln(w, line); err != nil {
			return 0, err
		}
	}
	return len(points), nil
}

func formatStorageLineProtocol(p *storage.DataPoint) string {
	var sb strings.Builder

	sb.WriteString(p.Measurement)

	for k, v := range p.Tags {
		sb.WriteByte(',')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(v)
	}

	sb.WriteByte(' ')

	first := true
	for k, v := range p.Fields {
		if !first {
			sb.WriteByte(',')
		}
		first = false

		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(formatStorageFieldValue(v))
	}

	sb.WriteByte(' ')
	sb.WriteString(fmt.Sprintf("%d", p.Timestamp))

	return sb.String()
}

func formatStorageFieldValue(fv storage.FieldValue) string {
	switch fv.Type {
	case storage.FieldTypeFloat:
		return fmt.Sprintf("%v", fv.FloatValue)
	case storage.FieldTypeInteger:
		return fmt.Sprintf("%di", fv.IntValue)
	case storage.FieldTypeString:
		return fmt.Sprintf("%q", fv.StringValue)
	case storage.FieldTypeBoolean:
		if fv.BooleanValue {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func writeJSON(w io.Writer, points []*storage.DataPoint) (int, error) {
	data, err := protocol.ToJSON(convertToProtocolPoints(points))
	if err != nil {
		return 0, err
	}

	// Pretty print
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		return 0, err
	}

	if _, err := buf.WriteTo(w); err != nil {
		return 0, err
	}

	return len(points), nil
}

func writeCSV(w io.Writer, points []*storage.DataPoint) (int, error) {
	if err := protocol.ToCSV(convertToProtocolPoints(points), w); err != nil {
		return 0, err
	}
	return len(points), nil
}

func convertToProtocolPoints(storagePoints []*storage.DataPoint) []*storage.DataPoint {
	// They're the same type, just return
	return storagePoints
}
