package protocol

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

// CSVConfig holds configuration for CSV parsing
type CSVConfig struct {
	// Measurement name (required if not in CSV)
	Measurement string
	// TagColumns lists columns that are tags (by name or index)
	TagColumns []string
	// FieldColumns lists columns that are fields (if empty, all non-tag, non-time columns are fields)
	FieldColumns []string
	// TimeColumn is the name of the timestamp column
	TimeColumn string
	// TimeFormat is the format for parsing timestamps (default: RFC3339)
	TimeFormat string
	// Delimiter is the CSV delimiter (default: comma)
	Delimiter rune
	// HasHeader indicates if the first row is a header
	HasHeader bool
}

// DefaultCSVConfig returns default CSV configuration
func DefaultCSVConfig() CSVConfig {
	return CSVConfig{
		TimeColumn: "time",
		TimeFormat: time.RFC3339Nano,
		Delimiter:  ',',
		HasHeader:  true,
	}
}

// ParseCSV parses CSV-formatted data with headers
func ParseCSV(data string, config CSVConfig) ([]*storage.DataPoint, error) {
	reader := csv.NewReader(strings.NewReader(data))
	reader.Comma = config.Delimiter
	reader.TrimLeadingSpace = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv parse error: %w", err)
	}

	if len(records) == 0 {
		return nil, nil
	}

	var headers []string
	var dataStart int

	if config.HasHeader {
		if len(records) < 2 {
			return nil, nil // Just header, no data
		}
		headers = records[0]
		dataStart = 1
	} else {
		// Generate numeric headers
		headers = make([]string, len(records[0]))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i)
		}
		dataStart = 0
	}

	// Build column index maps
	headerIndex := make(map[string]int)
	for i, h := range headers {
		headerIndex[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// Identify tag columns
	tagCols := make(map[int]string)
	for _, tc := range config.TagColumns {
		if idx, err := strconv.Atoi(tc); err == nil {
			if idx >= 0 && idx < len(headers) {
				tagCols[idx] = headers[idx]
			}
		} else {
			if idx, ok := headerIndex[strings.ToLower(tc)]; ok {
				tagCols[idx] = tc
			}
		}
	}

	// Identify time column
	timeCol := -1
	timeName := strings.ToLower(config.TimeColumn)
	if timeName != "" {
		if idx, ok := headerIndex[timeName]; ok {
			timeCol = idx
		}
	}

	// Identify field columns
	fieldCols := make(map[int]string)
	if len(config.FieldColumns) > 0 {
		for _, fc := range config.FieldColumns {
			if idx, err := strconv.Atoi(fc); err == nil {
				if idx >= 0 && idx < len(headers) {
					fieldCols[idx] = headers[idx]
				}
			} else {
				if idx, ok := headerIndex[strings.ToLower(fc)]; ok {
					fieldCols[idx] = fc
				}
			}
		}
	} else {
		// All non-tag, non-time columns are fields
		for i, h := range headers {
			if i == timeCol {
				continue
			}
			if _, isTag := tagCols[i]; isTag {
				continue
			}
			// Skip measurement column if present
			if strings.ToLower(h) == "measurement" {
				continue
			}
			fieldCols[i] = h
		}
	}

	// Check for measurement column
	measCol := -1
	if idx, ok := headerIndex["measurement"]; ok {
		measCol = idx
	}

	// Parse data rows
	points := make([]*storage.DataPoint, 0, len(records)-dataStart)

	for rowNum, row := range records[dataStart:] {
		if len(row) == 0 {
			continue
		}

		// Get measurement
		measurement := config.Measurement
		if measCol >= 0 && measCol < len(row) {
			measurement = row[measCol]
		}
		if measurement == "" {
			return nil, fmt.Errorf("row %d: measurement is required", rowNum+dataStart+1)
		}

		// Get timestamp
		var timestamp int64
		if timeCol >= 0 && timeCol < len(row) && row[timeCol] != "" {
			ts, err := parseTimestamp(row[timeCol], config.TimeFormat)
			if err != nil {
				return nil, fmt.Errorf("row %d: invalid timestamp: %w", rowNum+dataStart+1, err)
			}
			timestamp = ts
		} else {
			timestamp = time.Now().UnixNano()
		}

		// Get tags
		tags := make(storage.Tags)
		for idx, name := range tagCols {
			if idx < len(row) && row[idx] != "" {
				tags[name] = row[idx]
			}
		}

		// Get fields
		fields := make(storage.Fields)
		for idx, name := range fieldCols {
			if idx >= len(row) || row[idx] == "" {
				continue
			}
			fv, err := parseCSVFieldValue(row[idx])
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", rowNum+dataStart+1, name, err)
			}
			fields[name] = fv
		}

		if len(fields) == 0 {
			continue // Skip rows with no field values
		}

		points = append(points, &storage.DataPoint{
			Measurement: measurement,
			Tags:        tags,
			Fields:      fields,
			Timestamp:   timestamp,
		})
	}

	return points, nil
}

// parseTimestamp parses a timestamp string in various formats
func parseTimestamp(s string, format string) (int64, error) {
	s = strings.TrimSpace(s)

	// Try as Unix timestamp (nanoseconds)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Detect precision based on magnitude
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

	// Try specified format
	if format != "" {
		if t, err := time.Parse(format, s); err == nil {
			return t.UnixNano(), nil
		}
	}

	// Try common formats
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, fmt.Errorf("unable to parse timestamp: %s", s)
}

// parseCSVFieldValue parses a CSV field value, inferring type
func parseCSVFieldValue(s string) (storage.FieldValue, error) {
	s = strings.TrimSpace(s)

	// Try boolean
	lower := strings.ToLower(s)
	if lower == "true" || lower == "t" {
		return storage.NewBoolField(true), nil
	}
	if lower == "false" || lower == "f" {
		return storage.NewBoolField(false), nil
	}

	// Try integer (look for 'i' suffix like line protocol)
	if strings.HasSuffix(s, "i") {
		if i, err := strconv.ParseInt(s[:len(s)-1], 10, 64); err == nil {
			return storage.NewIntField(i), nil
		}
	}

	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return storage.NewIntField(i), nil
	}

	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return storage.NewFloatField(f), nil
	}

	// Default to string
	return storage.NewStringField(s), nil
}

// ToCSV converts DataPoints to CSV format
func ToCSV(points []*storage.DataPoint, w io.Writer) error {
	if len(points) == 0 {
		return nil
	}

	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Collect all unique tag keys and field keys
	tagKeys := make(map[string]bool)
	fieldKeys := make(map[string]bool)

	for _, p := range points {
		for k := range p.Tags {
			tagKeys[k] = true
		}
		for k := range p.Fields {
			fieldKeys[k] = true
		}
	}

	// Build sorted key lists
	tags := sortedKeys(tagKeys)
	fields := sortedKeys(fieldKeys)

	// Write header
	header := []string{"time", "measurement"}
	header = append(header, tags...)
	header = append(header, fields...)

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	for _, p := range points {
		row := make([]string, len(header))
		row[0] = time.Unix(0, p.Timestamp).UTC().Format(time.RFC3339Nano)
		row[1] = p.Measurement

		// Tags
		for i, k := range tags {
			if v, ok := p.Tags[k]; ok {
				row[2+i] = v
			}
		}

		// Fields
		for i, k := range fields {
			if fv, ok := p.Fields[k]; ok {
				row[2+len(tags)+i] = formatFieldValue(fv)
			}
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Sort for deterministic output
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func formatFieldValue(fv storage.FieldValue) string {
	switch fv.Type {
	case storage.FieldTypeFloat:
		return strconv.FormatFloat(fv.FloatValue, 'f', -1, 64)
	case storage.FieldTypeInteger:
		return strconv.FormatInt(fv.IntValue, 10)
	case storage.FieldTypeString:
		return fv.StringValue
	case storage.FieldTypeBoolean:
		return strconv.FormatBool(fv.BooleanValue)
	default:
		return ""
	}
}
