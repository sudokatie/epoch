package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseCSVWithHeader(t *testing.T) {
	data := `time,host,value
2024-01-15T12:00:00Z,server01,0.64
2024-01-15T12:01:00Z,server02,0.55`

	config := DefaultCSVConfig()
	config.Measurement = "cpu"
	config.TagColumns = []string{"host"}

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}

	p := points[0]
	if p.Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got %q", p.Measurement)
	}
	if p.Tags["host"] != "server01" {
		t.Errorf("expected tag host=server01, got %q", p.Tags["host"])
	}
	if p.Fields["value"].FloatValue != 0.64 {
		t.Errorf("expected value 0.64, got %v", p.Fields["value"].FloatValue)
	}
}

func TestParseCSVMeasurementColumn(t *testing.T) {
	data := `measurement,time,value
cpu,2024-01-15T12:00:00Z,0.64
mem,2024-01-15T12:00:00Z,1024`

	config := DefaultCSVConfig()

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}

	if points[0].Measurement != "cpu" {
		t.Errorf("expected first measurement 'cpu', got %q", points[0].Measurement)
	}
	if points[1].Measurement != "mem" {
		t.Errorf("expected second measurement 'mem', got %q", points[1].Measurement)
	}
}

func TestParseCSVUnixTimestamp(t *testing.T) {
	data := `time,value
1434055562000000000,0.64
1434055563,0.55`

	config := DefaultCSVConfig()
	config.Measurement = "test"

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}

	// First should be nanoseconds
	if points[0].Timestamp != 1434055562000000000 {
		t.Errorf("expected timestamp 1434055562000000000, got %d", points[0].Timestamp)
	}

	// Second should be seconds converted to nanoseconds
	if points[1].Timestamp != 1434055563000000000 {
		t.Errorf("expected timestamp 1434055563000000000, got %d", points[1].Timestamp)
	}
}

func TestParseCSVFieldTypes(t *testing.T) {
	data := `float_val,int_val,str_val,bool_val
3.14,42,hello,true`

	config := DefaultCSVConfig()
	config.Measurement = "test"
	config.TimeColumn = "" // No time column

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}

	fields := points[0].Fields

	if fields["float_val"].FloatValue != 3.14 {
		t.Errorf("expected float 3.14, got %v", fields["float_val"].FloatValue)
	}
	if fields["int_val"].IntValue != 42 {
		t.Errorf("expected int 42, got %v", fields["int_val"].IntValue)
	}
	if fields["str_val"].StringValue != "hello" {
		t.Errorf("expected string 'hello', got %v", fields["str_val"].StringValue)
	}
	if fields["bool_val"].BooleanValue != true {
		t.Errorf("expected bool true, got %v", fields["bool_val"].BooleanValue)
	}
}

func TestParseCSVNoHeader(t *testing.T) {
	data := `2024-01-15T12:00:00Z,0.64
2024-01-15T12:01:00Z,0.55`

	config := DefaultCSVConfig()
	config.Measurement = "test"
	config.HasHeader = false
	config.TimeColumn = "col0"
	config.FieldColumns = []string{"col1"}

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}
}

func TestParseCSVCustomDelimiter(t *testing.T) {
	data := `time;value
2024-01-15T12:00:00Z;0.64`

	config := DefaultCSVConfig()
	config.Measurement = "test"
	config.Delimiter = ';'

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}
}

func TestParseCSVEmptyData(t *testing.T) {
	config := DefaultCSVConfig()
	config.Measurement = "test"

	points, err := ParseCSV("", config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 0 {
		t.Errorf("expected 0 points, got %d", len(points))
	}
}

func TestParseCSVHeaderOnly(t *testing.T) {
	data := `time,value`

	config := DefaultCSVConfig()
	config.Measurement = "test"

	points, err := ParseCSV(data, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 0 {
		t.Errorf("expected 0 points, got %d", len(points))
	}
}

func TestToCSV(t *testing.T) {
	points, _ := ParseLineProtocol(`cpu,host=server01 value=0.64 1434055562000000000
cpu,host=server02 value=0.55 1434055562000000000`)

	var buf bytes.Buffer
	err := ToCSV(points, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()

	// Check header
	if !strings.HasPrefix(output, "time,measurement,host,value") {
		t.Errorf("unexpected header: %s", strings.Split(output, "\n")[0])
	}

	// Check we have 3 lines (header + 2 data)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines, got %d", len(lines))
	}
}

func TestCSVRoundTrip(t *testing.T) {
	original := `cpu,host=server01,region=us-west value=0.64,temp=62i 1434055562000000000`

	points, err := ParseLineProtocol(original)
	if err != nil {
		t.Fatalf("failed to parse line protocol: %v", err)
	}

	var buf bytes.Buffer
	if err := ToCSV(points, &buf); err != nil {
		t.Fatalf("failed to convert to CSV: %v", err)
	}

	config := DefaultCSVConfig()
	config.TagColumns = []string{"host", "region"}

	parsed, err := ParseCSV(buf.String(), config)
	if err != nil {
		t.Fatalf("failed to parse CSV: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 point, got %d", len(parsed))
	}

	p := parsed[0]
	if p.Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got %q", p.Measurement)
	}
	if p.Tags["host"] != "server01" {
		t.Errorf("expected host=server01, got %q", p.Tags["host"])
	}
}
