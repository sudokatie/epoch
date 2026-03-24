package protocol

import (
	"testing"
)

func TestParseJSONSinglePoint(t *testing.T) {
	data := `{
		"measurement": "cpu",
		"tags": {"host": "server01"},
		"fields": {"value": 0.64},
		"timestamp": 1434055562000000000
	}`

	points, err := ParseJSON([]byte(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}

	p := points[0]
	if p.Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got %q", p.Measurement)
	}
	if p.Tags["host"] != "server01" {
		t.Errorf("expected tag host=server01, got %q", p.Tags["host"])
	}
	if p.Timestamp != 1434055562000000000 {
		t.Errorf("expected timestamp 1434055562000000000, got %d", p.Timestamp)
	}
}

func TestParseJSONArray(t *testing.T) {
	data := `[
		{"measurement": "cpu", "fields": {"value": 0.64}},
		{"measurement": "mem", "fields": {"used": 1024}}
	]`

	points, err := ParseJSON([]byte(data))
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

func TestParseJSONBatch(t *testing.T) {
	data := `{
		"database": "mydb",
		"points": [
			{"measurement": "cpu", "fields": {"value": 0.5}},
			{"measurement": "cpu", "fields": {"value": 0.6}}
		]
	}`

	points, err := ParseJSON([]byte(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}
}

func TestParseJSONWithTimeString(t *testing.T) {
	data := `{
		"measurement": "cpu",
		"fields": {"value": 1.0},
		"time": "2024-01-15T12:00:00Z"
	}`

	points, err := ParseJSON([]byte(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}

	// 2024-01-15T12:00:00Z in nanoseconds
	expected := int64(1705320000000000000)
	if points[0].Timestamp != expected {
		t.Errorf("expected timestamp %d, got %d", expected, points[0].Timestamp)
	}
}

func TestParseJSONFieldTypes(t *testing.T) {
	data := `{
		"measurement": "test",
		"fields": {
			"float_val": 3.14,
			"int_val": 42,
			"str_val": "hello",
			"bool_val": true
		}
	}`

	points, err := ParseJSON([]byte(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fields := points[0].Fields
	if fields["str_val"].StringValue != "hello" {
		t.Errorf("expected string field 'hello'")
	}
	if fields["bool_val"].BooleanValue != true {
		t.Errorf("expected bool field true")
	}
}

func TestParseJSONMissingMeasurement(t *testing.T) {
	data := `{"fields": {"value": 1.0}}`

	_, err := ParseJSON([]byte(data))
	if err == nil {
		t.Error("expected error for missing measurement")
	}
}

func TestParseJSONMissingFields(t *testing.T) {
	data := `{"measurement": "cpu"}`

	_, err := ParseJSON([]byte(data))
	if err == nil {
		t.Error("expected error for missing fields")
	}
}

func TestToJSON(t *testing.T) {
	points, _ := ParseLineProtocol("cpu,host=server01 value=0.64 1434055562000000000")

	data, err := ToJSON(points)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Parse it back
	parsed, err := ParseJSON(data)
	if err != nil {
		t.Fatalf("failed to parse generated JSON: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 point, got %d", len(parsed))
	}

	if parsed[0].Measurement != "cpu" {
		t.Errorf("expected measurement 'cpu', got %q", parsed[0].Measurement)
	}
}
