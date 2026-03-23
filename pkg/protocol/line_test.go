package protocol

import (
	"testing"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

func TestParseLineProtocolSimple(t *testing.T) {
	line := "cpu usage=45.2 1679616000000000000"
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}

	p := points[0]
	if p.Measurement != "cpu" {
		t.Errorf("measurement = %q, want %q", p.Measurement, "cpu")
	}
	if len(p.Tags) != 0 {
		t.Errorf("expected 0 tags, got %d", len(p.Tags))
	}
	if len(p.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(p.Fields))
	}
	if p.Fields["usage"].Type != storage.FieldTypeFloat {
		t.Errorf("field type = %v, want Float", p.Fields["usage"].Type)
	}
	if p.Fields["usage"].FloatValue != 45.2 {
		t.Errorf("field value = %v, want 45.2", p.Fields["usage"].FloatValue)
	}
	if p.Timestamp != 1679616000000000000 {
		t.Errorf("timestamp = %d, want 1679616000000000000", p.Timestamp)
	}
}

func TestParseLineProtocolWithTags(t *testing.T) {
	line := "cpu,host=server1,region=us-west usage=45.2,temp=62i 1679616000000000000"
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	p := points[0]
	if p.Measurement != "cpu" {
		t.Errorf("measurement = %q, want %q", p.Measurement, "cpu")
	}
	if len(p.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(p.Tags))
	}
	if p.Tags["host"] != "server1" {
		t.Errorf("tag host = %q, want %q", p.Tags["host"], "server1")
	}
	if p.Tags["region"] != "us-west" {
		t.Errorf("tag region = %q, want %q", p.Tags["region"], "us-west")
	}
	if len(p.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(p.Fields))
	}
	if p.Fields["temp"].Type != storage.FieldTypeInteger {
		t.Errorf("temp type = %v, want Integer", p.Fields["temp"].Type)
	}
	if p.Fields["temp"].IntValue != 62 {
		t.Errorf("temp value = %d, want 62", p.Fields["temp"].IntValue)
	}
}

func TestParseLineProtocolAllFieldTypes(t *testing.T) {
	line := `test float=3.14,int=42i,str="hello world",bool=true 1679616000000000000`
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	p := points[0]

	// Float
	if p.Fields["float"].Type != storage.FieldTypeFloat {
		t.Errorf("float type = %v, want Float", p.Fields["float"].Type)
	}
	if p.Fields["float"].FloatValue != 3.14 {
		t.Errorf("float value = %v, want 3.14", p.Fields["float"].FloatValue)
	}

	// Integer
	if p.Fields["int"].Type != storage.FieldTypeInteger {
		t.Errorf("int type = %v, want Integer", p.Fields["int"].Type)
	}
	if p.Fields["int"].IntValue != 42 {
		t.Errorf("int value = %d, want 42", p.Fields["int"].IntValue)
	}

	// String
	if p.Fields["str"].Type != storage.FieldTypeString {
		t.Errorf("str type = %v, want String", p.Fields["str"].Type)
	}
	if p.Fields["str"].StringValue != "hello world" {
		t.Errorf("str value = %q, want %q", p.Fields["str"].StringValue, "hello world")
	}

	// Boolean
	if p.Fields["bool"].Type != storage.FieldTypeBoolean {
		t.Errorf("bool type = %v, want Boolean", p.Fields["bool"].Type)
	}
	if !p.Fields["bool"].BooleanValue {
		t.Errorf("bool value = false, want true")
	}
}

func TestParseLineProtocolBooleanVariants(t *testing.T) {
	variants := []struct {
		input string
		want  bool
	}{
		{"test v=t", true},
		{"test v=T", true},
		{"test v=true", true},
		{"test v=True", true},
		{"test v=TRUE", true},
		{"test v=f", false},
		{"test v=F", false},
		{"test v=false", false},
		{"test v=False", false},
		{"test v=FALSE", false},
	}

	for _, tt := range variants {
		t.Run(tt.input, func(t *testing.T) {
			points, err := ParseLineProtocol(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if points[0].Fields["v"].BooleanValue != tt.want {
				t.Errorf("got %v, want %v", points[0].Fields["v"].BooleanValue, tt.want)
			}
		})
	}
}

func TestParseLineProtocolNoTimestamp(t *testing.T) {
	before := time.Now().UnixNano()
	points, err := ParseLineProtocol("cpu usage=45.2")
	after := time.Now().UnixNano()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ts := points[0].Timestamp
	if ts < before || ts > after {
		t.Errorf("timestamp %d not in range [%d, %d]", ts, before, after)
	}
}

func TestParseLineProtocolEscapedMeasurement(t *testing.T) {
	line := `cpu\ usage,host=server1 value=1 1679616000000000000`
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if points[0].Measurement != "cpu usage" {
		t.Errorf("measurement = %q, want %q", points[0].Measurement, "cpu usage")
	}
}

func TestParseLineProtocolEscapedTags(t *testing.T) {
	line := `cpu,host\ name=server\ 1,region\=zone=us\,west value=1 1679616000000000000`
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	p := points[0]
	if p.Tags["host name"] != "server 1" {
		t.Errorf("tag 'host name' = %q, want %q", p.Tags["host name"], "server 1")
	}
	if p.Tags["region=zone"] != "us,west" {
		t.Errorf("tag 'region=zone' = %q, want %q", p.Tags["region=zone"], "us,west")
	}
}

func TestParseLineProtocolEscapedString(t *testing.T) {
	line := `test msg="hello \"world\"" 1679616000000000000`
	points, err := ParseLineProtocol(line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if points[0].Fields["msg"].StringValue != `hello "world"` {
		t.Errorf("msg = %q, want %q", points[0].Fields["msg"].StringValue, `hello "world"`)
	}
}

func TestParseLineProtocolMultipleLines(t *testing.T) {
	lines := `cpu,host=server1 usage=45.2 1679616000000000000
cpu,host=server2 usage=67.8 1679616001000000000
# this is a comment
cpu,host=server3 usage=23.4 1679616002000000000`

	points, err := ParseLineProtocol(lines)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if len(points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(points))
	}

	if points[0].Tags["host"] != "server1" {
		t.Errorf("point 0 host = %q", points[0].Tags["host"])
	}
	if points[1].Tags["host"] != "server2" {
		t.Errorf("point 1 host = %q", points[1].Tags["host"])
	}
	if points[2].Tags["host"] != "server3" {
		t.Errorf("point 2 host = %q", points[2].Tags["host"])
	}
}

func TestParseLineProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"no measurement", ",host=server1 value=1"},
		{"no fields", "cpu,host=server1"},
		{"empty field key", "cpu =1"},
		{"empty tag key", "cpu,=value field=1"},
		{"invalid integer", "cpu value=notani"},
		{"invalid float", "cpu value=not.a.float"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseLineProtocol(tt.input)
			if err == nil {
				t.Errorf("expected error for %q", tt.input)
			}
		})
	}
}

func TestFormatLineProtocol(t *testing.T) {
	dp := &storage.DataPoint{
		Measurement: "cpu",
		Tags: storage.Tags{
			"host":   "server1",
			"region": "us-west",
		},
		Fields: storage.Fields{
			"usage": storage.NewFloatField(45.2),
			"temp":  storage.NewIntField(62),
		},
		Timestamp: 1679616000000000000,
	}

	result := FormatLineProtocol(dp)

	// Parse it back
	points, err := ParseLineProtocol(result)
	if err != nil {
		t.Fatalf("failed to parse formatted output: %v", err)
	}

	p := points[0]
	if p.Measurement != dp.Measurement {
		t.Errorf("measurement = %q, want %q", p.Measurement, dp.Measurement)
	}
	if p.Tags["host"] != "server1" {
		t.Errorf("host = %q", p.Tags["host"])
	}
	if p.Fields["usage"].FloatValue != 45.2 {
		t.Errorf("usage = %v", p.Fields["usage"].FloatValue)
	}
	if p.Timestamp != dp.Timestamp {
		t.Errorf("timestamp = %d", p.Timestamp)
	}
}

func TestFormatLineProtocolAllTypes(t *testing.T) {
	dp := &storage.DataPoint{
		Measurement: "test",
		Fields: storage.Fields{
			"float":  storage.NewFloatField(3.14),
			"int":    storage.NewIntField(42),
			"str":    storage.NewStringField("hello"),
			"bool_t": storage.NewBoolField(true),
			"bool_f": storage.NewBoolField(false),
		},
		Timestamp: 1679616000000000000,
	}

	result := FormatLineProtocol(dp)
	points, err := ParseLineProtocol(result)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	p := points[0]
	if p.Fields["float"].FloatValue != 3.14 {
		t.Errorf("float = %v", p.Fields["float"].FloatValue)
	}
	if p.Fields["int"].IntValue != 42 {
		t.Errorf("int = %d", p.Fields["int"].IntValue)
	}
	if p.Fields["str"].StringValue != "hello" {
		t.Errorf("str = %q", p.Fields["str"].StringValue)
	}
	if !p.Fields["bool_t"].BooleanValue {
		t.Error("bool_t should be true")
	}
	if p.Fields["bool_f"].BooleanValue {
		t.Error("bool_f should be false")
	}
}

func TestEscapeFunctions(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		escape func(string) string
		want   string
	}{
		{"measurement space", "cpu usage", EscapeMeasurement, `cpu\ usage`},
		{"measurement comma", "cpu,idle", EscapeMeasurement, `cpu\,idle`},
		{"tag key equals", "region=zone", EscapeTagKey, `region\=zone`},
		{"string quote", `hello "world"`, EscapeString, `hello \"world\"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.escape(tt.input)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func BenchmarkParseLineProtocol(b *testing.B) {
	line := "cpu,host=server1,region=us-west usage=45.2,temp=62i,active=true 1679616000000000000"

	for i := 0; i < b.N; i++ {
		ParseLineProtocol(line)
	}
}

func BenchmarkFormatLineProtocol(b *testing.B) {
	dp := &storage.DataPoint{
		Measurement: "cpu",
		Tags:        storage.Tags{"host": "server1", "region": "us-west"},
		Fields:      storage.Fields{"usage": storage.NewFloatField(45.2), "temp": storage.NewIntField(62)},
		Timestamp:   1679616000000000000,
	}

	for i := 0; i < b.N; i++ {
		FormatLineProtocol(dp)
	}
}
