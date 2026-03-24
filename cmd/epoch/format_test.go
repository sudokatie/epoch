package main

import (
	"testing"
	"time"
)

func TestFormatTimestamp(t *testing.T) {
	// 2024-01-15 12:00:00 UTC in nanoseconds
	ts := int64(1705320000000000000)

	tests := []struct {
		precision string
		input     int64
		wantTime  string
	}{
		{"ns", ts, "2024-01-15T12:00:00Z"},
		{"us", ts / 1000, "2024-01-15T12:00:00Z"},
		{"ms", ts / 1000000, "2024-01-15T12:00:00Z"},
		{"s", ts / 1000000000, "2024-01-15T12:00:00Z"},
	}

	for _, tt := range tests {
		t.Run(tt.precision, func(t *testing.T) {
			got := FormatTimestamp(tt.input, tt.precision)
			// Just check that it parses and contains expected date
			if got[:10] != "2024-01-15" {
				t.Errorf("FormatTimestamp() = %v, want date 2024-01-15", got)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		precision string
		want      int64
		wantErr   bool
	}{
		{
			name:      "RFC3339",
			input:     "2024-01-15T12:00:00Z",
			precision: "ns",
			want:      1705320000000000000,
		},
		{
			name:      "seconds integer",
			input:     "1705320000",
			precision: "s",
			want:      1705320000000000000,
		},
		{
			name:      "milliseconds integer",
			input:     "1705320000000",
			precision: "ms",
			want:      1705320000000000000,
		},
		{
			name:      "nanoseconds integer",
			input:     "1705320000000000000",
			precision: "ns",
			want:      1705320000000000000,
		},
		{
			name:      "invalid",
			input:     "not-a-timestamp",
			precision: "ns",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimestamp(tt.input, tt.precision)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{500 * time.Nanosecond, "500ns"},
		{1500 * time.Nanosecond, "1.50us"},
		{1500 * time.Microsecond, "1.50ms"},
		{1500 * time.Millisecond, "1.50s"},
		{90 * time.Second, "1.5m"},
		{90 * time.Minute, "1.5h"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatDuration(tt.input)
			if got != tt.want {
				t.Errorf("FormatDuration(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"1s", time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"100ms", 100 * time.Millisecond, false},
		{"50us", 50 * time.Microsecond, false},
		{"1h30m", 90 * time.Minute, false},
		{"", 0, true},
		{"abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatBytes(tt.input)
			if got != tt.want {
				t.Errorf("FormatBytes(%d) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{1234567890, "1,234,567,890"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatNumber(tt.input)
			if got != tt.want {
				t.Errorf("FormatNumber(%d) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello world", 8, "hello..."},
		{"abc", 2, "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := TruncateString(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("TruncateString(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
		})
	}
}

func TestPadRight(t *testing.T) {
	tests := []struct {
		input string
		width int
		want  string
	}{
		{"hello", 10, "hello     "},
		{"hello", 5, "hello"},
		{"hello", 3, "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := PadRight(tt.input, tt.width)
			if got != tt.want {
				t.Errorf("PadRight(%q, %d) = %q, want %q", tt.input, tt.width, got, tt.want)
			}
		})
	}
}

func TestPadLeft(t *testing.T) {
	tests := []struct {
		input string
		width int
		want  string
	}{
		{"hello", 10, "     hello"},
		{"hello", 5, "hello"},
		{"hello", 3, "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := PadLeft(tt.input, tt.width)
			if got != tt.want {
				t.Errorf("PadLeft(%q, %d) = %q, want %q", tt.input, tt.width, got, tt.want)
			}
		})
	}
}

func TestCenter(t *testing.T) {
	tests := []struct {
		input string
		width int
		want  string
	}{
		{"hi", 6, "  hi  "},
		{"hello", 5, "hello"},
		{"a", 4, " a  "},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := Center(tt.input, tt.width)
			if got != tt.want {
				t.Errorf("Center(%q, %d) = %q, want %q", tt.input, tt.width, got, tt.want)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{nil, "<null>"},
		{3.14159, "3.14159"},
		{100.0, "100"},
		{"hello", "hello"},
		{true, "true"},
		{false, "false"},
		{42, "42"},
	}

	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.want {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatCSVValue(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{nil, ""},
		{"hello", "hello"},
		{"hello,world", `"hello,world"`},
		{"say \"hi\"", `"say \"hi\""`},
		{100.0, "100"},
	}

	for _, tt := range tests {
		got := formatCSVValue(tt.input)
		if got != tt.want {
			t.Errorf("formatCSVValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseTimestampEdgeCases(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"invalid", true},
		{"", true},
		{"abc123", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := ParseTimestamp(tt.input, "ns")
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimestamp(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParseDurationEdgeCases(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"invalid", true},
		{"", true},
		{"10x", true},
		{"-5m", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}
