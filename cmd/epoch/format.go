package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Precision constants
const (
	PrecisionNanoseconds  = "ns"
	PrecisionMicroseconds = "us"
	PrecisionMilliseconds = "ms"
	PrecisionSeconds      = "s"
)

// FormatTimestamp formats a timestamp according to precision
func FormatTimestamp(ts int64, precision string) string {
	var t time.Time

	switch precision {
	case PrecisionSeconds:
		t = time.Unix(ts, 0)
	case PrecisionMilliseconds:
		t = time.UnixMilli(ts)
	case PrecisionMicroseconds:
		t = time.UnixMicro(ts)
	default: // nanoseconds
		t = time.Unix(0, ts)
	}

	return t.UTC().Format(time.RFC3339Nano)
}

// ParseTimestamp parses a timestamp string to nanoseconds
func ParseTimestamp(s string, precision string) (int64, error) {
	// Try parsing as RFC3339
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano(), nil
	}

	// Try parsing as integer
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp: %s", s)
	}

	// Convert to nanoseconds based on precision
	switch precision {
	case PrecisionSeconds:
		return val * 1e9, nil
	case PrecisionMilliseconds:
		return val * 1e6, nil
	case PrecisionMicroseconds:
		return val * 1e3, nil
	default:
		return val, nil
	}
}

// FormatDuration formats a duration in a human-readable way
func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fus", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// ParseDuration parses an InfluxDB-style duration string
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	var total time.Duration
	var num string

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c >= '0' && c <= '9' || c == '.' {
			num += string(c)
			continue
		}

		if num == "" {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}

		val, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration number: %s", num)
		}

		var unit time.Duration
		switch c {
		case 'n': // ns
			if i+1 < len(s) && s[i+1] == 's' {
				i++
			}
			unit = time.Nanosecond
		case 'u', 'µ': // us
			if i+1 < len(s) && s[i+1] == 's' {
				i++
			}
			unit = time.Microsecond
		case 'm':
			if i+1 < len(s) && s[i+1] == 's' {
				i++
				unit = time.Millisecond
			} else {
				unit = time.Minute
			}
		case 's':
			unit = time.Second
		case 'h':
			unit = time.Hour
		case 'd':
			unit = 24 * time.Hour
		case 'w':
			unit = 7 * 24 * time.Hour
		default:
			return 0, fmt.Errorf("invalid duration unit: %c", c)
		}

		total += time.Duration(val * float64(unit))
		num = ""
	}

	if num != "" {
		// Trailing number without unit, assume nanoseconds
		val, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}
		total += time.Duration(val)
	}

	return total, nil
}

// FormatBytes formats a byte count in a human-readable way
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatNumber formats a number with commas
func FormatNumber(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	start := len(str) % 3
	if start == 0 {
		start = 3
	}
	result.WriteString(str[:start])

	for i := start; i < len(str); i += 3 {
		result.WriteByte(',')
		result.WriteString(str[i : i+3])
	}

	return result.String()
}

// TruncateString truncates a string to maxLen, adding ellipsis if needed
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// PadRight pads a string to width with spaces on the right
func PadRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// PadLeft pads a string to width with spaces on the left
func PadLeft(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return strings.Repeat(" ", width-len(s)) + s
}

// Center centers a string within width
func Center(s string, width int) string {
	if len(s) >= width {
		return s
	}
	left := (width - len(s)) / 2
	right := width - len(s) - left
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", right)
}
