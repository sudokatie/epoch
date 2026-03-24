package protocol

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

// ParseLineProtocol parses one or more lines of InfluxDB line protocol.
// Format: <measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]
func ParseLineProtocol(data string) ([]*storage.DataPoint, error) {
	var points []*storage.DataPoint

	lines := strings.Split(data, "\n")
	for lineNum, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue // skip empty lines and comments
		}

		point, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum+1, err)
		}
		points = append(points, point)
	}

	return points, nil
}

// parseLine parses a single line of line protocol
func parseLine(line string) (*storage.DataPoint, error) {
	// Split into: key (measurement + tags), fields, timestamp
	// Key and fields are separated by first unescaped space
	// Fields and timestamp by last unescaped space (if timestamp present)

	keyEnd := findUnescapedSpace(line)
	if keyEnd == -1 {
		return nil, fmt.Errorf("no field set found")
	}

	key := line[:keyEnd]
	rest := line[keyEnd+1:]

	// Parse measurement and tags from key
	measurement, tags, err := parseKey(key)
	if err != nil {
		return nil, err
	}

	// Find timestamp (last space-separated value if it's a number)
	var fieldsStr string
	var timestamp int64

	lastSpace := strings.LastIndex(rest, " ")
	if lastSpace != -1 {
		maybeTs := rest[lastSpace+1:]
		maybeFields := rest[:lastSpace]

		// Try to parse as timestamp
		ts, err := strconv.ParseInt(maybeTs, 10, 64)
		if err == nil {
			timestamp = ts
			fieldsStr = maybeFields
		} else {
			// Not a timestamp, all of rest is fields
			fieldsStr = rest
		}
	} else {
		fieldsStr = rest
	}

	// Use current time if no timestamp
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

	// Parse fields
	fields, err := parseFields(fieldsStr)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("at least one field is required")
	}

	return &storage.DataPoint{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Timestamp:   timestamp,
	}, nil
}

// parseKey parses the measurement and tags from the key portion
func parseKey(key string) (string, storage.Tags, error) {
	// Measurement is everything before first unescaped comma
	commaIdx := findUnescapedComma(key)

	if commaIdx == -1 {
		// No tags
		measurement := unescapeMeasurement(key)
		if measurement == "" {
			return "", nil, fmt.Errorf("measurement name is required")
		}
		return measurement, nil, nil
	}

	measurement := unescapeMeasurement(key[:commaIdx])
	if measurement == "" {
		return "", nil, fmt.Errorf("measurement name is required")
	}

	tagsStr := key[commaIdx+1:]
	tags, err := parseTags(tagsStr)
	if err != nil {
		return "", nil, err
	}

	return measurement, tags, nil
}

// parseTags parses comma-separated tag key=value pairs
func parseTags(tagsStr string) (storage.Tags, error) {
	if tagsStr == "" {
		return nil, nil
	}

	tags := make(storage.Tags)
	pairs := splitUnescaped(tagsStr, ',')

	for _, pair := range pairs {
		eqIdx := findUnescapedEquals(pair)
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid tag: %q (missing =)", pair)
		}

		key := unescapeTagKey(pair[:eqIdx])
		value := unescapeTagValue(pair[eqIdx+1:])

		if key == "" {
			return nil, fmt.Errorf("tag key cannot be empty")
		}

		tags[key] = value
	}

	return tags, nil
}

// parseFields parses comma-separated field key=value pairs
func parseFields(fieldsStr string) (storage.Fields, error) {
	if fieldsStr == "" {
		return nil, fmt.Errorf("field set is required")
	}

	fields := make(storage.Fields)
	pairs := splitUnescaped(fieldsStr, ',')

	for _, pair := range pairs {
		eqIdx := findUnescapedEquals(pair)
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid field: %q (missing =)", pair)
		}

		key := unescapeFieldKey(pair[:eqIdx])
		valueStr := pair[eqIdx+1:]

		if key == "" {
			return nil, fmt.Errorf("field key cannot be empty")
		}

		value, err := parseFieldValue(valueStr)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}

		fields[key] = value
	}

	return fields, nil
}

// parseFieldValue parses a field value and determines its type
func parseFieldValue(s string) (storage.FieldValue, error) {
	if s == "" {
		return storage.FieldValue{}, fmt.Errorf("field value cannot be empty")
	}

	// Boolean: t, T, true, True, TRUE, f, F, false, False, FALSE
	switch s {
	case "t", "T", "true", "True", "TRUE":
		return storage.NewBoolField(true), nil
	case "f", "F", "false", "False", "FALSE":
		return storage.NewBoolField(false), nil
	}

	// String: starts and ends with quotes
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		// Unescape string content
		content := s[1 : len(s)-1]
		unescaped := unescapeString(content)
		return storage.NewStringField(unescaped), nil
	}

	// Integer: ends with 'i'
	if len(s) > 1 && s[len(s)-1] == 'i' {
		numStr := s[:len(s)-1]
		n, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return storage.FieldValue{}, fmt.Errorf("invalid integer: %s", numStr)
		}
		return storage.NewIntField(n), nil
	}

	// Float: everything else that parses as a number
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return storage.FieldValue{}, fmt.Errorf("invalid field value: %s", s)
	}
	return storage.NewFloatField(f), nil
}

// findUnescapedSpace finds the first unescaped space
func findUnescapedSpace(s string) int {
	escaped := false
	for i := 0; i < len(s); i++ {
		if escaped {
			escaped = false
			continue
		}
		if s[i] == '\\' {
			escaped = true
			continue
		}
		if s[i] == ' ' {
			return i
		}
	}
	return -1
}

// findUnescapedComma finds the first unescaped comma
func findUnescapedComma(s string) int {
	escaped := false
	for i := 0; i < len(s); i++ {
		if escaped {
			escaped = false
			continue
		}
		if s[i] == '\\' {
			escaped = true
			continue
		}
		if s[i] == ',' {
			return i
		}
	}
	return -1
}

// findUnescapedEquals finds the first unescaped equals sign
func findUnescapedEquals(s string) int {
	escaped := false
	for i := 0; i < len(s); i++ {
		if escaped {
			escaped = false
			continue
		}
		if s[i] == '\\' {
			escaped = true
			continue
		}
		if s[i] == '=' {
			return i
		}
	}
	return -1
}

// splitUnescaped splits a string by an unescaped delimiter
func splitUnescaped(s string, delim byte) []string {
	var result []string
	var current strings.Builder
	escaped := false

	for i := 0; i < len(s); i++ {
		if escaped {
			current.WriteByte(s[i])
			escaped = false
			continue
		}
		if s[i] == '\\' {
			current.WriteByte(s[i])
			escaped = true
			continue
		}
		if s[i] == delim {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(s[i])
	}

	// Don't forget the last segment
	if current.Len() > 0 || len(s) > 0 {
		result = append(result, current.String())
	}

	return result
}

// Unescape functions for different contexts
// In line protocol:
// - Measurement: escape space, comma
// - Tag key/value: escape space, comma, equals
// - Field key: escape space, comma, equals
// - String field value: escape backslash, double-quote

func unescapeMeasurement(s string) string {
	return unescape(s, map[byte]byte{
		' ':  ' ',
		',':  ',',
		'\\': '\\',
	})
}

func unescapeTagKey(s string) string {
	return unescape(s, map[byte]byte{
		' ':  ' ',
		',':  ',',
		'=':  '=',
		'\\': '\\',
	})
}

func unescapeTagValue(s string) string {
	return unescapeTagKey(s)
}

func unescapeFieldKey(s string) string {
	return unescapeTagKey(s)
}

func unescapeString(s string) string {
	return unescape(s, map[byte]byte{
		'"':  '"',
		'\\': '\\',
	})
}

func unescape(s string, escapes map[byte]byte) string {
	var result strings.Builder
	result.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			if replacement, ok := escapes[s[i+1]]; ok {
				result.WriteByte(replacement)
				i++
				continue
			}
		}
		result.WriteByte(s[i])
	}

	return result.String()
}

// Escaping functions for writing line protocol

// EscapeMeasurement escapes a measurement name for line protocol
func EscapeMeasurement(s string) string {
	return escape(s, " ,")
}

// EscapeTagKey escapes a tag key for line protocol
func EscapeTagKey(s string) string {
	return escape(s, " ,=")
}

// EscapeTagValue escapes a tag value for line protocol
func EscapeTagValue(s string) string {
	return escape(s, " ,=")
}

// EscapeFieldKey escapes a field key for line protocol
func EscapeFieldKey(s string) string {
	return escape(s, " ,=")
}

// EscapeString escapes a string field value for line protocol
func EscapeString(s string) string {
	return escape(s, "\"\\")
}

func escape(s string, chars string) string {
	var result strings.Builder
	result.Grow(len(s) * 2) // worst case

	for i := 0; i < len(s); i++ {
		if strings.ContainsRune(chars, rune(s[i])) {
			result.WriteByte('\\')
		}
		result.WriteByte(s[i])
	}

	return result.String()
}

// FormatLineProtocol formats a DataPoint as line protocol
func FormatLineProtocol(dp *storage.DataPoint) string {
	var sb strings.Builder

	// Measurement
	sb.WriteString(EscapeMeasurement(dp.Measurement))

	// Tags (sorted for deterministic output)
	if len(dp.Tags) > 0 {
		sb.WriteString(",")
		sb.WriteString(formatTags(dp.Tags))
	}

	// Fields
	sb.WriteString(" ")
	sb.WriteString(formatFields(dp.Fields))

	// Timestamp
	sb.WriteString(" ")
	sb.WriteString(strconv.FormatInt(dp.Timestamp, 10))

	return sb.String()
}

func formatTags(tags storage.Tags) string {
	// Need sorted keys for deterministic output
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	// Simple sort
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(EscapeTagKey(k))
		sb.WriteByte('=')
		sb.WriteString(EscapeTagValue(tags[k]))
	}
	return sb.String()
}

func formatFields(fields storage.Fields) string {
	// Need sorted keys for deterministic output
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(EscapeFieldKey(k))
		sb.WriteByte('=')
		sb.WriteString(formatLineFieldValue(fields[k]))
	}
	return sb.String()
}

func formatLineFieldValue(fv storage.FieldValue) string {
	switch fv.Type {
	case storage.FieldTypeFloat:
		return strconv.FormatFloat(fv.FloatValue, 'f', -1, 64)
	case storage.FieldTypeInteger:
		return strconv.FormatInt(fv.IntValue, 10) + "i"
	case storage.FieldTypeString:
		return "\"" + EscapeString(fv.StringValue) + "\""
	case storage.FieldTypeBoolean:
		if fv.BooleanValue {
			return "true"
		}
		return "false"
	default:
		return "0"
	}
}
