package storage

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// FieldType represents the type of a field value
type FieldType int

const (
	FieldTypeFloat FieldType = iota
	FieldTypeInteger
	FieldTypeString
	FieldTypeBoolean
)

func (ft FieldType) String() string {
	switch ft {
	case FieldTypeFloat:
		return "float"
	case FieldTypeInteger:
		return "integer"
	case FieldTypeString:
		return "string"
	case FieldTypeBoolean:
		return "boolean"
	default:
		return "unknown"
	}
}

// FieldValue holds a typed field value
type FieldValue struct {
	Type         FieldType
	FloatValue   float64
	IntValue     int64
	StringValue  string
	BooleanValue bool
}

// NewFloatField creates a float field value
func NewFloatField(v float64) FieldValue {
	return FieldValue{Type: FieldTypeFloat, FloatValue: v}
}

// NewIntField creates an integer field value
func NewIntField(v int64) FieldValue {
	return FieldValue{Type: FieldTypeInteger, IntValue: v}
}

// NewStringField creates a string field value
func NewStringField(v string) FieldValue {
	return FieldValue{Type: FieldTypeString, StringValue: v}
}

// NewBoolField creates a boolean field value
func NewBoolField(v bool) FieldValue {
	return FieldValue{Type: FieldTypeBoolean, BooleanValue: v}
}

// Tags represents a set of key-value tag pairs
type Tags map[string]string

// String returns a sorted, canonical string representation
func (t Tags) String() string {
	if len(t) == 0 {
		return ""
	}
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(t[k])
	}
	return sb.String()
}

// Fields represents a set of field key-value pairs
type Fields map[string]FieldValue

// DataPoint represents a single time series data point
type DataPoint struct {
	Measurement string
	Tags        Tags
	Fields      Fields
	Timestamp   int64 // Unix nanoseconds
}

// Validate checks if the data point is valid
func (dp *DataPoint) Validate() error {
	if dp.Measurement == "" {
		return fmt.Errorf("measurement name is required")
	}
	if len(dp.Fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}
	if dp.Timestamp <= 0 {
		return fmt.Errorf("timestamp must be positive")
	}
	return nil
}

// SeriesKey returns a unique key for this series (measurement + tags)
func (dp *DataPoint) SeriesKey() string {
	if len(dp.Tags) == 0 {
		return dp.Measurement
	}
	return dp.Measurement + "," + dp.Tags.String()
}

// Series represents a unique time series (measurement + tag set)
type Series struct {
	ID          uint64
	Measurement string
	Tags        Tags
	FieldTypes  map[string]FieldType
}

// Key returns the series key
func (s *Series) Key() string {
	if len(s.Tags) == 0 {
		return s.Measurement
	}
	return s.Measurement + "," + s.Tags.String()
}

// ShardInfo contains metadata about a shard
type ShardInfo struct {
	ID        uint64
	Database  string
	StartTime time.Time
	EndTime   time.Time
	Path      string
	ReadOnly  bool
}

// Contains checks if a timestamp falls within this shard's time range
func (si *ShardInfo) Contains(ts int64) bool {
	t := time.Unix(0, ts)
	return !t.Before(si.StartTime) && t.Before(si.EndTime)
}

// Database represents a database with its retention policies
type Database struct {
	Name              string
	RetentionPolicies map[string]*RetentionPolicy
	DefaultRP         string
}

// RetentionPolicy defines data retention settings
type RetentionPolicy struct {
	Name            string
	Duration        time.Duration
	ShardDuration   time.Duration
	ReplicationFactor int
	Default         bool
}

// WriteRequest represents a batch write request
type WriteRequest struct {
	Database        string
	RetentionPolicy string
	Precision       string // ns, us, ms, s
	Points          []*DataPoint
}

// QueryResult holds query results
type QueryResult struct {
	Series    []*ResultSeries
	Error     error
	StatementID int
}

// ResultSeries holds a single series result
type ResultSeries struct {
	Name    string
	Tags    Tags
	Columns []string
	Values  [][]interface{}
}
