package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

// JSONPoint represents a data point in JSON format
type JSONPoint struct {
	Measurement string                 `json:"measurement"`
	Tags        map[string]string      `json:"tags,omitempty"`
	Fields      map[string]interface{} `json:"fields"`
	Timestamp   *int64                 `json:"timestamp,omitempty"`
	Time        string                 `json:"time,omitempty"` // RFC3339 format alternative
}

// JSONBatch represents a batch of points
type JSONBatch struct {
	Database  string      `json:"database,omitempty"`
	Precision string      `json:"precision,omitempty"`
	Points    []JSONPoint `json:"points"`
}

// ParseJSON parses JSON-formatted data points
// Supports both single point and array of points
func ParseJSON(data []byte) ([]*storage.DataPoint, error) {
	// Trim whitespace to detect type
	trimmed := data
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n' || trimmed[0] == '\r') {
		trimmed = trimmed[1:]
	}

	if len(trimmed) == 0 {
		return nil, nil
	}

	// Check first character to determine type
	if trimmed[0] == '[' {
		// Array of points
		var points []JSONPoint
		if err := json.Unmarshal(data, &points); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
		return convertJSONPoints(points)
	}

	// Object - could be single point or batch
	if trimmed[0] == '{' {
		// Try as batch object first (has "points" field)
		var batch JSONBatch
		if err := json.Unmarshal(data, &batch); err == nil && len(batch.Points) > 0 {
			return convertJSONPoints(batch.Points)
		}

		// Try as single point
		var point JSONPoint
		if err := json.Unmarshal(data, &point); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %w", err)
		}
		return convertJSONPoints([]JSONPoint{point})
	}

	return nil, fmt.Errorf("invalid JSON: expected array or object")
}

// convertJSONPoints converts JSONPoints to storage DataPoints
func convertJSONPoints(jsonPoints []JSONPoint) ([]*storage.DataPoint, error) {
	points := make([]*storage.DataPoint, 0, len(jsonPoints))

	for i, jp := range jsonPoints {
		if jp.Measurement == "" {
			return nil, fmt.Errorf("point %d: measurement is required", i)
		}
		if len(jp.Fields) == 0 {
			return nil, fmt.Errorf("point %d: at least one field is required", i)
		}

		// Determine timestamp
		var timestamp int64
		if jp.Timestamp != nil {
			timestamp = *jp.Timestamp
		} else if jp.Time != "" {
			t, err := time.Parse(time.RFC3339Nano, jp.Time)
			if err != nil {
				// Try RFC3339
				t, err = time.Parse(time.RFC3339, jp.Time)
				if err != nil {
					return nil, fmt.Errorf("point %d: invalid time format: %w", i, err)
				}
			}
			timestamp = t.UnixNano()
		} else {
			timestamp = time.Now().UnixNano()
		}

		// Convert fields
		fields := make(storage.Fields, len(jp.Fields))
		for k, v := range jp.Fields {
			fv, err := convertFieldValue(v)
			if err != nil {
				return nil, fmt.Errorf("point %d, field %q: %w", i, k, err)
			}
			fields[k] = fv
		}

		// Convert tags
		tags := make(storage.Tags, len(jp.Tags))
		for k, v := range jp.Tags {
			tags[k] = v
		}

		points = append(points, &storage.DataPoint{
			Measurement: jp.Measurement,
			Tags:        tags,
			Fields:      fields,
			Timestamp:   timestamp,
		})
	}

	return points, nil
}

// convertFieldValue converts a JSON value to a FieldValue
func convertFieldValue(v interface{}) (storage.FieldValue, error) {
	switch val := v.(type) {
	case float64:
		// JSON numbers are float64 by default
		// Check if it's actually an integer
		if val == float64(int64(val)) && val >= -9007199254740992 && val <= 9007199254740992 {
			// Could be either, default to float
			return storage.NewFloatField(val), nil
		}
		return storage.NewFloatField(val), nil
	case int64:
		return storage.NewIntField(val), nil
	case int:
		return storage.NewIntField(int64(val)), nil
	case string:
		return storage.NewStringField(val), nil
	case bool:
		return storage.NewBoolField(val), nil
	case nil:
		return storage.FieldValue{}, fmt.Errorf("null values not supported")
	default:
		return storage.FieldValue{}, fmt.Errorf("unsupported type: %T", v)
	}
}

// ToJSON converts DataPoints to JSON format
func ToJSON(points []*storage.DataPoint) ([]byte, error) {
	jsonPoints := make([]JSONPoint, len(points))

	for i, p := range points {
		jp := JSONPoint{
			Measurement: p.Measurement,
			Tags:        p.Tags,
			Timestamp:   &p.Timestamp,
			Fields:      make(map[string]interface{}),
		}

		for k, v := range p.Fields {
			switch v.Type {
			case storage.FieldTypeFloat:
				jp.Fields[k] = v.FloatValue
			case storage.FieldTypeInteger:
				jp.Fields[k] = v.IntValue
			case storage.FieldTypeString:
				jp.Fields[k] = v.StringValue
			case storage.FieldTypeBoolean:
				jp.Fields[k] = v.BooleanValue
			}
		}

		jsonPoints[i] = jp
	}

	return json.Marshal(jsonPoints)
}
