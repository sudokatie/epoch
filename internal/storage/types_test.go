package storage

import (
	"testing"
	"time"
)

func TestFieldTypeString(t *testing.T) {
	tests := []struct {
		ft   FieldType
		want string
	}{
		{FieldTypeFloat, "float"},
		{FieldTypeInteger, "integer"},
		{FieldTypeString, "string"},
		{FieldTypeBoolean, "boolean"},
		{FieldType(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.ft.String(); got != tt.want {
			t.Errorf("FieldType(%d).String() = %q, want %q", tt.ft, got, tt.want)
		}
	}
}

func TestFieldValues(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		fv := NewFloatField(3.14)
		if fv.Type != FieldTypeFloat {
			t.Errorf("Type = %v, want Float", fv.Type)
		}
		if fv.FloatValue != 3.14 {
			t.Errorf("FloatValue = %v, want 3.14", fv.FloatValue)
		}
	})

	t.Run("integer", func(t *testing.T) {
		fv := NewIntField(42)
		if fv.Type != FieldTypeInteger {
			t.Errorf("Type = %v, want Integer", fv.Type)
		}
		if fv.IntValue != 42 {
			t.Errorf("IntValue = %v, want 42", fv.IntValue)
		}
	})

	t.Run("string", func(t *testing.T) {
		fv := NewStringField("hello")
		if fv.Type != FieldTypeString {
			t.Errorf("Type = %v, want String", fv.Type)
		}
		if fv.StringValue != "hello" {
			t.Errorf("StringValue = %q, want hello", fv.StringValue)
		}
	})

	t.Run("boolean", func(t *testing.T) {
		fv := NewBoolField(true)
		if fv.Type != FieldTypeBoolean {
			t.Errorf("Type = %v, want Boolean", fv.Type)
		}
		if fv.BooleanValue != true {
			t.Errorf("BooleanValue = %v, want true", fv.BooleanValue)
		}
	})
}

func TestTagsString(t *testing.T) {
	tests := []struct {
		name string
		tags Tags
		want string
	}{
		{"empty", Tags{}, ""},
		{"single", Tags{"host": "server1"}, "host=server1"},
		{"multiple sorted", Tags{"region": "us-west", "host": "server1"}, "host=server1,region=us-west"},
		{"three tags", Tags{"c": "3", "a": "1", "b": "2"}, "a=1,b=2,c=3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tags.String(); got != tt.want {
				t.Errorf("Tags.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDataPointValidate(t *testing.T) {
	now := time.Now().UnixNano()

	tests := []struct {
		name    string
		dp      *DataPoint
		wantErr bool
	}{
		{
			name: "valid",
			dp: &DataPoint{
				Measurement: "cpu",
				Tags:        Tags{"host": "server1"},
				Fields:      Fields{"usage": NewFloatField(45.2)},
				Timestamp:   now,
			},
			wantErr: false,
		},
		{
			name: "missing measurement",
			dp: &DataPoint{
				Measurement: "",
				Fields:      Fields{"usage": NewFloatField(45.2)},
				Timestamp:   now,
			},
			wantErr: true,
		},
		{
			name: "no fields",
			dp: &DataPoint{
				Measurement: "cpu",
				Fields:      Fields{},
				Timestamp:   now,
			},
			wantErr: true,
		},
		{
			name: "zero timestamp",
			dp: &DataPoint{
				Measurement: "cpu",
				Fields:      Fields{"usage": NewFloatField(45.2)},
				Timestamp:   0,
			},
			wantErr: true,
		},
		{
			name: "negative timestamp",
			dp: &DataPoint{
				Measurement: "cpu",
				Fields:      Fields{"usage": NewFloatField(45.2)},
				Timestamp:   -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.dp.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataPointSeriesKey(t *testing.T) {
	tests := []struct {
		name string
		dp   *DataPoint
		want string
	}{
		{
			name: "no tags",
			dp: &DataPoint{
				Measurement: "cpu",
				Fields:      Fields{"usage": NewFloatField(45.2)},
			},
			want: "cpu",
		},
		{
			name: "with tags",
			dp: &DataPoint{
				Measurement: "cpu",
				Tags:        Tags{"host": "server1", "region": "us-west"},
				Fields:      Fields{"usage": NewFloatField(45.2)},
			},
			want: "cpu,host=server1,region=us-west",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dp.SeriesKey(); got != tt.want {
				t.Errorf("SeriesKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSeriesKey(t *testing.T) {
	s := &Series{
		ID:          1,
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
	}
	want := "cpu,host=server1"
	if got := s.Key(); got != want {
		t.Errorf("Series.Key() = %q, want %q", got, want)
	}
}

func TestShardInfoContains(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	si := &ShardInfo{
		StartTime: start,
		EndTime:   end,
	}

	tests := []struct {
		name string
		ts   int64
		want bool
	}{
		{"before", start.Add(-time.Hour).UnixNano(), false},
		{"at start", start.UnixNano(), true},
		{"middle", start.Add(12 * time.Hour).UnixNano(), true},
		{"at end", end.UnixNano(), false},
		{"after", end.Add(time.Hour).UnixNano(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := si.Contains(tt.ts); got != tt.want {
				t.Errorf("Contains(%d) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}
