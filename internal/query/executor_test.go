package query

import (
	"context"
	"testing"
	"time"
)

func TestExecutorConfig(t *testing.T) {
	config := DefaultExecutorConfig()

	if config.QueryTimeout != 30*time.Second {
		t.Errorf("expected 30s timeout, got %v", config.QueryTimeout)
	}

	if config.MaxSeriesPerQuery != 10000 {
		t.Errorf("expected 10000 max series, got %d", config.MaxSeriesPerQuery)
	}

	if config.MaxPointsPerQuery != 1000000 {
		t.Errorf("expected 1000000 max points, got %d", config.MaxPointsPerQuery)
	}
}

func TestExecuteFilter(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{"host": "server1"},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{int64(1000), 50.0},
			{int64(2000), 80.0},
			{int64(3000), 30.0},
		},
	}}

	// Filter: usage > 40
	predicate := &BinaryExpr{
		Left:  &Identifier{Name: "usage"},
		Op:    GT,
		Right: &NumberLiteral{Value: 40},
	}

	result := e.executeFilter(series, predicate)

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	if len(result[0].Values) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result[0].Values))
	}
}

func TestExecuteGroup(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{"host": "server1"},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{int64(1000), 10.0},
			{int64(2000), 20.0},
			{int64(3000), 30.0},
		},
	}}

	group := &GroupNode{
		Aggregates: []*AggregateExpr{
			{Func: "mean", Field: "usage", Alias: "mean_usage"},
			{Func: "count", Field: "*", Alias: "count"},
		},
	}

	result := e.executeGroup(series, group)

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	// Should have 1 bucket with aggregated values
	if len(result[0].Values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result[0].Values))
	}

	// Check mean
	mean := result[0].Values[0][1].(float64)
	if mean != 20.0 {
		t.Errorf("expected mean 20.0, got %v", mean)
	}

	// Check count
	count := result[0].Values[0][2].(float64)
	if count != 3.0 {
		t.Errorf("expected count 3.0, got %v", count)
	}
}

func TestExecuteGroupByTime(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{base.UnixNano(), 10.0},
			{base.Add(30 * time.Minute).UnixNano(), 20.0},
			{base.Add(90 * time.Minute).UnixNano(), 30.0},
			{base.Add(120 * time.Minute).UnixNano(), 40.0},
		},
	}}

	group := &GroupNode{
		Interval: time.Hour,
		Aggregates: []*AggregateExpr{
			{Func: "mean", Field: "usage", Alias: "mean"},
		},
	}

	result := e.executeGroup(series, group)

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	// Should have 3 hourly buckets: 00:00, 01:00, 02:00
	if len(result[0].Values) != 3 {
		t.Errorf("expected 3 buckets, got %d", len(result[0].Values))
	}
}

func TestExecuteProject(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "usage", "temp", "load"},
		Values: [][]interface{}{
			{int64(1000), 50.0, 65.0, 1.5},
			{int64(2000), 60.0, 70.0, 2.0},
		},
	}}

	result := e.executeProject(series, []string{"time", "usage"})

	if len(result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result))
	}

	if len(result[0].Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result[0].Columns))
	}

	if result[0].Columns[0] != "time" || result[0].Columns[1] != "usage" {
		t.Errorf("unexpected columns: %v", result[0].Columns)
	}

	// Each row should have 2 values
	for _, row := range result[0].Values {
		if len(row) != 2 {
			t.Errorf("expected 2 values per row, got %d", len(row))
		}
	}
}

func TestExecuteSort(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{int64(1000), 50.0},
			{int64(2000), 30.0},
			{int64(3000), 80.0},
		},
	}}

	// Sort ascending
	result := e.executeSort(series, "usage", false)
	values := result[0].Values
	if values[0][1].(float64) != 30.0 || values[1][1].(float64) != 50.0 || values[2][1].(float64) != 80.0 {
		t.Error("ascending sort failed")
	}

	// Sort descending
	result = e.executeSort(series, "usage", true)
	values = result[0].Values
	if values[0][1].(float64) != 80.0 || values[1][1].(float64) != 50.0 || values[2][1].(float64) != 30.0 {
		t.Error("descending sort failed")
	}
}

func TestExecuteLimit(t *testing.T) {
	e := &Executor{config: DefaultExecutorConfig()}

	series := []*Series{{
		Name:    "cpu",
		Tags:    map[string]string{},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{int64(1000), 10.0},
			{int64(2000), 20.0},
			{int64(3000), 30.0},
			{int64(4000), 40.0},
			{int64(5000), 50.0},
		},
	}}

	// LIMIT 2
	result := e.executeLimit(series, 2, 0)
	if len(result[0].Values) != 2 {
		t.Errorf("expected 2 rows with LIMIT 2, got %d", len(result[0].Values))
	}

	// LIMIT 2 OFFSET 2
	result = e.executeLimit(series, 2, 2)
	if len(result[0].Values) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result[0].Values))
	}
	if result[0].Values[0][1].(float64) != 30.0 {
		t.Error("OFFSET not applied correctly")
	}

	// OFFSET beyond data
	result = e.executeLimit(series, 10, 100)
	if len(result[0].Values) != 0 {
		t.Errorf("expected 0 rows with high offset, got %d", len(result[0].Values))
	}
}

func TestEvaluatePredicate(t *testing.T) {
	row := []interface{}{int64(1000), 50.0, "server1"}
	colIndex := map[string]int{"time": 0, "usage": 1, "host": 2}

	tests := []struct {
		name     string
		pred     Expr
		expected bool
	}{
		{
			name: "greater than true",
			pred: &BinaryExpr{
				Left:  &Identifier{Name: "usage"},
				Op:    GT,
				Right: &NumberLiteral{Value: 40},
			},
			expected: true,
		},
		{
			name: "greater than false",
			pred: &BinaryExpr{
				Left:  &Identifier{Name: "usage"},
				Op:    GT,
				Right: &NumberLiteral{Value: 60},
			},
			expected: false,
		},
		{
			name: "equality true",
			pred: &BinaryExpr{
				Left:  &Identifier{Name: "usage"},
				Op:    EQ,
				Right: &NumberLiteral{Value: 50},
			},
			expected: true,
		},
		{
			name: "AND condition",
			pred: &BinaryExpr{
				Left: &BinaryExpr{
					Left:  &Identifier{Name: "usage"},
					Op:    GT,
					Right: &NumberLiteral{Value: 40},
				},
				Op: AND,
				Right: &BinaryExpr{
					Left:  &Identifier{Name: "usage"},
					Op:    LT,
					Right: &NumberLiteral{Value: 60},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evaluatePredicate(tt.pred, row, colIndex)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		left, right interface{}
		expected    int
	}{
		{10.0, 20.0, -1},
		{20.0, 10.0, 1},
		{10.0, 10.0, 0},
		{int64(10), float64(10), 0},
		{int64(10), int64(20), -1},
	}

	for _, tt := range tests {
		result := compareValues(tt.left, tt.right)
		if result != tt.expected {
			t.Errorf("compareValues(%v, %v) = %d, want %d", tt.left, tt.right, result, tt.expected)
		}
	}
}

func TestGetFloat(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
	}{
		{10.5, 10.5},
		{int64(42), 42.0},
		{int(100), 100.0},
		{float32(3.14), 3.14},
	}

	for _, tt := range tests {
		result := getFloat(tt.input)
		// Use approximate comparison for float32 conversion
		diff := result - tt.expected
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Errorf("getFloat(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestGetTimestamp(t *testing.T) {
	now := time.Now()

	tests := []struct {
		input    interface{}
		expected int64
		ok       bool
	}{
		{int64(1000), 1000, true},
		{int(2000), 2000, true},
		{float64(3000), 3000, true},
		{now, now.UnixNano(), true},
		{"invalid", 0, false},
	}

	for _, tt := range tests {
		result, ok := getTimestamp(tt.input)
		if ok != tt.ok {
			t.Errorf("getTimestamp(%v) ok = %v, want %v", tt.input, ok, tt.ok)
		}
		if ok && result != tt.expected {
			t.Errorf("getTimestamp(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestCountPoints(t *testing.T) {
	series := []*Series{
		{Values: make([][]interface{}, 10)},
		{Values: make([][]interface{}, 20)},
		{Values: make([][]interface{}, 5)},
	}

	count := countPoints(series)
	if count != 35 {
		t.Errorf("expected 35 points, got %d", count)
	}
}

func TestExecutionStats(t *testing.T) {
	stats := &ExecutionStats{
		Duration:       100 * time.Millisecond,
		SeriesScanned:  5,
		PointsScanned:  1000,
		PointsReturned: 100,
	}

	if stats.Duration != 100*time.Millisecond {
		t.Error("duration not set correctly")
	}

	if stats.SeriesScanned != 5 {
		t.Error("series scanned not set correctly")
	}
}

func TestExecutorTimeout(t *testing.T) {
	e := &Executor{config: ExecutorConfig{QueryTimeout: 1 * time.Nanosecond}}

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := e.executePlan(ctx, "test", &ScanNode{Measurement: "test"})
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestSeriesResult(t *testing.T) {
	series := &Series{
		Name:    "cpu",
		Tags:    map[string]string{"host": "server1"},
		Columns: []string{"time", "usage"},
		Values: [][]interface{}{
			{int64(1000), 50.0},
		},
	}

	if series.Name != "cpu" {
		t.Error("name not set")
	}

	if series.Tags["host"] != "server1" {
		t.Error("tags not set")
	}

	if len(series.Columns) != 2 {
		t.Error("columns not set")
	}

	if len(series.Values) != 1 {
		t.Error("values not set")
	}
}
