package query

import (
	"testing"
	"time"
)

var (
	testTime    = time.Now()
	fiveMinutes = 5 * time.Minute
)

func BenchmarkLexer(b *testing.B) {
	query := "SELECT mean(value), max(value), min(value) FROM cpu WHERE host = 'server01' AND time > now() - 1h GROUP BY time(5m), host ORDER BY time DESC LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer := NewLexer(query)
		for {
			tok, _ := lexer.NextToken()
			if tok == EOF || tok == ILLEGAL {
				break
			}
		}
	}
}

func BenchmarkParser(b *testing.B) {
	query := "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := NewParser(query)
		parser.Parse()
	}
}

func BenchmarkParserComplex(b *testing.B) {
	query := "SELECT mean(value), max(value), percentile(value, 95) FROM cpu WHERE host = 'server01' AND region = 'us-west' AND time > now() - 24h GROUP BY time(1h), host ORDER BY time DESC LIMIT 1000"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := NewParser(query)
		parser.Parse()
	}
}

func BenchmarkPlanner(b *testing.B) {
	query := "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)"
	parser := NewParser(query)
	stmt, _ := parser.Parse()
	selectStmt := stmt.(*SelectStatement)

	planner := NewPlanner()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		planner.Plan(selectStmt)
	}
}

func BenchmarkAggregatesMean(b *testing.B) {
	agg := NewAggregator("mean")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg.Reset()
		for j := 0; j < 1000; j++ {
			agg.Push(float64(j), TruncateTime(testTime, 0))
		}
		agg.Result()
	}
}

func BenchmarkAggregatesStddev(b *testing.B) {
	agg := NewAggregator("stddev")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg.Reset()
		for j := 0; j < 1000; j++ {
			agg.Push(float64(j), TruncateTime(testTime, 0))
		}
		agg.Result()
	}
}

func BenchmarkAggregatesPercentile(b *testing.B) {
	agg := NewPercentileAggregator(95)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg.Reset()
		for j := 0; j < 1000; j++ {
			agg.Push(float64(j), TruncateTime(testTime, 0))
		}
		agg.Result()
	}
}

func BenchmarkTimeBucketing(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TruncateTime(testTime, fiveMinutes)
	}
}

func BenchmarkExpressionEvaluation(b *testing.B) {
	// Build a moderately complex expression
	expr := &BinaryExpr{
		Left: &BinaryExpr{
			Left:  &Identifier{Name: "value"},
			Op:    GT,
			Right: &NumberLiteral{Value: 0.5},
		},
		Op: AND,
		Right: &BinaryExpr{
			Left:  &Identifier{Name: "count"},
			Op:    LT,
			Right: &NumberLiteral{Value: 100},
		},
	}

	row := []interface{}{0.75, 50.0}
	colIndex := map[string]int{"value": 0, "count": 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evaluatePredicate(expr, row, colIndex)
	}
}

func BenchmarkSeriesSort(b *testing.B) {
	// Create test series with values
	series := &Series{
		Name:    "test",
		Columns: []string{"time", "value"},
		Values:  make([][]interface{}, 1000),
	}
	for i := 0; i < 1000; i++ {
		series.Values[i] = []interface{}{int64(1000 - i), float64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy to avoid modifying original
		copied := &Series{
			Name:    series.Name,
			Columns: series.Columns,
			Values:  make([][]interface{}, len(series.Values)),
		}
		copy(copied.Values, series.Values)

		e := &Executor{}
		e.executeSort([]*Series{copied}, "time", false)
	}
}

func BenchmarkSeriesFilter(b *testing.B) {
	series := &Series{
		Name:    "test",
		Columns: []string{"time", "value"},
		Values:  make([][]interface{}, 1000),
	}
	for i := 0; i < 1000; i++ {
		series.Values[i] = []interface{}{int64(i), float64(i) / 10.0}
	}

	input := []*Series{series}
	predicate := &BinaryExpr{
		Left:  &Identifier{Name: "value"},
		Op:    GT,
		Right: &NumberLiteral{Value: 50.0},
	}

	e := &Executor{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.executeFilter(input, predicate)
	}
}

func BenchmarkSeriesLimit(b *testing.B) {
	series := &Series{
		Name:    "test",
		Columns: []string{"time", "value"},
		Values:  make([][]interface{}, 1000),
	}
	for i := 0; i < 1000; i++ {
		series.Values[i] = []interface{}{int64(i), float64(i)}
	}

	input := []*Series{series}
	e := &Executor{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.executeLimit(input, 100, 0)
	}
}
