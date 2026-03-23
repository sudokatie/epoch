package query

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
)

// Executor executes query plans against the storage engine
type Executor struct {
	engine *storage.Engine
	config ExecutorConfig
}

// ExecutorConfig holds executor configuration
type ExecutorConfig struct {
	// QueryTimeout is the maximum time a query can run
	QueryTimeout time.Duration
	// MaxSeriesPerQuery limits the number of series returned
	MaxSeriesPerQuery int
	// MaxPointsPerQuery limits the total number of points
	MaxPointsPerQuery int64
}

// DefaultExecutorConfig returns sensible defaults
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		QueryTimeout:      30 * time.Second,
		MaxSeriesPerQuery: 10000,
		MaxPointsPerQuery: 1000000,
	}
}

// NewExecutor creates a new query executor
func NewExecutor(engine *storage.Engine, config ExecutorConfig) *Executor {
	return &Executor{
		engine: engine,
		config: config,
	}
}

// Result holds query execution results
type Result struct {
	// Series contains the result data
	Series []*Series
	// Messages contains any warnings or info
	Messages []string
	// Stats contains execution statistics
	Stats *ExecutionStats
}

// Series represents a single result series
type Series struct {
	Name    string
	Tags    map[string]string
	Columns []string
	Values  [][]interface{}
}

// ExecutionStats tracks query execution metrics
type ExecutionStats struct {
	// Duration is total execution time
	Duration time.Duration
	// SeriesScanned is the number of series examined
	SeriesScanned int
	// PointsScanned is the number of points examined
	PointsScanned int64
	// PointsReturned is the number of points in result
	PointsReturned int64
}

// Execute executes a query plan and returns results
func (e *Executor) Execute(ctx context.Context, database string, plan Plan) (*Result, error) {
	start := time.Now()

	// Create timeout context if not already set
	if _, ok := ctx.Deadline(); !ok && e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout)
		defer cancel()
	}

	// Execute the plan
	result, stats, err := e.executePlan(ctx, database, plan)
	if err != nil {
		return nil, err
	}

	stats.Duration = time.Since(start)

	return &Result{
		Series: result,
		Stats:  stats,
	}, nil
}

// executePlan recursively executes a plan node
func (e *Executor) executePlan(ctx context.Context, database string, plan Plan) ([]*Series, *ExecutionStats, error) {
	// Check for timeout
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	stats := &ExecutionStats{}

	switch p := plan.(type) {
	case *ScanNode:
		return e.executeScan(ctx, database, p, stats)

	case *FilterNode:
		series, childStats, err := e.executePlan(ctx, database, p.Input)
		if err != nil {
			return nil, nil, err
		}
		stats.SeriesScanned = childStats.SeriesScanned
		stats.PointsScanned = childStats.PointsScanned

		filtered := e.executeFilter(series, p.Predicate)
		stats.PointsReturned = countPoints(filtered)
		return filtered, stats, nil

	case *GroupNode:
		series, childStats, err := e.executePlan(ctx, database, p.Input)
		if err != nil {
			return nil, nil, err
		}
		stats.SeriesScanned = childStats.SeriesScanned
		stats.PointsScanned = childStats.PointsScanned

		grouped := e.executeGroup(series, p)
		stats.PointsReturned = countPoints(grouped)
		return grouped, stats, nil

	case *ProjectNode:
		series, childStats, err := e.executePlan(ctx, database, p.Input)
		if err != nil {
			return nil, nil, err
		}
		stats.SeriesScanned = childStats.SeriesScanned
		stats.PointsScanned = childStats.PointsScanned

		projected := e.executeProject(series, p.Fields)
		stats.PointsReturned = countPoints(projected)
		return projected, stats, nil

	case *SortNode:
		series, childStats, err := e.executePlan(ctx, database, p.Input)
		if err != nil {
			return nil, nil, err
		}
		stats.SeriesScanned = childStats.SeriesScanned
		stats.PointsScanned = childStats.PointsScanned

		sorted := e.executeSort(series, p.Field, p.Desc)
		stats.PointsReturned = countPoints(sorted)
		return sorted, stats, nil

	case *LimitNode:
		series, childStats, err := e.executePlan(ctx, database, p.Input)
		if err != nil {
			return nil, nil, err
		}
		stats.SeriesScanned = childStats.SeriesScanned
		stats.PointsScanned = childStats.PointsScanned

		limited := e.executeLimit(series, p.Limit, p.Offset)
		stats.PointsReturned = countPoints(limited)
		return limited, stats, nil

	default:
		return nil, nil, fmt.Errorf("unsupported plan node: %T", plan)
	}
}

// executeScan reads data from the storage engine
func (e *Executor) executeScan(ctx context.Context, database string, scan *ScanNode, stats *ExecutionStats) ([]*Series, *ExecutionStats, error) {
	// Convert time range to nanoseconds
	var minTime, maxTime int64
	if !scan.TimeRange.Start.IsZero() {
		minTime = scan.TimeRange.Start.UnixNano()
	}
	if !scan.TimeRange.End.IsZero() {
		maxTime = scan.TimeRange.End.UnixNano()
	} else {
		maxTime = time.Now().UnixNano()
	}

	// Query the storage engine
	// Note: We request all fields with empty field list for now
	result, err := e.engine.Query(database, scan.Measurement, scan.TagFilters, minTime, maxTime, nil)
	if err != nil {
		return nil, stats, err
	}

	// Convert to executor result format
	series := make([]*Series, 0, len(result.Series))
	for _, rs := range result.Series {
		s := &Series{
			Name:    rs.Name,
			Tags:    rs.Tags,
			Columns: rs.Columns,
			Values:  rs.Values,
		}
		series = append(series, s)
		stats.SeriesScanned++
		stats.PointsScanned += int64(len(rs.Values))
	}

	// Check limits
	if e.config.MaxSeriesPerQuery > 0 && len(series) > e.config.MaxSeriesPerQuery {
		return nil, stats, fmt.Errorf("query returned too many series: %d > %d", len(series), e.config.MaxSeriesPerQuery)
	}

	stats.PointsReturned = stats.PointsScanned
	return series, stats, nil
}

// executeFilter applies a predicate to filter rows
func (e *Executor) executeFilter(series []*Series, predicate Expr) []*Series {
	result := make([]*Series, 0, len(series))

	for _, s := range series {
		filtered := &Series{
			Name:    s.Name,
			Tags:    s.Tags,
			Columns: s.Columns,
			Values:  make([][]interface{}, 0),
		}

		// Build column index map
		colIndex := make(map[string]int)
		for i, col := range s.Columns {
			colIndex[col] = i
		}

		// Filter each row
		for _, row := range s.Values {
			if evaluatePredicate(predicate, row, colIndex) {
				filtered.Values = append(filtered.Values, row)
			}
		}

		if len(filtered.Values) > 0 {
			result = append(result, filtered)
		}
	}

	return result
}

// executeGroup performs GROUP BY aggregation
func (e *Executor) executeGroup(series []*Series, group *GroupNode) []*Series {
	if len(series) == 0 || len(group.Aggregates) == 0 {
		return series
	}

	result := make([]*Series, 0)

	for _, s := range series {
		// Build column index map
		colIndex := make(map[string]int)
		for i, col := range s.Columns {
			colIndex[col] = i
		}

		timeIdx, hasTime := colIndex["time"]

		// Build buckets
		buckets := make(map[string]*aggregateBucket)

		for _, row := range s.Values {
			// Determine bucket key
			var bucketKey string
			if group.Interval > 0 && hasTime {
				ts, ok := getTimestamp(row[timeIdx])
				if ok {
					bucketStart := TruncateTime(time.Unix(0, ts), group.Interval)
					bucketKey = bucketStart.Format(time.RFC3339Nano)
				}
			}

			// Add tag values to bucket key
			for _, tag := range group.Tags {
				if v, ok := s.Tags[tag]; ok {
					bucketKey += "|" + tag + "=" + v
				}
			}

			// Get or create bucket
			bucket, exists := buckets[bucketKey]
			if !exists {
				bucket = &aggregateBucket{
					timestamp:   time.Time{},
					aggregators: make([]Aggregator, len(group.Aggregates)),
				}
				for i, agg := range group.Aggregates {
					bucket.aggregators[i] = NewAggregator(agg.Func)
				}
				if group.Interval > 0 && hasTime {
					ts, ok := getTimestamp(row[timeIdx])
					if ok {
						bucket.timestamp = TruncateTime(time.Unix(0, ts), group.Interval)
					}
				}
				buckets[bucketKey] = bucket
			}

			// Push values to aggregators
			for i, agg := range group.Aggregates {
				if agg.Field == "*" {
					// COUNT(*) counts all rows
					bucket.aggregators[i].Push(1.0, time.Time{})
				} else if idx, ok := colIndex[agg.Field]; ok {
					val := getFloat(row[idx])
					ts := time.Time{}
					if hasTime {
						if t, ok := getTimestamp(row[timeIdx]); ok {
							ts = time.Unix(0, t)
						}
					}
					bucket.aggregators[i].Push(val, ts)
				}
			}
		}

		// Build result series
		if len(buckets) > 0 {
			grouped := &Series{
				Name:    s.Name,
				Tags:    s.Tags,
				Columns: []string{"time"},
				Values:  make([][]interface{}, 0, len(buckets)),
			}

			// Add aggregate columns
			for _, agg := range group.Aggregates {
				grouped.Columns = append(grouped.Columns, agg.Alias)
			}

			// Sort buckets by time
			keys := make([]string, 0, len(buckets))
			for k := range buckets {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			// Build result rows
			for _, key := range keys {
				bucket := buckets[key]
				row := make([]interface{}, len(grouped.Columns))
				row[0] = bucket.timestamp.UnixNano()

				for i, agg := range bucket.aggregators {
					row[i+1] = agg.Result()
				}
				grouped.Values = append(grouped.Values, row)
			}

			result = append(result, grouped)
		}
	}

	return result
}

type aggregateBucket struct {
	timestamp   time.Time
	aggregators []Aggregator
}

// executeProject selects specific columns
func (e *Executor) executeProject(series []*Series, fields []string) []*Series {
	if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
		return series
	}

	result := make([]*Series, 0, len(series))

	for _, s := range series {
		// Build column index map
		colIndex := make(map[string]int)
		for i, col := range s.Columns {
			colIndex[col] = i
		}

		// Build projected series
		projected := &Series{
			Name:    s.Name,
			Tags:    s.Tags,
			Columns: make([]string, 0, len(fields)),
			Values:  make([][]interface{}, 0, len(s.Values)),
		}

		// Find indices for requested fields
		indices := make([]int, 0, len(fields))
		for _, field := range fields {
			if idx, ok := colIndex[field]; ok {
				indices = append(indices, idx)
				projected.Columns = append(projected.Columns, field)
			}
		}

		// Project each row
		for _, row := range s.Values {
			newRow := make([]interface{}, len(indices))
			for i, idx := range indices {
				newRow[i] = row[idx]
			}
			projected.Values = append(projected.Values, newRow)
		}

		result = append(result, projected)
	}

	return result
}

// executeSort sorts results by a field
func (e *Executor) executeSort(series []*Series, field string, desc bool) []*Series {
	result := make([]*Series, 0, len(series))

	for _, s := range series {
		// Find field index
		fieldIdx := -1
		for i, col := range s.Columns {
			if col == field {
				fieldIdx = i
				break
			}
		}

		if fieldIdx < 0 {
			result = append(result, s)
			continue
		}

		// Sort values
		sorted := &Series{
			Name:    s.Name,
			Tags:    s.Tags,
			Columns: s.Columns,
			Values:  make([][]interface{}, len(s.Values)),
		}
		copy(sorted.Values, s.Values)

		sort.Slice(sorted.Values, func(i, j int) bool {
			vi := getFloat(sorted.Values[i][fieldIdx])
			vj := getFloat(sorted.Values[j][fieldIdx])
			if desc {
				return vi > vj
			}
			return vi < vj
		})

		result = append(result, sorted)
	}

	return result
}

// executeLimit applies LIMIT and OFFSET
func (e *Executor) executeLimit(series []*Series, limit, offset int) []*Series {
	result := make([]*Series, 0, len(series))

	for _, s := range series {
		limited := &Series{
			Name:    s.Name,
			Tags:    s.Tags,
			Columns: s.Columns,
		}

		start := offset
		if start > len(s.Values) {
			start = len(s.Values)
		}

		end := start + limit
		if end > len(s.Values) {
			end = len(s.Values)
		}

		if start < end {
			limited.Values = s.Values[start:end]
		}

		result = append(result, limited)
	}

	return result
}

// Helper functions

func countPoints(series []*Series) int64 {
	var count int64
	for _, s := range series {
		count += int64(len(s.Values))
	}
	return count
}

func evaluatePredicate(predicate Expr, row []interface{}, colIndex map[string]int) bool {
	val := evaluateExpr(predicate, row, colIndex)
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}

func evaluateExpr(expr Expr, row []interface{}, colIndex map[string]int) interface{} {
	switch e := expr.(type) {
	case *Identifier:
		if idx, ok := colIndex[e.Name]; ok && idx < len(row) {
			return row[idx]
		}
		return nil

	case *NumberLiteral:
		return e.Value

	case *StringLiteral:
		return e.Value

	case *BooleanLiteral:
		return e.Value

	case *BinaryExpr:
		left := evaluateExpr(e.Left, row, colIndex)
		right := evaluateExpr(e.Right, row, colIndex)
		return evaluateBinaryOp(e.Op, left, right)

	case *ParenExpr:
		return evaluateExpr(e.Expr, row, colIndex)

	default:
		return nil
	}
}

func evaluateBinaryOp(op Token, left, right interface{}) interface{} {
	switch op {
	case EQ:
		return compareValues(left, right) == 0
	case NEQ:
		return compareValues(left, right) != 0
	case LT:
		return compareValues(left, right) < 0
	case LTE:
		return compareValues(left, right) <= 0
	case GT:
		return compareValues(left, right) > 0
	case GTE:
		return compareValues(left, right) >= 0
	case AND:
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if lok && rok {
			return lb && rb
		}
		return false
	case OR:
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if lok && rok {
			return lb || rb
		}
		return false
	default:
		return nil
	}
}

func compareValues(left, right interface{}) int {
	lf := getFloat(left)
	rf := getFloat(right)

	if math.IsNaN(lf) && math.IsNaN(rf) {
		return 0
	}
	if math.IsNaN(lf) {
		return -1
	}
	if math.IsNaN(rf) {
		return 1
	}

	if lf < rf {
		return -1
	}
	if lf > rf {
		return 1
	}
	return 0
}

func getFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	default:
		return math.NaN()
	}
}

func getTimestamp(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	case time.Time:
		return val.UnixNano(), true
	default:
		return 0, false
	}
}
