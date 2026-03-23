package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ContinuousQuery defines a continuous query for downsampling
type ContinuousQuery struct {
	// Name is the unique identifier
	Name string `json:"name"`
	// Database is the source database
	Database string `json:"database"`
	// SourceMeasurement is the measurement to read from
	SourceMeasurement string `json:"source_measurement"`
	// DestMeasurement is the measurement to write to
	DestMeasurement string `json:"dest_measurement"`
	// DestRetentionPolicy is the destination retention policy (optional)
	DestRetentionPolicy string `json:"dest_retention_policy,omitempty"`
	// Query is the aggregation query (simplified)
	Query string `json:"query"`
	// Interval is how often the CQ runs
	Interval time.Duration `json:"interval"`
	// GroupByInterval is the time bucket size for aggregation
	GroupByInterval time.Duration `json:"group_by_interval"`
	// AggregateFunc is the aggregation function (mean, sum, count, etc.)
	AggregateFunc string `json:"aggregate_func"`
	// SourceField is the field to aggregate
	SourceField string `json:"source_field"`
	// Enabled indicates if the CQ is active
	Enabled bool `json:"enabled"`
	// LastRun is the last time the CQ was executed
	LastRun time.Time `json:"last_run"`
	// LastProcessedTime is the latest timestamp that was processed
	LastProcessedTime time.Time `json:"last_processed_time"`
}

// DownsampleManager manages continuous queries
type DownsampleManager struct {
	mu sync.RWMutex

	// Path for persistence
	path string

	// Continuous queries by name
	queries map[string]*ContinuousQuery

	// Engine reference for executing queries
	engine *Engine

	// Background execution
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// DownsampleConfig holds configuration for the downsample manager
type DownsampleConfig struct {
	Path     string
	Interval time.Duration
}

// NewDownsampleManager creates a new downsample manager
func NewDownsampleManager(config DownsampleConfig, engine *Engine) (*DownsampleManager, error) {
	dm := &DownsampleManager{
		path:     config.Path,
		queries:  make(map[string]*ContinuousQuery),
		engine:   engine,
		interval: config.Interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	if dm.interval == 0 {
		dm.interval = time.Minute
	}

	// Load existing CQs
	if err := dm.load(); err != nil {
		return nil, err
	}

	return dm, nil
}

// load loads CQs from disk
func (dm *DownsampleManager) load() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	cqPath := filepath.Join(dm.path, "continuous_queries.json")

	data, err := os.ReadFile(cqPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read continuous queries: %w", err)
	}

	var queries []*ContinuousQuery
	if err := json.Unmarshal(data, &queries); err != nil {
		return fmt.Errorf("parse continuous queries: %w", err)
	}

	for _, cq := range queries {
		dm.queries[cq.Name] = cq
	}

	return nil
}

// save saves CQs to disk
func (dm *DownsampleManager) save() error {
	if err := os.MkdirAll(dm.path, 0755); err != nil {
		return fmt.Errorf("create downsample dir: %w", err)
	}

	queries := make([]*ContinuousQuery, 0, len(dm.queries))
	for _, cq := range dm.queries {
		queries = append(queries, cq)
	}

	data, err := json.MarshalIndent(queries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal continuous queries: %w", err)
	}

	cqPath := filepath.Join(dm.path, "continuous_queries.json")
	if err := os.WriteFile(cqPath, data, 0644); err != nil {
		return fmt.Errorf("write continuous queries: %w", err)
	}

	return nil
}

// CreateCQ creates a new continuous query
func (dm *DownsampleManager) CreateCQ(cq *ContinuousQuery) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, exists := dm.queries[cq.Name]; exists {
		return fmt.Errorf("continuous query %q already exists", cq.Name)
	}

	if err := validateCQ(cq); err != nil {
		return err
	}

	cq.Enabled = true
	cq.LastRun = time.Time{}
	cq.LastProcessedTime = time.Time{}

	dm.queries[cq.Name] = cq
	return dm.save()
}

// DropCQ removes a continuous query
func (dm *DownsampleManager) DropCQ(name string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, exists := dm.queries[name]; !exists {
		return fmt.Errorf("continuous query %q not found", name)
	}

	delete(dm.queries, name)
	return dm.save()
}

// EnableCQ enables a continuous query
func (dm *DownsampleManager) EnableCQ(name string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	cq, exists := dm.queries[name]
	if !exists {
		return fmt.Errorf("continuous query %q not found", name)
	}

	cq.Enabled = true
	return dm.save()
}

// DisableCQ disables a continuous query
func (dm *DownsampleManager) DisableCQ(name string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	cq, exists := dm.queries[name]
	if !exists {
		return fmt.Errorf("continuous query %q not found", name)
	}

	cq.Enabled = false
	return dm.save()
}

// GetCQ returns a continuous query by name
func (dm *DownsampleManager) GetCQ(name string) (*ContinuousQuery, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	cq, exists := dm.queries[name]
	if !exists {
		return nil, false
	}

	// Return a copy
	copy := *cq
	return &copy, true
}

// ListCQs returns all continuous queries
func (dm *DownsampleManager) ListCQs() []*ContinuousQuery {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	queries := make([]*ContinuousQuery, 0, len(dm.queries))
	for _, cq := range dm.queries {
		copy := *cq
		queries = append(queries, &copy)
	}
	return queries
}

// ListCQsForDatabase returns CQs for a specific database
func (dm *DownsampleManager) ListCQsForDatabase(database string) []*ContinuousQuery {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	queries := make([]*ContinuousQuery, 0)
	for _, cq := range dm.queries {
		if cq.Database == database {
			copy := *cq
			queries = append(queries, &copy)
		}
	}
	return queries
}

// Start starts the background CQ executor
func (dm *DownsampleManager) Start() {
	go dm.runLoop()
}

// Stop stops the background CQ executor
func (dm *DownsampleManager) Stop() {
	close(dm.stopCh)
	<-dm.doneCh
}

// runLoop runs CQs periodically
func (dm *DownsampleManager) runLoop() {
	defer close(dm.doneCh)

	ticker := time.NewTicker(dm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dm.RunAll(context.Background())
		case <-dm.stopCh:
			return
		}
	}
}

// RunAll executes all enabled CQs
func (dm *DownsampleManager) RunAll(ctx context.Context) int {
	dm.mu.RLock()
	queries := make([]*ContinuousQuery, 0)
	for _, cq := range dm.queries {
		if cq.Enabled {
			queries = append(queries, cq)
		}
	}
	dm.mu.RUnlock()

	executed := 0
	for _, cq := range queries {
		if err := dm.RunCQ(ctx, cq.Name); err == nil {
			executed++
		}
	}

	return executed
}

// RunCQ executes a single continuous query
func (dm *DownsampleManager) RunCQ(ctx context.Context, name string) error {
	dm.mu.Lock()
	cq, exists := dm.queries[name]
	if !exists {
		dm.mu.Unlock()
		return fmt.Errorf("continuous query %q not found", name)
	}
	if !cq.Enabled {
		dm.mu.Unlock()
		return fmt.Errorf("continuous query %q is disabled", name)
	}

	// Determine time range to process
	now := time.Now()
	startTime := cq.LastProcessedTime
	if startTime.IsZero() {
		// First run - start from 24 hours ago
		startTime = now.Add(-24 * time.Hour)
	}
	endTime := now.Add(-cq.GroupByInterval) // Leave a buffer for late data

	// Copy values we need and unlock
	sourceDB := cq.Database
	sourceMeas := cq.SourceMeasurement
	destMeas := cq.DestMeasurement
	sourceField := cq.SourceField
	aggregateFunc := cq.AggregateFunc
	groupByInterval := cq.GroupByInterval
	dm.mu.Unlock()

	// Query source data
	result, err := dm.engine.Query(
		sourceDB,
		sourceMeas,
		nil, // no tag filters
		startTime.UnixNano(),
		endTime.UnixNano(),
		[]string{sourceField},
	)
	if err != nil {
		return fmt.Errorf("query source data: %w", err)
	}

	// Process and aggregate
	points := dm.aggregate(result, destMeas, sourceField, aggregateFunc, groupByInterval)

	// Write results
	if len(points) > 0 {
		if err := dm.engine.WriteBatch(sourceDB, points); err != nil {
			return fmt.Errorf("write aggregated data: %w", err)
		}
	}

	// Update state
	dm.mu.Lock()
	cq.LastRun = now
	cq.LastProcessedTime = endTime
	dm.save()
	dm.mu.Unlock()

	return nil
}

// aggregate performs aggregation on query results
func (dm *DownsampleManager) aggregate(result *QueryResult, destMeas, sourceField, aggFunc string, interval time.Duration) []*DataPoint {
	if result == nil || len(result.Series) == 0 {
		return nil
	}

	var points []*DataPoint

	for _, series := range result.Series {
		// Find time and field columns
		timeIdx := -1
		fieldIdx := -1
		for i, col := range series.Columns {
			if col == "time" {
				timeIdx = i
			} else if col == sourceField {
				fieldIdx = i
			}
		}

		if timeIdx < 0 || fieldIdx < 0 {
			continue
		}

		// Group by time buckets
		buckets := make(map[int64]*aggregateBucketCQ)

		for _, row := range series.Values {
			ts, ok := getTimestampValue(row[timeIdx])
			if !ok {
				continue
			}

			// Truncate to bucket
			bucketTime := (ts / int64(interval)) * int64(interval)

			bucket, exists := buckets[bucketTime]
			if !exists {
				bucket = &aggregateBucketCQ{
					timestamp: bucketTime,
				}
				buckets[bucketTime] = bucket
			}

			// Add value
			val := getFloatValue(row[fieldIdx])
			bucket.values = append(bucket.values, val)
		}

		// Calculate aggregates and create points
		for _, bucket := range buckets {
			if len(bucket.values) == 0 {
				continue
			}

			aggValue := calculateAggregate(aggFunc, bucket.values)

			point := &DataPoint{
				Measurement: destMeas,
				Tags:        series.Tags,
				Fields: Fields{
					sourceField: NewFloatField(aggValue),
				},
				Timestamp: bucket.timestamp,
			}
			points = append(points, point)
		}
	}

	return points
}

type aggregateBucketCQ struct {
	timestamp int64
	values    []float64
}

// calculateAggregate calculates an aggregate value
func calculateAggregate(fn string, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	switch fn {
	case "mean", "avg":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case "count":
		return float64(len(values))

	case "min":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case "max":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	case "first":
		return values[0]

	case "last":
		return values[len(values)-1]

	default:
		// Default to mean
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}
}

// getTimestampValue extracts a timestamp from an interface value
func getTimestampValue(v interface{}) (int64, bool) {
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

// getFloatValue extracts a float from an interface value
func getFloatValue(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

// validateCQ validates a continuous query definition
func validateCQ(cq *ContinuousQuery) error {
	if cq.Name == "" {
		return fmt.Errorf("continuous query name cannot be empty")
	}

	if cq.Database == "" {
		return fmt.Errorf("database cannot be empty")
	}

	if cq.SourceMeasurement == "" {
		return fmt.Errorf("source measurement cannot be empty")
	}

	if cq.DestMeasurement == "" {
		return fmt.Errorf("destination measurement cannot be empty")
	}

	if cq.SourceField == "" {
		return fmt.Errorf("source field cannot be empty")
	}

	if cq.AggregateFunc == "" {
		return fmt.Errorf("aggregate function cannot be empty")
	}

	if cq.GroupByInterval <= 0 {
		return fmt.Errorf("group by interval must be positive")
	}

	if cq.Interval <= 0 {
		cq.Interval = time.Minute // default
	}

	// Validate aggregate function
	validFuncs := map[string]bool{
		"mean": true, "avg": true, "sum": true, "count": true,
		"min": true, "max": true, "first": true, "last": true,
	}
	if !validFuncs[cq.AggregateFunc] {
		return fmt.Errorf("invalid aggregate function: %s", cq.AggregateFunc)
	}

	return nil
}

// CQStatus represents the status of a continuous query
type CQStatus struct {
	Name              string    `json:"name"`
	Enabled           bool      `json:"enabled"`
	LastRun           time.Time `json:"last_run"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	NextRun           time.Time `json:"next_run"`
}

// Status returns the status of a continuous query
func (dm *DownsampleManager) Status(name string) (*CQStatus, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	cq, exists := dm.queries[name]
	if !exists {
		return nil, fmt.Errorf("continuous query %q not found", name)
	}

	nextRun := cq.LastRun.Add(cq.Interval)
	if cq.LastRun.IsZero() {
		nextRun = time.Now()
	}

	return &CQStatus{
		Name:              cq.Name,
		Enabled:           cq.Enabled,
		LastRun:           cq.LastRun,
		LastProcessedTime: cq.LastProcessedTime,
		NextRun:           nextRun,
	}, nil
}
