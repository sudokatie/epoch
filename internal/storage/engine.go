package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Engine is the main storage engine that coordinates all components
type Engine struct {
	mu sync.RWMutex

	// Configuration
	config EngineConfig

	// Databases
	databases map[string]*DatabaseState

	// Root directory
	dataDir string
}

// EngineConfig holds engine configuration
type EngineConfig struct {
	DataDir         string
	WALDir          string
	ShardDuration   time.Duration
	RetentionPeriod time.Duration
	MaxBufferSize   int
	FlushInterval   time.Duration
}

// DatabaseState holds state for a single database
type DatabaseState struct {
	mu sync.RWMutex

	name   string
	path   string
	index  *TagIndex
	shards map[uint64]*Shard

	// Shard configuration
	shardDuration time.Duration
	nextShardID   uint64
}

// DefaultEngineConfig returns sensible defaults
func DefaultEngineConfig(dataDir string) EngineConfig {
	return EngineConfig{
		DataDir:         dataDir,
		WALDir:          filepath.Join(dataDir, "wal"),
		ShardDuration:   24 * time.Hour,
		RetentionPeriod: 7 * 24 * time.Hour,
		MaxBufferSize:   10000,
		FlushInterval:   10 * time.Second,
	}
}

// NewEngine creates a new storage engine
func NewEngine(config EngineConfig) (*Engine, error) {
	if config.ShardDuration == 0 {
		config.ShardDuration = 24 * time.Hour
	}
	if config.MaxBufferSize == 0 {
		config.MaxBufferSize = 10000
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	e := &Engine{
		config:    config,
		databases: make(map[string]*DatabaseState),
		dataDir:   config.DataDir,
	}

	// Load existing databases
	if err := e.loadDatabases(); err != nil {
		return nil, err
	}

	return e, nil
}

// loadDatabases discovers and loads existing databases
func (e *Engine) loadDatabases() error {
	entries, err := os.ReadDir(e.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if it's a database directory (has index file)
		indexPath := filepath.Join(e.dataDir, entry.Name(), "index.idx")
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			continue
		}

		dbName := entry.Name()
		if _, err := e.openDatabase(dbName); err != nil {
			return fmt.Errorf("open database %s: %w", dbName, err)
		}
	}

	return nil
}

// CreateDatabase creates a new database
func (e *Engine) CreateDatabase(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	dbPath := filepath.Join(e.dataDir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("create database dir: %w", err)
	}

	db := &DatabaseState{
		name:          name,
		path:          dbPath,
		index:         NewTagIndex(),
		shards:        make(map[uint64]*Shard),
		shardDuration: e.config.ShardDuration,
		nextShardID:   1,
	}

	// Save initial index
	if err := db.index.Save(filepath.Join(dbPath, "index.idx")); err != nil {
		return fmt.Errorf("save index: %w", err)
	}

	e.databases[name] = db
	return nil
}

// openDatabase opens an existing database
func (e *Engine) openDatabase(name string) (*DatabaseState, error) {
	dbPath := filepath.Join(e.dataDir, name)

	db := &DatabaseState{
		name:          name,
		path:          dbPath,
		index:         NewTagIndex(),
		shards:        make(map[uint64]*Shard),
		shardDuration: e.config.ShardDuration,
		nextShardID:   1,
	}

	// Load index
	indexPath := filepath.Join(dbPath, "index.idx")
	if err := db.index.Load(indexPath); err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}

	// Discover shards
	// For now, we don't load shards until needed (lazy loading)

	e.databases[name] = db
	return db, nil
}

// DropDatabase drops a database
func (e *Engine) DropDatabase(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, ok := e.databases[name]
	if !ok {
		return fmt.Errorf("database %s not found", name)
	}

	// Close all shards
	db.mu.Lock()
	for _, shard := range db.shards {
		shard.Close()
	}
	db.mu.Unlock()

	// Remove from disk
	if err := os.RemoveAll(db.path); err != nil {
		return fmt.Errorf("remove database: %w", err)
	}

	delete(e.databases, name)
	return nil
}

// GetDatabase returns a database by name
func (e *Engine) GetDatabase(name string) (*DatabaseState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	db, ok := e.databases[name]
	return db, ok
}

// ListDatabases returns all database names
func (e *Engine) ListDatabases() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.databases))
	for name := range e.databases {
		names = append(names, name)
	}
	return names
}

// Write writes a data point to the engine
func (e *Engine) Write(database string, point *DataPoint) error {
	db, ok := e.GetDatabase(database)
	if !ok {
		return fmt.Errorf("database %s not found", database)
	}

	return db.Write(point, e.config)
}

// WriteBatch writes multiple data points
func (e *Engine) WriteBatch(database string, points []*DataPoint) error {
	db, ok := e.GetDatabase(database)
	if !ok {
		return fmt.Errorf("database %s not found", database)
	}

	for _, point := range points {
		if err := db.Write(point, e.config); err != nil {
			return err
		}
	}
	return nil
}

// Query executes a query against the engine
func (e *Engine) Query(database string, measurement string, tags map[string]string, minTime, maxTime int64, fields []string) (*QueryResult, error) {
	db, ok := e.GetDatabase(database)
	if !ok {
		return nil, fmt.Errorf("database %s not found", database)
	}

	return db.Query(measurement, tags, minTime, maxTime, fields)
}

// Flush flushes all databases
func (e *Engine) Flush() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, db := range e.databases {
		if err := db.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the engine
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, db := range e.databases {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// DatabaseState methods

// Write writes a data point to the database
func (db *DatabaseState) Write(point *DataPoint, config EngineConfig) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Add series to index
	db.index.AddSeries(point.Measurement, point.Tags)

	// Find or create appropriate shard
	shard, err := db.getOrCreateShard(point.Timestamp, config)
	if err != nil {
		return err
	}

	return shard.Write(point)
}

// getOrCreateShard finds or creates a shard for the given timestamp
func (db *DatabaseState) getOrCreateShard(timestamp int64, config EngineConfig) (*Shard, error) {
	// Calculate shard boundaries
	ts := time.Unix(0, timestamp)
	shardStart := ts.Truncate(db.shardDuration)
	shardEnd := shardStart.Add(db.shardDuration)

	// Find existing shard
	for _, shard := range db.shards {
		if shard.Info().StartTime.Equal(shardStart) {
			return shard, nil
		}
	}

	// Create new shard
	shardID := db.nextShardID
	db.nextShardID++

	shard, err := NewShard(ShardConfig{
		Dir:           filepath.Join(db.path, "shards"),
		ID:            shardID,
		Database:      db.name,
		StartTime:     shardStart,
		EndTime:       shardEnd,
		FlushInterval: config.FlushInterval,
		MaxBufferSize: config.MaxBufferSize,
	})
	if err != nil {
		return nil, err
	}

	db.shards[shardID] = shard
	return shard, nil
}

// Query queries the database
func (db *DatabaseState) Query(measurement string, tags map[string]string, minTime, maxTime int64, fields []string) (*QueryResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Build filter
	filter := make(map[string]string)
	if measurement != "" {
		filter["_measurement"] = measurement
	}
	for k, v := range tags {
		filter[k] = v
	}

	// Get matching series
	seriesIDs := db.index.Query(filter)
	if len(seriesIDs) == 0 {
		return &QueryResult{}, nil
	}

	// Query each matching series from relevant shards
	result := &QueryResult{}

	for _, seriesID := range seriesIDs {
		entry := db.index.GetSeries(seriesID)
		if entry == nil {
			continue
		}

		series := &ResultSeries{
			Name: entry.Measurement,
			Tags: entry.Tags,
		}

		// Query each shard
		for _, shard := range db.shards {
			info := shard.Info()
			// Skip shards outside time range
			if info.EndTime.UnixNano() < minTime || info.StartTime.UnixNano() > maxTime {
				continue
			}

			// Query each field
			for _, fieldName := range fields {
				timestamps, values, err := shard.Read(entry.Key, fieldName, minTime, maxTime)
				if err != nil {
					continue
				}
				if len(timestamps) == 0 {
					continue
				}

				// Build columns and values
				if len(series.Columns) == 0 {
					series.Columns = []string{"time"}
				}

				// Add field column if not present
				hasField := false
				for _, col := range series.Columns {
					if col == fieldName {
						hasField = true
						break
					}
				}
				if !hasField {
					series.Columns = append(series.Columns, fieldName)
				}

				// Add values
				for i, ts := range timestamps {
					row := make([]interface{}, len(series.Columns))
					row[0] = ts

					// Get field value
					switch v := values.(type) {
					case []float64:
						row[1] = v[i]
					case []int64:
						row[1] = v[i]
					case []string:
						row[1] = v[i]
					case []bool:
						row[1] = v[i]
					}

					series.Values = append(series.Values, row)
				}
			}
		}

		if len(series.Values) > 0 {
			result.Series = append(result.Series, series)
		}
	}

	return result, nil
}

// Flush flushes all shards
func (db *DatabaseState) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, shard := range db.shards {
		if err := shard.Flush(); err != nil {
			return err
		}
	}

	// Save index
	return db.index.Save(filepath.Join(db.path, "index.idx"))
}

// Close closes the database
func (db *DatabaseState) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close all shards
	for _, shard := range db.shards {
		if err := shard.Close(); err != nil {
			return err
		}
	}

	// Save index
	return db.index.Save(filepath.Join(db.path, "index.idx"))
}

// GetMeasurements returns all measurements
func (db *DatabaseState) GetMeasurements() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.GetMeasurements()
}

// GetTagKeys returns all tag keys
func (db *DatabaseState) GetTagKeys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.GetTagKeys()
}

// GetTagValues returns values for a tag key
func (db *DatabaseState) GetTagValues(key string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.GetTagValues(key)
}

// SeriesCount returns the number of series
func (db *DatabaseState) SeriesCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.SeriesCount()
}
