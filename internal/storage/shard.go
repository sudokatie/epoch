package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ShardState represents the state of a shard
type ShardState int

const (
	ShardStateHot ShardState = iota  // Active, in-memory writes
	ShardStateCold                    // Read-only, on disk
)

// Shard represents a time-bounded storage unit
type Shard struct {
	mu        sync.RWMutex
	info      *ShardInfo
	state     ShardState
	
	// In-memory write buffer
	buffer    map[string]*seriesBuffer // series key -> buffer
	
	// Column files per field
	columns   map[string]*ColumnFile
	
	// WAL for durability
	wal       *WAL
	
	// Configuration
	dir           string
	flushInterval time.Duration
	maxBufferSize int
}

// seriesBuffer holds buffered points for a single series
type seriesBuffer struct {
	timestamps []int64
	fields     map[string][]interface{} // field name -> values
}

// ShardConfig holds shard configuration
type ShardConfig struct {
	Dir           string
	ID            uint64
	Database      string
	StartTime     time.Time
	EndTime       time.Time
	FlushInterval time.Duration
	MaxBufferSize int
}

// NewShard creates a new shard
func NewShard(config ShardConfig) (*Shard, error) {
	if config.FlushInterval == 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.MaxBufferSize == 0 {
		config.MaxBufferSize = 10000
	}

	dir := filepath.Join(config.Dir, fmt.Sprintf("shard-%d", config.ID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create shard dir: %w", err)
	}

	// Create WAL
	wal, err := NewWAL(WALConfig{
		Dir:      filepath.Join(dir, "wal"),
		SyncMode: SyncEveryN,
		SyncEveryN: 100,
	})
	if err != nil {
		return nil, fmt.Errorf("create WAL: %w", err)
	}

	s := &Shard{
		info: &ShardInfo{
			ID:        config.ID,
			Database:  config.Database,
			StartTime: config.StartTime,
			EndTime:   config.EndTime,
			Path:      dir,
		},
		state:         ShardStateHot,
		buffer:        make(map[string]*seriesBuffer),
		columns:       make(map[string]*ColumnFile),
		wal:           wal,
		dir:           dir,
		flushInterval: config.FlushInterval,
		maxBufferSize: config.MaxBufferSize,
	}

	return s, nil
}

// OpenShard opens an existing shard
func OpenShard(dir string) (*Shard, error) {
	// Read shard metadata
	metaPath := filepath.Join(dir, "meta.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("shard metadata not found")
	}

	// For now, create a basic shard - metadata loading can be added later
	wal, err := NewWAL(WALConfig{
		Dir:      filepath.Join(dir, "wal"),
		SyncMode: SyncEveryN,
	})
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	s := &Shard{
		info:    &ShardInfo{Path: dir},
		state:   ShardStateCold,
		buffer:  make(map[string]*seriesBuffer),
		columns: make(map[string]*ColumnFile),
		wal:     wal,
		dir:     dir,
	}

	// Load existing column files
	if err := s.loadColumnFiles(); err != nil {
		return nil, err
	}

	return s, nil
}

// loadColumnFiles discovers and opens existing column files
func (s *Shard) loadColumnFiles() error {
	colDir := filepath.Join(s.dir, "columns")
	entries, err := os.ReadDir(colDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".col" {
			continue
		}

		path := filepath.Join(colDir, entry.Name())
		cf, err := OpenColumnFile(path)
		if err != nil {
			return fmt.Errorf("open column %s: %w", entry.Name(), err)
		}

		fieldName := entry.Name()[:len(entry.Name())-4] // Remove .col
		s.columns[fieldName] = cf
	}

	return nil
}

// Write writes a data point to the shard
func (s *Shard) Write(point *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if point is within shard's time range
	if !s.info.Contains(point.Timestamp) {
		return fmt.Errorf("timestamp %d outside shard range [%d, %d)",
			point.Timestamp, s.info.StartTime.UnixNano(), s.info.EndTime.UnixNano())
	}

	// Write to WAL first
	// Use line protocol format for WAL entries
	walData := []byte(point.SeriesKey() + " " + fmt.Sprintf("%d", point.Timestamp))
	if err := s.wal.AppendWrite(walData); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	// Add to buffer
	seriesKey := point.SeriesKey()
	buf, ok := s.buffer[seriesKey]
	if !ok {
		buf = &seriesBuffer{
			fields: make(map[string][]interface{}),
		}
		s.buffer[seriesKey] = buf
	}

	buf.timestamps = append(buf.timestamps, point.Timestamp)
	for fieldName, fieldValue := range point.Fields {
		var value interface{}
		switch fieldValue.Type {
		case FieldTypeFloat:
			value = fieldValue.FloatValue
		case FieldTypeInteger:
			value = fieldValue.IntValue
		case FieldTypeString:
			value = fieldValue.StringValue
		case FieldTypeBoolean:
			value = fieldValue.BooleanValue
		}
		buf.fields[fieldName] = append(buf.fields[fieldName], value)
	}

	// Check if we need to flush
	totalPoints := 0
	for _, b := range s.buffer {
		totalPoints += len(b.timestamps)
	}
	if totalPoints >= s.maxBufferSize {
		return s.flushUnlocked()
	}

	return nil
}

// WriteBatch writes multiple data points
func (s *Shard) WriteBatch(points []*DataPoint) error {
	for _, point := range points {
		if err := s.Write(point); err != nil {
			return err
		}
	}
	return nil
}

// Flush writes buffered data to column files
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushUnlocked()
}

func (s *Shard) flushUnlocked() error {
	if len(s.buffer) == 0 {
		return nil
	}

	colDir := filepath.Join(s.dir, "columns")
	if err := os.MkdirAll(colDir, 0755); err != nil {
		return err
	}

	// Flush each series
	for seriesKey, buf := range s.buffer {
		if len(buf.timestamps) == 0 {
			continue
		}

		// Flush each field
		for fieldName, values := range buf.fields {
			colKey := seriesKey + "." + fieldName
			cf, ok := s.columns[colKey]
			if !ok {
				// Determine field type
				var fieldType FieldType
				switch values[0].(type) {
				case float64:
					fieldType = FieldTypeFloat
				case int64:
					fieldType = FieldTypeInteger
				case string:
					fieldType = FieldTypeString
				case bool:
					fieldType = FieldTypeBoolean
				}

				path := filepath.Join(colDir, colKey+".col")
				var err error
				cf, err = CreateColumnFile(path, fieldType)
				if err != nil {
					return fmt.Errorf("create column %s: %w", colKey, err)
				}
				s.columns[colKey] = cf
			}

			// Convert values to typed slice
			var typedValues interface{}
			switch values[0].(type) {
			case float64:
				floats := make([]float64, len(values))
				for i, v := range values {
					floats[i] = v.(float64)
				}
				typedValues = floats
			case int64:
				ints := make([]int64, len(values))
				for i, v := range values {
					ints[i] = v.(int64)
				}
				typedValues = ints
			case string:
				strs := make([]string, len(values))
				for i, v := range values {
					strs[i] = v.(string)
				}
				typedValues = strs
			case bool:
				bools := make([]bool, len(values))
				for i, v := range values {
					bools[i] = v.(bool)
				}
				typedValues = bools
			}

			if err := cf.AppendBlock(buf.timestamps, typedValues); err != nil {
				return fmt.Errorf("append to column %s: %w", colKey, err)
			}
		}
	}

	// Clear buffer
	s.buffer = make(map[string]*seriesBuffer)

	// Write checkpoint to WAL
	s.wal.AppendCheckpoint()

	return nil
}

// Read reads points for a series within a time range
func (s *Shard) Read(seriesKey string, fieldName string, minTime, maxTime int64) ([]int64, interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	colKey := seriesKey + "." + fieldName
	cf, ok := s.columns[colKey]
	if !ok {
		return nil, nil, nil // No data for this field
	}

	return cf.ReadTimeRange(minTime, maxTime)
}

// ReadBuffer returns buffered (unflushed) data for a series
func (s *Shard) ReadBuffer(seriesKey string) ([]int64, map[string][]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf, ok := s.buffer[seriesKey]
	if !ok {
		return nil, nil, false
	}

	// Copy to avoid races
	timestamps := make([]int64, len(buf.timestamps))
	copy(timestamps, buf.timestamps)

	fields := make(map[string][]interface{})
	for k, v := range buf.fields {
		copied := make([]interface{}, len(v))
		copy(copied, v)
		fields[k] = copied
	}

	return timestamps, fields, true
}

// Info returns shard metadata
func (s *Shard) Info() *ShardInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info
}

// State returns the shard state
func (s *Shard) State() ShardState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// SetReadOnly marks the shard as read-only (cold)
func (s *Shard) SetReadOnly() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush any remaining buffer
	if err := s.flushUnlocked(); err != nil {
		return err
	}

	s.state = ShardStateCold
	s.info.ReadOnly = true

	return nil
}

// Close flushes and closes the shard
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush buffer
	if err := s.flushUnlocked(); err != nil {
		return err
	}

	// Close column files
	for _, cf := range s.columns {
		if err := cf.Close(); err != nil {
			return err
		}
	}

	// Close WAL
	if err := s.wal.Close(); err != nil {
		return err
	}

	return nil
}

// Stats returns shard statistics
func (s *Shard) Stats() (bufferedPoints int, columnCount int, walSegments int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, buf := range s.buffer {
		bufferedPoints += len(buf.timestamps)
	}

	columnCount = len(s.columns)

	walSegs, _, _ := s.wal.Stats()
	walSegments = walSegs

	return
}

// Size returns the total size of the shard on disk in bytes
func (s *Shard) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalSize int64

	// Walk the shard directory and sum file sizes
	filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize
}
