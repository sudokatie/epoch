package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// CompactionConfig holds compaction configuration
type CompactionConfig struct {
	// MinShardSize is the minimum size in bytes below which shards are candidates for compaction
	MinShardSize int64
	// MaxShardsPerCompaction is the maximum number of shards to merge in one compaction
	MaxShardsPerCompaction int
	// CompactionInterval is how often to check for compaction candidates
	CompactionInterval time.Duration
	// MinAge is the minimum age of a shard before it can be compacted
	MinAge time.Duration
}

// DefaultCompactionConfig returns sensible defaults
func DefaultCompactionConfig() CompactionConfig {
	return CompactionConfig{
		MinShardSize:           10 * 1024 * 1024, // 10MB
		MaxShardsPerCompaction: 4,
		CompactionInterval:     1 * time.Hour,
		MinAge:                 24 * time.Hour,
	}
}

// CompactionManager handles shard compaction
type CompactionManager struct {
	mu       sync.Mutex
	config   CompactionConfig
	engine   *Engine
	running  bool
	stopChan chan struct{}
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(engine *Engine, config CompactionConfig) *CompactionManager {
	return &CompactionManager{
		config:   config,
		engine:   engine,
		stopChan: make(chan struct{}),
	}
}

// Start begins the compaction background process
func (cm *CompactionManager) Start() {
	cm.mu.Lock()
	if cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = true
	cm.mu.Unlock()

	go cm.compactionLoop()
}

// Stop stops the compaction background process
func (cm *CompactionManager) Stop() {
	cm.mu.Lock()
	if !cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = false
	cm.mu.Unlock()

	close(cm.stopChan)
}

func (cm *CompactionManager) compactionLoop() {
	ticker := time.NewTicker(cm.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.runCompaction()
		case <-cm.stopChan:
			return
		}
	}
}

// runCompaction checks for and performs compaction
func (cm *CompactionManager) runCompaction() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get all databases
	databases := cm.engine.ListDatabases()

	for _, dbName := range databases {
		db, ok := cm.engine.GetDatabase(dbName)
		if !ok {
			continue
		}

		candidates := cm.findCompactionCandidates(db)
		if len(candidates) >= 2 {
			cm.compactShards(db, candidates)
		}
	}
}

// findCompactionCandidates finds shards that are candidates for compaction
func (cm *CompactionManager) findCompactionCandidates(db *DatabaseState) []*Shard {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var candidates []*Shard
	now := time.Now()
	minAge := cm.config.MinAge

	for _, shard := range db.shards {
		// Only compact cold shards
		if shard.state != ShardStateCold {
			continue
		}

		// Check age
		if now.Sub(shard.info.EndTime) < minAge {
			continue
		}

		// Check size
		size := shard.Size()
		if size > cm.config.MinShardSize {
			continue
		}

		candidates = append(candidates, shard)
	}

	// Sort by start time
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].info.StartTime.Before(candidates[j].info.StartTime)
	})

	// Limit to max shards per compaction
	if len(candidates) > cm.config.MaxShardsPerCompaction {
		candidates = candidates[:cm.config.MaxShardsPerCompaction]
	}

	return candidates
}

// compactShards merges multiple shards into one
func (cm *CompactionManager) compactShards(db *DatabaseState, shards []*Shard) error {
	if len(shards) < 2 {
		return nil
	}

	// Determine the time range for the merged shard
	minTime := shards[0].info.StartTime
	maxTime := shards[0].info.EndTime

	for _, s := range shards[1:] {
		if s.info.StartTime.Before(minTime) {
			minTime = s.info.StartTime
		}
		if s.info.EndTime.After(maxTime) {
			maxTime = s.info.EndTime
		}
	}

	// Create new shard for merged data
	newShardID := db.nextShardID
	db.nextShardID++

	newShard, err := NewShard(ShardConfig{
		Dir:       db.path,
		ID:        newShardID,
		Database:  db.name,
		StartTime: minTime,
		EndTime:   maxTime,
	})
	if err != nil {
		return fmt.Errorf("create merged shard: %w", err)
	}

	// Merge data from all shards
	for _, oldShard := range shards {
		if err := cm.mergeShardData(oldShard, newShard); err != nil {
			return fmt.Errorf("merge shard %d: %w", oldShard.info.ID, err)
		}
	}

	// Flush the merged shard
	if err := newShard.Flush(); err != nil {
		return fmt.Errorf("flush merged shard: %w", err)
	}

	// Mark new shard as cold
	newShard.state = ShardStateCold

	// Update database state
	db.mu.Lock()
	db.shards[newShardID] = newShard
	for _, oldShard := range shards {
		delete(db.shards, oldShard.info.ID)
	}
	db.mu.Unlock()

	// Close and remove old shards
	for _, oldShard := range shards {
		oldShard.Close()
		os.RemoveAll(oldShard.dir)
	}

	return nil
}

// mergeShardData copies all data from source shard to destination
func (cm *CompactionManager) mergeShardData(src, dst *Shard) error {
	src.mu.RLock()
	defer src.mu.RUnlock()

	// Copy data from each column file
	for fieldName, srcCol := range src.columns {
		// Read all data from source column
		data, err := srcCol.ReadAll()
		if err != nil {
			return fmt.Errorf("read column %s: %w", fieldName, err)
		}

		// Get or create destination column
		dstCol, ok := dst.columns[fieldName]
		if !ok {
			colPath := filepath.Join(dst.dir, fieldName+".col")
			dstCol, err = CreateColumnFile(colPath, FieldType(srcCol.header.FieldType))
			if err != nil {
				return fmt.Errorf("create column %s: %w", fieldName, err)
			}
			dst.columns[fieldName] = dstCol
		}

		// Write data to destination
		for _, block := range data {
			if err := dstCol.WriteBlock(block); err != nil {
				return fmt.Errorf("write block to %s: %w", fieldName, err)
			}
		}
	}

	return nil
}

// CompactNow forces an immediate compaction check
func (cm *CompactionManager) CompactNow() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	databases := cm.engine.ListDatabases()

	for _, dbName := range databases {
		db, ok := cm.engine.GetDatabase(dbName)
		if !ok {
			continue
		}

		candidates := cm.findCompactionCandidates(db)
		if len(candidates) >= 2 {
			if err := cm.compactShards(db, candidates); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetStats returns compaction statistics
func (cm *CompactionManager) GetStats() CompactionStats {
	return CompactionStats{
		Running: cm.running,
	}
}

// CompactionStats holds compaction statistics
type CompactionStats struct {
	Running           bool
	LastCompaction    time.Time
	ShardsCompacted   int64
	BytesCompacted    int64
	CompactionErrors  int64
}
