package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()

	if cfg.MinShardSize != 10*1024*1024 {
		t.Errorf("MinShardSize = %d, want %d", cfg.MinShardSize, 10*1024*1024)
	}

	if cfg.MaxShardsPerCompaction != 4 {
		t.Errorf("MaxShardsPerCompaction = %d, want %d", cfg.MaxShardsPerCompaction, 4)
	}

	if cfg.CompactionInterval != 1*time.Hour {
		t.Errorf("CompactionInterval = %v, want %v", cfg.CompactionInterval, 1*time.Hour)
	}

	if cfg.MinAge != 24*time.Hour {
		t.Errorf("MinAge = %v, want %v", cfg.MinAge, 24*time.Hour)
	}
}

func TestNewCompactionManager(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	cfg := DefaultCompactionConfig()
	cm := NewCompactionManager(engine, cfg)

	if cm == nil {
		t.Fatal("NewCompactionManager() returned nil")
	}

	if cm.running {
		t.Error("CompactionManager should not be running initially")
	}
}

func TestCompactionManagerStartStop(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	cfg := DefaultCompactionConfig()
	cfg.CompactionInterval = 100 * time.Millisecond // Short interval for testing
	cm := NewCompactionManager(engine, cfg)

	// Start
	cm.Start()
	if !cm.running {
		t.Error("CompactionManager should be running after Start()")
	}

	// Start again (should be no-op)
	cm.Start()

	// Stop
	cm.Stop()
	time.Sleep(50 * time.Millisecond) // Give goroutine time to exit
	
	if cm.running {
		t.Error("CompactionManager should not be running after Stop()")
	}

	// Stop again (should be no-op)
	cm.Stop()
}

func TestCompactionManagerGetStats(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	cm := NewCompactionManager(engine, DefaultCompactionConfig())

	stats := cm.GetStats()
	if stats.Running {
		t.Error("expected Running = false")
	}

	cm.Start()
	defer cm.Stop()

	stats = cm.GetStats()
	if !stats.Running {
		t.Error("expected Running = true")
	}
}

func TestCompactNowNoShards(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	cm := NewCompactionManager(engine, DefaultCompactionConfig())

	// Should not error with no shards
	if err := cm.CompactNow(); err != nil {
		t.Errorf("CompactNow() error = %v", err)
	}
}

func TestShardSize(t *testing.T) {
	dir := t.TempDir()

	now := time.Now()
	shard, err := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		Database:  "testdb",
		StartTime: now.Add(-1 * time.Hour),
		EndTime:   now.Add(1 * time.Hour), // Extend end time to future
	})
	if err != nil {
		t.Fatalf("NewShard() error = %v", err)
	}
	defer shard.Close()

	size := shard.Size()
	if size < 0 {
		t.Errorf("Size() = %d, expected >= 0", size)
	}

	// Write some data with timestamp in the middle of the shard range
	point := &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"value": NewFloatField(42.5)},
		Timestamp:   now.UnixNano(), // Use captured 'now' which is within range
	}
	
	if err := shard.Write(point); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := shard.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	newSize := shard.Size()
	if newSize <= size {
		t.Errorf("Size after write = %d, expected > %d", newSize, size)
	}
}

func TestCompactionCandidateSelection(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	// Create database
	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}

	cfg := DefaultCompactionConfig()
	cfg.MinAge = 0 // Allow immediate compaction for testing
	cfg.MinShardSize = 1024 * 1024 * 1024 // 1GB - larger than any test shard
	cm := NewCompactionManager(engine, cfg)

	db, _ := engine.GetDatabase("testdb")
	candidates := cm.findCompactionCandidates(db)

	// No candidates expected since no cold shards
	if len(candidates) != 0 {
		t.Errorf("expected 0 candidates, got %d", len(candidates))
	}
}

func TestColumnFileReadAllEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("CreateColumnFile() error = %v", err)
	}
	defer cf.Close()

	blocks, err := cf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(blocks) != 0 {
		t.Errorf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestColumnFileReadAllWithData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("CreateColumnFile() error = %v", err)
	}
	defer cf.Close()

	// Write some data
	timestamps := []int64{1000, 2000, 3000}
	values := []float64{1.1, 2.2, 3.3}

	if err := cf.AppendBlock(timestamps, values); err != nil {
		t.Fatalf("AppendBlock() error = %v", err)
	}

	blocks, err := cf.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}

	if len(blocks[0].Timestamps) != 3 {
		t.Errorf("expected 3 timestamps, got %d", len(blocks[0].Timestamps))
	}
}

func TestColumnFileWriteBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("CreateColumnFile() error = %v", err)
	}
	defer cf.Close()

	block := Block{
		Timestamps: []int64{1000, 2000, 3000},
		Values:     []float64{1.1, 2.2, 3.3},
	}

	if err := cf.WriteBlock(block); err != nil {
		t.Fatalf("WriteBlock() error = %v", err)
	}

	pointCount, blockCount, _, _ := cf.Stats()
	if pointCount != 3 {
		t.Errorf("pointCount = %d, want 3", pointCount)
	}
	if blockCount != 1 {
		t.Errorf("blockCount = %d, want 1", blockCount)
	}
}

func TestCompactionStatsZeroValues(t *testing.T) {
	stats := CompactionStats{}

	if stats.Running {
		t.Error("default Running should be false")
	}
	if stats.ShardsCompacted != 0 {
		t.Error("default ShardsCompacted should be 0")
	}
	if stats.BytesCompacted != 0 {
		t.Error("default BytesCompacted should be 0")
	}
}

func TestShardSizeWithNonexistentDir(t *testing.T) {
	shard := &Shard{
		dir: "/nonexistent/path/shard",
	}

	size := shard.Size()
	// Should return 0 for nonexistent directory, not error
	if size != 0 {
		t.Errorf("Size() = %d for nonexistent dir, expected 0", size)
	}
}

func TestCompactionManagerRunCompactionNoDatabases(t *testing.T) {
	dir := t.TempDir()
	
	engine, err := NewEngine(EngineConfig{
		DataDir: dir,
		WALDir:  filepath.Join(dir, "wal"),
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	defer engine.Close()

	cm := NewCompactionManager(engine, DefaultCompactionConfig())

	// Should not panic or error with no databases
	cm.runCompaction()
}
