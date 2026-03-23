package storage

import (
	"testing"
	"time"
)

func TestEngineCreate(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(DefaultEngineConfig(dir))
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	defer engine.Close()

	// Should start with no databases
	dbs := engine.ListDatabases()
	if len(dbs) != 0 {
		t.Errorf("expected 0 databases, got %d", len(dbs))
	}
}

func TestEngineCreateDatabase(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(DefaultEngineConfig(dir))
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	defer engine.Close()

	if err := engine.CreateDatabase("testdb"); err != nil {
		t.Fatalf("create database: %v", err)
	}

	dbs := engine.ListDatabases()
	if len(dbs) != 1 || dbs[0] != "testdb" {
		t.Errorf("databases = %v, want [testdb]", dbs)
	}

	// Creating same database should fail
	if err := engine.CreateDatabase("testdb"); err == nil {
		t.Error("expected error for duplicate database")
	}
}

func TestEngineDropDatabase(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	if err := engine.DropDatabase("testdb"); err != nil {
		t.Fatalf("drop database: %v", err)
	}

	dbs := engine.ListDatabases()
	if len(dbs) != 0 {
		t.Errorf("databases after drop = %v", dbs)
	}

	// Dropping non-existent should fail
	if err := engine.DropDatabase("nonexistent"); err == nil {
		t.Error("expected error for non-existent database")
	}
}

func TestEngineWriteAndQuery(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	// Write some data
	now := time.Now()
	for i := 0; i < 10; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": "server1"},
			Fields: Fields{
				"usage": NewFloatField(float64(i) * 10),
			},
			Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
		}
		if err := engine.Write("testdb", point); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Flush to disk
	engine.Flush()

	// Query
	result, err := engine.Query(
		"testdb",
		"cpu",
		map[string]string{"host": "server1"},
		now.UnixNano(),
		now.Add(time.Hour).UnixNano(),
		[]string{"usage"},
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	series := result.Series[0]
	if series.Name != "cpu" {
		t.Errorf("series name = %q, want cpu", series.Name)
	}
	if len(series.Values) != 10 {
		t.Errorf("got %d values, want 10", len(series.Values))
	}
}

func TestEngineWriteBatch(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	now := time.Now()
	points := make([]*DataPoint, 100)
	for i := 0; i < 100; i++ {
		points[i] = &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": "server1"},
			Fields:      Fields{"usage": NewFloatField(float64(i))},
			Timestamp:   now.Add(time.Duration(i) * time.Second).UnixNano(),
		}
	}

	if err := engine.WriteBatch("testdb", points); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	// Verify
	db, _ := engine.GetDatabase("testdb")
	if db.SeriesCount() != 1 {
		t.Errorf("series count = %d, want 1", db.SeriesCount())
	}
}

func TestEngineMultipleSeries(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	now := time.Now()
	hosts := []string{"server1", "server2", "server3"}

	for _, host := range hosts {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": host},
			Fields:      Fields{"usage": NewFloatField(42.5)},
			Timestamp:   now.UnixNano(),
		}
		engine.Write("testdb", point)
	}

	engine.Flush()

	db, _ := engine.GetDatabase("testdb")
	if db.SeriesCount() != 3 {
		t.Errorf("series count = %d, want 3", db.SeriesCount())
	}

	// Query specific host
	result, _ := engine.Query(
		"testdb", "cpu",
		map[string]string{"host": "server1"},
		0, now.Add(time.Hour).UnixNano(),
		[]string{"usage"},
	)

	if len(result.Series) != 1 {
		t.Errorf("query returned %d series, want 1", len(result.Series))
	}
}

func TestEngineReopen(t *testing.T) {
	dir := t.TempDir()

	// Create engine and write data
	engine1, _ := NewEngine(DefaultEngineConfig(dir))
	engine1.CreateDatabase("testdb")

	now := time.Now()
	engine1.Write("testdb", &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"usage": NewFloatField(42.5)},
		Timestamp:   now.UnixNano(),
	})

	engine1.Flush()
	engine1.Close()

	// Reopen
	engine2, err := NewEngine(DefaultEngineConfig(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer engine2.Close()

	// Database should exist
	dbs := engine2.ListDatabases()
	if len(dbs) != 1 || dbs[0] != "testdb" {
		t.Errorf("databases after reopen = %v", dbs)
	}

	// Index should be loaded
	db, _ := engine2.GetDatabase("testdb")
	if db.SeriesCount() != 1 {
		t.Errorf("series count after reopen = %d, want 1", db.SeriesCount())
	}
}

func TestEngineWriteToNonExistentDB(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	point := &DataPoint{
		Measurement: "cpu",
		Fields:      Fields{"v": NewFloatField(1)},
		Timestamp:   time.Now().UnixNano(),
	}

	err := engine.Write("nonexistent", point)
	if err == nil {
		t.Error("expected error for non-existent database")
	}
}

func TestDatabaseStateMetadata(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	now := time.Now()
	engine.Write("testdb", &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1", "region": "us-west"},
		Fields:      Fields{"usage": NewFloatField(42.5)},
		Timestamp:   now.UnixNano(),
	})
	engine.Write("testdb", &DataPoint{
		Measurement: "memory",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"used": NewIntField(8192)},
		Timestamp:   now.UnixNano(),
	})

	db, _ := engine.GetDatabase("testdb")

	// Check measurements
	measurements := db.GetMeasurements()
	if len(measurements) != 2 {
		t.Errorf("measurements = %v", measurements)
	}

	// Check tag keys
	keys := db.GetTagKeys()
	if len(keys) != 2 { // host, region
		t.Errorf("tag keys = %v", keys)
	}

	// Check tag values
	hosts := db.GetTagValues("host")
	if len(hosts) != 1 || hosts[0] != "server1" {
		t.Errorf("host values = %v", hosts)
	}
}

func TestEngineQueryNoResults(t *testing.T) {
	dir := t.TempDir()

	engine, _ := NewEngine(DefaultEngineConfig(dir))
	defer engine.Close()

	engine.CreateDatabase("testdb")

	// Query empty database
	result, err := engine.Query("testdb", "cpu", nil, 0, time.Now().UnixNano(), []string{"usage"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(result.Series) != 0 {
		t.Errorf("expected 0 series, got %d", len(result.Series))
	}
}

func BenchmarkEngineWrite(b *testing.B) {
	dir := b.TempDir()

	engine, _ := NewEngine(EngineConfig{
		DataDir:       dir,
		ShardDuration: 24 * time.Hour,
		MaxBufferSize: 100000,
	})
	defer engine.Close()

	engine.CreateDatabase("bench")

	point := &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"usage": NewFloatField(42.5)},
	}

	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point.Timestamp = now.Add(time.Duration(i) * time.Millisecond).UnixNano()
		engine.Write("bench", point)
	}
}
