package storage

import (
	"testing"
	"time"
)

func TestShardCreate(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, err := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		Database:  "testdb",
		StartTime: start,
		EndTime:   end,
	})
	if err != nil {
		t.Fatalf("create shard: %v", err)
	}
	defer shard.Close()

	info := shard.Info()
	if info.ID != 1 {
		t.Errorf("ID = %d, want 1", info.ID)
	}
	if info.Database != "testdb" {
		t.Errorf("Database = %q, want testdb", info.Database)
	}
	if shard.State() != ShardStateHot {
		t.Errorf("state = %v, want Hot", shard.State())
	}
}

func TestShardWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, err := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		Database:  "testdb",
		StartTime: start,
		EndTime:   end,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Write some points
	for i := 0; i < 10; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": "server1"},
			Fields: Fields{
				"usage": NewFloatField(float64(i) * 10),
			},
			Timestamp: start.Add(time.Duration(i) * time.Hour).UnixNano(),
		}
		if err := shard.Write(point); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Flush to disk
	if err := shard.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Read back
	seriesKey := "cpu,host=server1"
	ts, vals, err := shard.Read(seriesKey, "usage", start.UnixNano(), end.UnixNano())
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(ts) != 10 {
		t.Errorf("got %d points, want 10", len(ts))
	}

	floats := vals.([]float64)
	if floats[0] != 0 {
		t.Errorf("first value = %v, want 0", floats[0])
	}
	if floats[9] != 90 {
		t.Errorf("last value = %v, want 90", floats[9])
	}

	shard.Close()
}

func TestShardTimeRangeValidation(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})
	defer shard.Close()

	// Point before range should fail
	beforePoint := &DataPoint{
		Measurement: "cpu",
		Fields:      Fields{"v": NewFloatField(1)},
		Timestamp:   start.Add(-time.Hour).UnixNano(),
	}
	if err := shard.Write(beforePoint); err == nil {
		t.Error("expected error for point before range")
	}

	// Point after range should fail
	afterPoint := &DataPoint{
		Measurement: "cpu",
		Fields:      Fields{"v": NewFloatField(1)},
		Timestamp:   end.Add(time.Hour).UnixNano(),
	}
	if err := shard.Write(afterPoint); err == nil {
		t.Error("expected error for point after range")
	}

	// Point within range should succeed
	validPoint := &DataPoint{
		Measurement: "cpu",
		Fields:      Fields{"v": NewFloatField(1)},
		Timestamp:   start.Add(time.Hour).UnixNano(),
	}
	if err := shard.Write(validPoint); err != nil {
		t.Errorf("write valid point: %v", err)
	}
}

func TestShardAutoFlush(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:           dir,
		ID:            1,
		StartTime:     start,
		EndTime:       end,
		MaxBufferSize: 10, // Small buffer to trigger auto-flush
	})
	defer shard.Close()

	// Write enough points to trigger auto-flush
	for i := 0; i < 15; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Fields:      Fields{"v": NewFloatField(float64(i))},
			Timestamp:   start.Add(time.Duration(i) * time.Minute).UnixNano(),
		}
		shard.Write(point)
	}

	// Should have been flushed
	buffered, _, _ := shard.Stats()
	if buffered >= 15 {
		t.Errorf("expected some points flushed, but %d still buffered", buffered)
	}
}

func TestShardReadBuffer(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})
	defer shard.Close()

	// Write but don't flush
	point := &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"usage": NewFloatField(42.5)},
		Timestamp:   start.Add(time.Hour).UnixNano(),
	}
	shard.Write(point)

	// Read from buffer
	seriesKey := "cpu,host=server1"
	ts, fields, ok := shard.ReadBuffer(seriesKey)
	if !ok {
		t.Fatal("buffer not found")
	}
	if len(ts) != 1 {
		t.Errorf("expected 1 timestamp, got %d", len(ts))
	}
	if len(fields["usage"]) != 1 {
		t.Errorf("expected 1 value, got %d", len(fields["usage"]))
	}
}

func TestShardSetReadOnly(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})

	// Write some data
	shard.Write(&DataPoint{
		Measurement: "cpu",
		Fields:      Fields{"v": NewFloatField(1)},
		Timestamp:   start.Add(time.Hour).UnixNano(),
	})

	// Set read-only
	if err := shard.SetReadOnly(); err != nil {
		t.Fatalf("set read-only: %v", err)
	}

	if shard.State() != ShardStateCold {
		t.Errorf("state = %v, want Cold", shard.State())
	}

	// Buffer should be flushed
	buffered, _, _ := shard.Stats()
	if buffered != 0 {
		t.Errorf("buffer not flushed: %d points", buffered)
	}

	shard.Close()
}

func TestShardMultipleFields(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})

	// Write point with multiple fields
	point := &DataPoint{
		Measurement: "system",
		Tags:        Tags{"host": "server1"},
		Fields: Fields{
			"cpu":    NewFloatField(45.2),
			"memory": NewIntField(8192),
			"status": NewStringField("running"),
			"active": NewBoolField(true),
		},
		Timestamp: start.Add(time.Hour).UnixNano(),
	}
	shard.Write(point)
	shard.Flush()

	seriesKey := "system,host=server1"

	// Read each field
	_, cpuVals, _ := shard.Read(seriesKey, "cpu", start.UnixNano(), end.UnixNano())
	if cpuVals.([]float64)[0] != 45.2 {
		t.Errorf("cpu = %v", cpuVals)
	}

	_, memVals, _ := shard.Read(seriesKey, "memory", start.UnixNano(), end.UnixNano())
	if memVals.([]int64)[0] != 8192 {
		t.Errorf("memory = %v", memVals)
	}

	_, statusVals, _ := shard.Read(seriesKey, "status", start.UnixNano(), end.UnixNano())
	if statusVals.([]string)[0] != "running" {
		t.Errorf("status = %v", statusVals)
	}

	_, activeVals, _ := shard.Read(seriesKey, "active", start.UnixNano(), end.UnixNano())
	if activeVals.([]bool)[0] != true {
		t.Errorf("active = %v", activeVals)
	}

	shard.Close()
}

func TestShardMultipleSeries(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})

	// Write to multiple series
	hosts := []string{"server1", "server2", "server3"}
	for i, host := range hosts {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": host},
			Fields:      Fields{"usage": NewFloatField(float64(i * 10))},
			Timestamp:   start.Add(time.Hour).UnixNano(),
		}
		shard.Write(point)
	}
	shard.Flush()

	// Read each series
	for i, host := range hosts {
		seriesKey := "cpu,host=" + host
		_, vals, err := shard.Read(seriesKey, "usage", start.UnixNano(), end.UnixNano())
		if err != nil {
			t.Errorf("read %s: %v", host, err)
			continue
		}
		floats := vals.([]float64)
		if floats[0] != float64(i*10) {
			t.Errorf("%s usage = %v, want %d", host, floats[0], i*10)
		}
	}

	shard.Close()
}

func TestShardStats(t *testing.T) {
	dir := t.TempDir()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:       dir,
		ID:        1,
		StartTime: start,
		EndTime:   end,
	})

	// Write some data
	for i := 0; i < 5; i++ {
		shard.Write(&DataPoint{
			Measurement: "cpu",
			Fields:      Fields{"v": NewFloatField(1)},
			Timestamp:   start.Add(time.Duration(i) * time.Hour).UnixNano(),
		})
	}

	buffered, cols, walSegs := shard.Stats()
	if buffered != 5 {
		t.Errorf("buffered = %d, want 5", buffered)
	}
	if cols != 0 {
		t.Errorf("columns = %d, want 0 (not flushed)", cols)
	}
	if walSegs < 1 {
		t.Errorf("walSegments = %d, want >= 1", walSegs)
	}

	shard.Flush()

	buffered2, cols2, _ := shard.Stats()
	if buffered2 != 0 {
		t.Errorf("after flush: buffered = %d, want 0", buffered2)
	}
	if cols2 != 1 {
		t.Errorf("after flush: columns = %d, want 1", cols2)
	}

	shard.Close()
}

func BenchmarkShardWrite(b *testing.B) {
	dir := b.TempDir()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	shard, _ := NewShard(ShardConfig{
		Dir:           dir,
		ID:            1,
		StartTime:     start,
		EndTime:       end,
		MaxBufferSize: 100000,
	})
	defer shard.Close()

	point := &DataPoint{
		Measurement: "cpu",
		Tags:        Tags{"host": "server1"},
		Fields:      Fields{"usage": NewFloatField(45.2)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point.Timestamp = start.Add(time.Duration(i) * time.Second).UnixNano()
		shard.Write(point)
	}
}
