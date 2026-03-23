package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestColumnFileCreateAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	// Create and write
	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	timestamps := []int64{1000000000, 1000000001, 1000000002}
	values := []float64{1.1, 2.2, 3.3}

	if err := cf.AppendBlock(timestamps, values); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Open and read
	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	ts, vals, err := cf2.ReadBlock(0)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}

	if len(ts) != 3 {
		t.Errorf("expected 3 timestamps, got %d", len(ts))
	}

	floats := vals.([]float64)
	if len(floats) != 3 {
		t.Errorf("expected 3 values, got %d", len(floats))
	}

	for i, want := range values {
		if floats[i] != want {
			t.Errorf("value %d: got %v, want %v", i, floats[i], want)
		}
	}
}

func TestColumnFileMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Write multiple blocks
	for i := 0; i < 5; i++ {
		base := int64(i * 1000000)
		timestamps := []int64{base + 1, base + 2, base + 3}
		values := []float64{float64(i), float64(i) + 0.1, float64(i) + 0.2}
		if err := cf.AppendBlock(timestamps, values); err != nil {
			t.Fatalf("append block %d: %v", i, err)
		}
	}

	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Open and verify
	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	pointCount, blockCount, minTime, maxTime := cf2.Stats()
	if pointCount != 15 {
		t.Errorf("pointCount = %d, want 15", pointCount)
	}
	if blockCount != 5 {
		t.Errorf("blockCount = %d, want 5", blockCount)
	}
	if minTime != 1 {
		t.Errorf("minTime = %d, want 1", minTime)
	}
	if maxTime != 4000003 {
		t.Errorf("maxTime = %d, want 4000003", maxTime)
	}

	// Read each block
	for i := 0; i < 5; i++ {
		ts, vals, err := cf2.ReadBlock(i)
		if err != nil {
			t.Fatalf("read block %d: %v", i, err)
		}
		if len(ts) != 3 {
			t.Errorf("block %d: got %d timestamps, want 3", i, len(ts))
		}
		floats := vals.([]float64)
		if floats[0] != float64(i) {
			t.Errorf("block %d first value: got %v, want %v", i, floats[0], float64(i))
		}
	}
}

func TestColumnFileTimeRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Write blocks spanning a range
	for i := 0; i < 10; i++ {
		base := int64(i * 100)
		timestamps := []int64{base + 10, base + 20, base + 30}
		values := []float64{float64(i), float64(i) + 0.1, float64(i) + 0.2}
		if err := cf.AppendBlock(timestamps, values); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	// Query a time range that spans multiple blocks
	ts, vals, err := cf2.ReadTimeRange(250, 450)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}

	floats := vals.([]float64)

	// Should get points from blocks 2, 3, 4 (timestamps 210-230, 310-330, 410-430)
	// Within range 250-450: 310, 320, 330, 410, 420, 430
	expectedCount := 6
	if len(ts) != expectedCount {
		t.Errorf("got %d points, want %d", len(ts), expectedCount)
	}
	if len(floats) != len(ts) {
		t.Errorf("value count %d doesn't match timestamp count %d", len(floats), len(ts))
	}
}

func TestColumnFileIntegerType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeInteger)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	timestamps := []int64{1000, 2000, 3000}
	values := []int64{100, 200, 300}

	if err := cf.AppendBlock(timestamps, values); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	ts, vals, err := cf2.ReadBlock(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	ints := vals.([]int64)
	for i, want := range values {
		if ts[i] != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts[i], timestamps[i])
		}
		if ints[i] != want {
			t.Errorf("value %d: got %d, want %d", i, ints[i], want)
		}
	}
}

func TestColumnFileStringType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeString)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	timestamps := []int64{1000, 2000, 3000}
	values := []string{"hello", "world", "test"}

	if err := cf.AppendBlock(timestamps, values); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	ts, vals, err := cf2.ReadBlock(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	strings := vals.([]string)
	for i, want := range values {
		if ts[i] != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts[i], timestamps[i])
		}
		if strings[i] != want {
			t.Errorf("value %d: got %q, want %q", i, strings[i], want)
		}
	}
}

func TestColumnFileBooleanType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeBoolean)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	timestamps := []int64{1000, 2000, 3000, 4000}
	values := []bool{true, false, true, false}

	if err := cf.AppendBlock(timestamps, values); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	ts, vals, err := cf2.ReadBlock(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	bools := vals.([]bool)
	for i, want := range values {
		if ts[i] != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts[i], timestamps[i])
		}
		if bools[i] != want {
			t.Errorf("value %d: got %v, want %v", i, bools[i], want)
		}
	}
}

func TestColumnFileEmptyBlockError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer cf.Close()

	err = cf.AppendBlock([]int64{}, []float64{})
	if err == nil {
		t.Error("expected error for empty block")
	}
}

func TestColumnFileInvalidMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	// Write garbage
	if err := os.WriteFile(path, []byte("not a column file with enough bytes for header"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := OpenColumnFile(path)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestColumnFileBlockOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.col")

	cf, err := CreateColumnFile(path, FieldTypeFloat)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := cf.AppendBlock([]int64{1}, []float64{1.0}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cf2, err := OpenColumnFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cf2.Close()

	_, _, err = cf2.ReadBlock(5)
	if err == nil {
		t.Error("expected error for out of range block")
	}
}

func BenchmarkColumnFileWrite(b *testing.B) {
	dir := b.TempDir()

	timestamps := make([]int64, 1000)
	values := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		timestamps[i] = int64(i * 1000000000)
		values[i] = float64(i) * 0.1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, "bench.col")
		cf, _ := CreateColumnFile(path, FieldTypeFloat)
		cf.AppendBlock(timestamps, values)
		cf.Close()
		os.Remove(path)
	}
}

func BenchmarkColumnFileRead(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.col")

	timestamps := make([]int64, 1000)
	values := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		timestamps[i] = int64(i * 1000000000)
		values[i] = float64(i) * 0.1
	}

	cf, _ := CreateColumnFile(path, FieldTypeFloat)
	cf.AppendBlock(timestamps, values)
	cf.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cf, _ := OpenColumnFile(path)
		cf.ReadBlock(0)
		cf.Close()
	}
}
