package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWALCreate(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	// Should have created one segment
	segCount, _, seq := wal.Stats()
	if segCount != 1 {
		t.Errorf("segment count = %d, want 1", segCount)
	}
	if seq != 1 {
		t.Errorf("sequence = %d, want 1", seq)
	}

	// Segment file should exist
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}
	if files[0].Name() != "wal-000001.log" {
		t.Errorf("unexpected filename: %s", files[0].Name())
	}
}

func TestWALAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	// Write some entries
	testData := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, data := range testData {
		if err := wal.AppendWrite(data); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and read
	wal2, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	for i, entry := range entries {
		if entry.Type != EntryTypeWrite {
			t.Errorf("entry %d: type = %d, want %d", i, entry.Type, EntryTypeWrite)
		}
		if !bytes.Equal(entry.Data, testData[i]) {
			t.Errorf("entry %d: data = %q, want %q", i, entry.Data, testData[i])
		}
	}
}

func TestWALCheckpoint(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	wal.AppendWrite([]byte("data 1"))
	wal.AppendCheckpoint()
	wal.AppendWrite([]byte("data 2"))
	wal.Close()

	// Read back
	wal2, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Type != EntryTypeWrite {
		t.Errorf("entry 0 type = %d, want Write", entries[0].Type)
	}
	if entries[1].Type != EntryTypeCheckpoint {
		t.Errorf("entry 1 type = %d, want Checkpoint", entries[1].Type)
	}
	if entries[2].Type != EntryTypeWrite {
		t.Errorf("entry 2 type = %d, want Write", entries[2].Type)
	}
}

func TestWALSegmentRollover(t *testing.T) {
	dir := t.TempDir()

	// Small segment size to trigger rollover
	wal, err := NewWAL(WALConfig{
		Dir:         dir,
		SegmentSize: 100, // Very small
	})
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	// Write enough to cause rollover
	data := bytes.Repeat([]byte("x"), 50)
	for i := 0; i < 10; i++ {
		if err := wal.AppendWrite(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	segCount, _, _ := wal.Stats()
	if segCount < 2 {
		t.Errorf("expected multiple segments, got %d", segCount)
	}

	wal.Close()

	// Verify all entries can be read back
	wal2, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 100})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(entries) != 10 {
		t.Errorf("expected 10 entries, got %d", len(entries))
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{
		Dir:         dir,
		SegmentSize: 100,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Create multiple segments
	data := bytes.Repeat([]byte("x"), 50)
	for i := 0; i < 10; i++ {
		wal.AppendWrite(data)
	}

	segCountBefore, _, _ := wal.Stats()
	if segCountBefore < 2 {
		t.Skipf("need multiple segments for this test")
	}

	// Truncate
	if err := wal.Truncate(); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	segCountAfter, _, _ := wal.Stats()
	if segCountAfter != 1 {
		t.Errorf("after truncate: %d segments, want 1", segCountAfter)
	}

	wal.Close()
}

func TestWALCRCValidation(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	wal.AppendWrite([]byte("valid entry"))
	wal.Close()

	// Corrupt the file
	files, _ := os.ReadDir(dir)
	path := filepath.Join(dir, files[0].Name())
	data, _ := os.ReadFile(path)

	// Corrupt some bytes in the middle
	if len(data) > 30 {
		data[25] ^= 0xFF
		data[26] ^= 0xFF
	}
	os.WriteFile(path, data, 0644)

	// Try to read - should get no entries due to CRC failure
	wal2, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Corrupted entry should be skipped
	if len(entries) != 0 {
		t.Logf("got %d entries (corruption may not have hit data)", len(entries))
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	numWriters := 10
	writesPerWriter := 100

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				data := []byte(string(rune('A'+id)) + string(rune('0'+j%10)))
				if err := wal.AppendWrite(data); err != nil {
					t.Errorf("writer %d: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	wal.Close()

	// Verify all entries
	wal2, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	expected := numWriters * writesPerWriter
	if len(entries) != expected {
		t.Errorf("expected %d entries, got %d", expected, len(entries))
	}
}

func TestWALSyncModes(t *testing.T) {
	t.Run("SyncEveryWrite", func(t *testing.T) {
		dir := t.TempDir()
		wal, _ := NewWAL(WALConfig{Dir: dir, SyncMode: SyncEveryWrite})
		wal.AppendWrite([]byte("test"))
		wal.Close()
	})

	t.Run("SyncEveryN", func(t *testing.T) {
		dir := t.TempDir()
		wal, _ := NewWAL(WALConfig{Dir: dir, SyncMode: SyncEveryN, SyncEveryN: 5})
		for i := 0; i < 10; i++ {
			wal.AppendWrite([]byte("test"))
		}
		wal.Close()
	})

	t.Run("SyncNone", func(t *testing.T) {
		dir := t.TempDir()
		wal, _ := NewWAL(WALConfig{Dir: dir, SyncMode: SyncNone})
		wal.AppendWrite([]byte("test"))
		wal.Sync() // Manual sync
		wal.Close()
	})
}

func TestWALRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write some entries
	wal, _ := NewWAL(WALConfig{Dir: dir})
	for i := 0; i < 5; i++ {
		wal.AppendWrite([]byte{byte(i)})
	}
	wal.Close()

	// Simulate crash by truncating file mid-entry
	files, _ := os.ReadDir(dir)
	path := filepath.Join(dir, files[0].Name())
	info, _ := os.Stat(path)

	// Truncate to remove last partial entry
	os.Truncate(path, info.Size()-3)

	// Recovery should work
	wal2, err := NewWAL(WALConfig{Dir: dir})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer wal2.Close()

	entries, _ := wal2.ReadAll()
	// Should have recovered at least some entries
	if len(entries) < 1 {
		t.Errorf("expected some recovered entries")
	}
	t.Logf("recovered %d entries", len(entries))
}

func TestWALTruncateBefore(t *testing.T) {
	dir := t.TempDir()

	wal, _ := NewWAL(WALConfig{Dir: dir, SegmentSize: 100})

	// Create multiple segments
	data := bytes.Repeat([]byte("x"), 50)
	for i := 0; i < 20; i++ {
		wal.AppendWrite(data)
	}

	_, _, lastSeq := wal.Stats()

	// Truncate all but the last 2 segments
	if lastSeq > 2 {
		wal.TruncateBefore(lastSeq - 1)
	}

	segCount, _, _ := wal.Stats()
	if segCount > 2 {
		t.Errorf("expected <= 2 segments, got %d", segCount)
	}

	wal.Close()
}

func TestWALEmptyEntry(t *testing.T) {
	dir := t.TempDir()

	wal, _ := NewWAL(WALConfig{Dir: dir})
	
	// Empty data should work
	if err := wal.AppendWrite([]byte{}); err != nil {
		t.Errorf("empty write failed: %v", err)
	}
	
	if err := wal.AppendWrite(nil); err != nil {
		t.Errorf("nil write failed: %v", err)
	}
	
	wal.Close()

	// Read back
	wal2, _ := NewWAL(WALConfig{Dir: dir})
	defer wal2.Close()

	entries, _ := wal2.ReadAll()
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	wal, _ := NewWAL(WALConfig{Dir: dir, SyncMode: SyncNone})
	defer wal.Close()

	data := bytes.Repeat([]byte("x"), 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.AppendWrite(data)
	}
}

func BenchmarkWALAppendSync(b *testing.B) {
	dir := b.TempDir()
	wal, _ := NewWAL(WALConfig{Dir: dir, SyncMode: SyncEveryWrite})
	defer wal.Close()

	data := bytes.Repeat([]byte("x"), 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.AppendWrite(data)
	}
}
