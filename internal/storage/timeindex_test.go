package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTimeIndexAddAndLookup(t *testing.T) {
	idx := NewTimeIndex("")

	// Add some entries
	idx.AddEntry(1000, 0, 0, 0)
	idx.AddEntry(2000, 100, 1, 0)
	idx.AddEntry(3000, 200, 2, 0)

	// Exact lookup
	entry, exact := idx.Lookup(2000)
	if !exact {
		t.Error("expected exact match")
	}
	if entry.Timestamp != 2000 {
		t.Errorf("expected timestamp 2000, got %d", entry.Timestamp)
	}
	if entry.Offset != 100 {
		t.Errorf("expected offset 100, got %d", entry.Offset)
	}

	// Lookup before first entry
	entry, exact = idx.Lookup(500)
	if exact {
		t.Error("expected non-exact match")
	}
	if entry.Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", entry.Timestamp)
	}

	// Lookup between entries
	entry, exact = idx.Lookup(2500)
	if exact {
		t.Error("expected non-exact match")
	}
	if entry.Timestamp != 2000 {
		t.Errorf("expected timestamp 2000, got %d", entry.Timestamp)
	}

	// Lookup after last entry
	entry, exact = idx.Lookup(4000)
	if exact {
		t.Error("expected non-exact match")
	}
	if entry.Timestamp != 3000 {
		t.Errorf("expected timestamp 3000, got %d", entry.Timestamp)
	}
}

func TestTimeIndexLookupRange(t *testing.T) {
	idx := NewTimeIndex("")

	idx.AddEntry(1000, 0, 0, 0)
	idx.AddEntry(2000, 100, 1, 0)
	idx.AddEntry(3000, 200, 2, 0)
	idx.AddEntry(4000, 300, 3, 0)
	idx.AddEntry(5000, 400, 4, 0)

	// Range in middle
	start, end, found := idx.LookupRange(2500, 4500)
	if !found {
		t.Fatal("expected to find range")
	}
	if start.Timestamp != 2000 {
		t.Errorf("expected start timestamp 2000, got %d", start.Timestamp)
	}
	if end.Timestamp != 5000 {
		t.Errorf("expected end timestamp 5000, got %d", end.Timestamp)
	}

	// Range spanning all
	start, end, found = idx.LookupRange(500, 6000)
	if !found {
		t.Fatal("expected to find range")
	}
	if start.Timestamp != 1000 {
		t.Errorf("expected start timestamp 1000, got %d", start.Timestamp)
	}
	if end.Timestamp != 5000 {
		t.Errorf("expected end timestamp 5000, got %d", end.Timestamp)
	}
}

func TestTimeIndexEmptyLookup(t *testing.T) {
	idx := NewTimeIndex("")

	_, found := idx.Lookup(1000)
	if found {
		t.Error("expected not found on empty index")
	}

	_, _, found = idx.LookupRange(1000, 2000)
	if found {
		t.Error("expected not found on empty index")
	}
}

func TestTimeIndexStats(t *testing.T) {
	idx := NewTimeIndex("")

	idx.AddEntry(1000, 0, 0, 0)
	idx.UpdateStats(1500)
	idx.UpdateStats(2000)
	idx.AddEntry(3000, 100, 1, 0)

	min, max := idx.GetTimeRange()
	if min != 1000 {
		t.Errorf("expected min 1000, got %d", min)
	}
	if max != 3000 {
		t.Errorf("expected max 3000, got %d", max)
	}

	if idx.PointCount() != 4 {
		t.Errorf("expected 4 points, got %d", idx.PointCount())
	}

	if idx.EntryCount() != 2 {
		t.Errorf("expected 2 entries, got %d", idx.EntryCount())
	}
}

func TestTimeIndexSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tidx")

	// Create and save
	idx := NewTimeIndex(path)
	idx.AddEntry(1000, 0, 0, 0)
	idx.AddEntry(2000, 100, 1, 0)
	idx.AddEntry(3000, 200, 2, 0)
	idx.UpdateStats(4000)

	if err := idx.Save(); err != nil {
		t.Fatalf("save error: %v", err)
	}

	// Load into new index
	idx2 := NewTimeIndex(path)
	if err := idx2.Load(); err != nil {
		t.Fatalf("load error: %v", err)
	}

	// Verify
	if idx2.EntryCount() != 3 {
		t.Errorf("expected 3 entries, got %d", idx2.EntryCount())
	}

	min, max := idx2.GetTimeRange()
	if min != 1000 {
		t.Errorf("expected min 1000, got %d", min)
	}
	if max != 4000 {
		t.Errorf("expected max 4000, got %d", max)
	}

	entry, exact := idx2.Lookup(2000)
	if !exact {
		t.Error("expected exact match")
	}
	if entry.Offset != 100 {
		t.Errorf("expected offset 100, got %d", entry.Offset)
	}
}

func TestTimeIndexLoadNonExistent(t *testing.T) {
	idx := NewTimeIndex("/nonexistent/path.tidx")
	if err := idx.Load(); err != nil {
		t.Errorf("expected no error for non-existent file, got: %v", err)
	}
}

func TestTimeIndexBuilder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "builder.tidx")

	builder := NewTimeIndexBuilder(path)

	// Add many points (more than sparsity)
	for i := 0; i < 3500; i++ {
		builder.AddPoint(int64(i*1000), uint64(i*10), uint32(i/1000), uint32(i%1000))
	}

	idx := builder.Finish()

	// Should have 4 entries (at 1, 1001, 2001, 3001)
	if idx.EntryCount() != 4 {
		t.Errorf("expected 4 entries, got %d", idx.EntryCount())
	}

	// Point count should be full
	if idx.PointCount() != 3500 {
		t.Errorf("expected 3500 points, got %d", idx.PointCount())
	}

	// Save and verify
	if err := builder.Save(); err != nil {
		t.Fatalf("save error: %v", err)
	}

	_, err := os.Stat(path)
	if err != nil {
		t.Errorf("index file not created: %v", err)
	}
}

func TestTimeIndexLookupPrecision(t *testing.T) {
	idx := NewTimeIndex("")

	// Add entries at various nanosecond timestamps
	idx.AddEntry(1609459200000000000, 0, 0, 0)   // 2021-01-01 00:00:00
	idx.AddEntry(1609459260000000000, 100, 1, 0) // +1 minute
	idx.AddEntry(1609459320000000000, 200, 2, 0) // +2 minutes

	// Look up exact nanosecond timestamp
	entry, exact := idx.Lookup(1609459260000000000)
	if !exact {
		t.Error("expected exact match for nanosecond timestamp")
	}
	if entry.BlockNum != 1 {
		t.Errorf("expected block 1, got %d", entry.BlockNum)
	}
}
