package storage

import (
	"path/filepath"
	"testing"
)

func TestTagIndexAddSeries(t *testing.T) {
	idx := NewTagIndex()

	id1 := idx.AddSeries("cpu", Tags{"host": "server1", "region": "us-west"})
	if id1 != 1 {
		t.Errorf("first ID = %d, want 1", id1)
	}

	id2 := idx.AddSeries("cpu", Tags{"host": "server2", "region": "us-west"})
	if id2 != 2 {
		t.Errorf("second ID = %d, want 2", id2)
	}

	// Same series should return same ID
	id1Again := idx.AddSeries("cpu", Tags{"host": "server1", "region": "us-west"})
	if id1Again != id1 {
		t.Errorf("duplicate series ID = %d, want %d", id1Again, id1)
	}

	if idx.SeriesCount() != 2 {
		t.Errorf("series count = %d, want 2", idx.SeriesCount())
	}
}

func TestTagIndexQuery(t *testing.T) {
	idx := NewTagIndex()

	// Add some series
	idx.AddSeries("cpu", Tags{"host": "server1", "region": "us-west"})
	idx.AddSeries("cpu", Tags{"host": "server2", "region": "us-west"})
	idx.AddSeries("cpu", Tags{"host": "server3", "region": "us-east"})
	idx.AddSeries("memory", Tags{"host": "server1", "region": "us-west"})

	// Query by single tag
	results := idx.Query(map[string]string{"host": "server1"})
	if len(results) != 2 {
		t.Errorf("host=server1 returned %d results, want 2", len(results))
	}

	// Query by region
	results = idx.Query(map[string]string{"region": "us-west"})
	if len(results) != 3 {
		t.Errorf("region=us-west returned %d results, want 3", len(results))
	}

	// Query by multiple tags (AND)
	results = idx.Query(map[string]string{"host": "server1", "region": "us-west"})
	if len(results) != 2 {
		t.Errorf("host=server1 AND region=us-west returned %d results, want 2", len(results))
	}

	// Query with no matches
	results = idx.Query(map[string]string{"host": "server99"})
	if len(results) != 0 {
		t.Errorf("non-existent host returned %d results, want 0", len(results))
	}

	// Empty filter returns all
	results = idx.Query(nil)
	if len(results) != 4 {
		t.Errorf("empty filter returned %d results, want 4", len(results))
	}
}

func TestTagIndexQueryByMeasurement(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})
	idx.AddSeries("cpu", Tags{"host": "server2"})
	idx.AddSeries("memory", Tags{"host": "server1"})

	results := idx.QueryByMeasurement("cpu")
	if len(results) != 2 {
		t.Errorf("cpu measurement returned %d results, want 2", len(results))
	}

	results = idx.QueryByMeasurement("memory")
	if len(results) != 1 {
		t.Errorf("memory measurement returned %d results, want 1", len(results))
	}
}

func TestTagIndexQueryOr(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})
	idx.AddSeries("cpu", Tags{"host": "server2"})
	idx.AddSeries("cpu", Tags{"host": "server3"})

	// OR query
	results := idx.QueryOr(map[string]string{
		"host": "server1",
	})
	if len(results) != 1 {
		t.Errorf("OR query returned %d results, want 1", len(results))
	}
}

func TestTagIndexGetSeries(t *testing.T) {
	idx := NewTagIndex()

	id := idx.AddSeries("cpu", Tags{"host": "server1"})
	entry := idx.GetSeries(id)

	if entry == nil {
		t.Fatal("GetSeries returned nil")
	}
	if entry.Measurement != "cpu" {
		t.Errorf("measurement = %q, want cpu", entry.Measurement)
	}
	if entry.Tags["host"] != "server1" {
		t.Errorf("host tag = %q, want server1", entry.Tags["host"])
	}
}

func TestTagIndexGetSeriesID(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})

	id := idx.GetSeriesID("cpu,host=server1")
	if id != 1 {
		t.Errorf("GetSeriesID = %d, want 1", id)
	}

	id = idx.GetSeriesID("nonexistent")
	if id != 0 {
		t.Errorf("GetSeriesID for nonexistent = %d, want 0", id)
	}
}

func TestTagIndexGetTagKeys(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1", "region": "us-west", "dc": "dc1"})

	keys := idx.GetTagKeys()
	if len(keys) != 3 {
		t.Errorf("got %d keys, want 3", len(keys))
	}

	// Should be sorted
	expected := []string{"dc", "host", "region"}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("key %d = %q, want %q", i, k, expected[i])
		}
	}
}

func TestTagIndexGetTagValues(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})
	idx.AddSeries("cpu", Tags{"host": "server2"})
	idx.AddSeries("cpu", Tags{"host": "server3"})

	values := idx.GetTagValues("host")
	if len(values) != 3 {
		t.Errorf("got %d values, want 3", len(values))
	}

	// Should be sorted
	expected := []string{"server1", "server2", "server3"}
	for i, v := range values {
		if v != expected[i] {
			t.Errorf("value %d = %q, want %q", i, v, expected[i])
		}
	}
}

func TestTagIndexGetMeasurements(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})
	idx.AddSeries("memory", Tags{"host": "server1"})
	idx.AddSeries("disk", Tags{"host": "server1"})

	measurements := idx.GetMeasurements()
	if len(measurements) != 3 {
		t.Errorf("got %d measurements, want 3", len(measurements))
	}

	expected := []string{"cpu", "disk", "memory"}
	for i, m := range measurements {
		if m != expected[i] {
			t.Errorf("measurement %d = %q, want %q", i, m, expected[i])
		}
	}
}

func TestTagIndexBloomFilter(t *testing.T) {
	idx := NewTagIndex()

	idx.AddSeries("cpu", Tags{"host": "server1"})
	idx.AddSeries("cpu", Tags{"host": "server2"})

	// Query with non-existent value should use bloom filter
	results := idx.Query(map[string]string{"host": "server99"})
	if len(results) != 0 {
		t.Errorf("bloom filter should have caught this")
	}
}

func TestTagIndexSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.idx")

	// Create and populate index
	idx := NewTagIndex()
	idx.AddSeries("cpu", Tags{"host": "server1", "region": "us-west"})
	idx.AddSeries("cpu", Tags{"host": "server2", "region": "us-east"})
	idx.AddSeries("memory", Tags{"host": "server1"})

	// Save
	if err := idx.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Load into new index
	idx2 := NewTagIndex()
	if err := idx2.Load(path); err != nil {
		t.Fatalf("load: %v", err)
	}

	// Verify
	if idx2.SeriesCount() != 3 {
		t.Errorf("loaded series count = %d, want 3", idx2.SeriesCount())
	}

	// Query should work
	results := idx2.Query(map[string]string{"host": "server1"})
	if len(results) != 2 {
		t.Errorf("query after load returned %d results, want 2", len(results))
	}

	// Get series should work
	entry := idx2.GetSeries(1)
	if entry == nil || entry.Measurement != "cpu" {
		t.Error("GetSeries failed after load")
	}

	// Tag values should be preserved
	values := idx2.GetTagValues("host")
	if len(values) != 2 {
		t.Errorf("tag values count = %d, want 2", len(values))
	}
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter()

	bf.Add("hello")
	bf.Add("world")

	if !bf.MayContain("hello") {
		t.Error("bloom filter should contain hello")
	}
	if !bf.MayContain("world") {
		t.Error("bloom filter should contain world")
	}

	// False positives are possible but unlikely for this simple test
	falsePositives := 0
	for i := 0; i < 100; i++ {
		if bf.MayContain(string(rune('A' + i))) {
			falsePositives++
		}
	}
	// With 3 hash functions and 8KB filter, false positive rate should be very low
	if falsePositives > 10 {
		t.Errorf("too many false positives: %d", falsePositives)
	}
}

func TestIntersectSorted(t *testing.T) {
	tests := []struct {
		a, b   []uint64
		expect []uint64
	}{
		{[]uint64{1, 2, 3}, []uint64{2, 3, 4}, []uint64{2, 3}},
		{[]uint64{1, 2, 3}, []uint64{4, 5, 6}, []uint64{}},
		{[]uint64{1, 3, 5}, []uint64{1, 2, 3, 4, 5}, []uint64{1, 3, 5}},
		{[]uint64{}, []uint64{1, 2, 3}, []uint64{}},
		{[]uint64{1}, []uint64{1}, []uint64{1}},
	}

	for _, tt := range tests {
		result := intersectSorted(tt.a, tt.b)
		if len(result) != len(tt.expect) {
			t.Errorf("intersect(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expect)
			continue
		}
		for i := range result {
			if result[i] != tt.expect[i] {
				t.Errorf("intersect(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expect)
				break
			}
		}
	}
}

func TestTagIndexNoTags(t *testing.T) {
	idx := NewTagIndex()

	// Series with no tags
	id := idx.AddSeries("cpu", nil)
	if id != 1 {
		t.Errorf("ID = %d, want 1", id)
	}

	results := idx.QueryByMeasurement("cpu")
	if len(results) != 1 {
		t.Errorf("query returned %d results, want 1", len(results))
	}
}

func BenchmarkTagIndexAdd(b *testing.B) {
	idx := NewTagIndex()
	tags := Tags{"host": "server1", "region": "us-west"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.AddSeries("cpu", tags)
	}
}

func BenchmarkTagIndexQuery(b *testing.B) {
	idx := NewTagIndex()

	// Add many series
	for i := 0; i < 10000; i++ {
		idx.AddSeries("cpu", Tags{
			"host":   string(rune('a' + (i % 26))),
			"region": string(rune('A' + (i % 10))),
		})
	}

	filter := map[string]string{"host": "a"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Query(filter)
	}
}
