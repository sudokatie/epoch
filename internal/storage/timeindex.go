package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// TimeIndex constants
const (
	TimeIndexMagic   = 0x54494458 // "TIDX"
	TimeIndexVersion = 1

	// Sparse index: one entry per N points
	TimeIndexSparsity = 1000
)

// TimeIndexEntry represents a sparse index entry
type TimeIndexEntry struct {
	// Timestamp at this index position
	Timestamp int64
	// File offset to the block containing this timestamp
	Offset uint64
	// Block number within the column file
	BlockNum uint32
	// Point number within the block
	PointNum uint32
}

// TimeIndex provides efficient timestamp-based lookups
// Uses sparse indexing (one entry per TimeIndexSparsity points)
type TimeIndex struct {
	mu sync.RWMutex

	// Path to the index file
	path string

	// Sparse index entries (sorted by timestamp)
	entries []TimeIndexEntry

	// Statistics
	minTime    int64
	maxTime    int64
	pointCount uint64
}

// NewTimeIndex creates a new time index
func NewTimeIndex(path string) *TimeIndex {
	return &TimeIndex{
		path:    path,
		entries: make([]TimeIndexEntry, 0),
	}
}

// AddEntry adds an entry to the sparse index
// Should be called every TimeIndexSparsity points
func (ti *TimeIndex) AddEntry(timestamp int64, offset uint64, blockNum, pointNum uint32) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	entry := TimeIndexEntry{
		Timestamp: timestamp,
		Offset:    offset,
		BlockNum:  blockNum,
		PointNum:  pointNum,
	}

	ti.entries = append(ti.entries, entry)
	ti.pointCount++

	if ti.minTime == 0 || timestamp < ti.minTime {
		ti.minTime = timestamp
	}
	if timestamp > ti.maxTime {
		ti.maxTime = timestamp
	}
}

// UpdateStats updates the index statistics without adding an entry
func (ti *TimeIndex) UpdateStats(timestamp int64) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	ti.pointCount++
	if ti.minTime == 0 || timestamp < ti.minTime {
		ti.minTime = timestamp
	}
	if timestamp > ti.maxTime {
		ti.maxTime = timestamp
	}
}

// Lookup finds the index entry at or before the given timestamp
// Returns the entry and whether an exact match was found
func (ti *TimeIndex) Lookup(timestamp int64) (TimeIndexEntry, bool) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return TimeIndexEntry{}, false
	}

	// Binary search for the entry at or before timestamp
	idx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp > timestamp
	})

	if idx == 0 {
		// All entries are after timestamp
		return ti.entries[0], false
	}

	// idx-1 is the last entry <= timestamp
	entry := ti.entries[idx-1]
	exact := entry.Timestamp == timestamp
	return entry, exact
}

// LookupRange finds entries covering a time range
// Returns the first entry at or before startTime and the last entry at or after endTime
func (ti *TimeIndex) LookupRange(startTime, endTime int64) (start, end TimeIndexEntry, found bool) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if len(ti.entries) == 0 {
		return TimeIndexEntry{}, TimeIndexEntry{}, false
	}

	// Find start entry (at or before startTime)
	startIdx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp > startTime
	})
	if startIdx > 0 {
		startIdx--
	}

	// Find end entry (at or after endTime)
	endIdx := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].Timestamp >= endTime
	})
	if endIdx >= len(ti.entries) {
		endIdx = len(ti.entries) - 1
	}

	return ti.entries[startIdx], ti.entries[endIdx], true
}

// GetTimeRange returns the min and max timestamps in the index
func (ti *TimeIndex) GetTimeRange() (min, max int64) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return ti.minTime, ti.maxTime
}

// PointCount returns the total number of points indexed
func (ti *TimeIndex) PointCount() uint64 {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return ti.pointCount
}

// EntryCount returns the number of sparse index entries
func (ti *TimeIndex) EntryCount() int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return len(ti.entries)
}

// Save persists the time index to disk
func (ti *TimeIndex) Save() error {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	file, err := os.Create(ti.path)
	if err != nil {
		return fmt.Errorf("create time index: %w", err)
	}
	defer file.Close()

	// Write header
	header := make([]byte, 48)
	binary.LittleEndian.PutUint32(header[0:4], TimeIndexMagic)
	binary.LittleEndian.PutUint16(header[4:6], TimeIndexVersion)
	binary.LittleEndian.PutUint64(header[8:16], ti.pointCount)
	binary.LittleEndian.PutUint64(header[16:24], uint64(ti.minTime))
	binary.LittleEndian.PutUint64(header[24:32], uint64(ti.maxTime))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(ti.entries)))

	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write entries
	entryBuf := make([]byte, 24)
	for _, e := range ti.entries {
		binary.LittleEndian.PutUint64(entryBuf[0:8], uint64(e.Timestamp))
		binary.LittleEndian.PutUint64(entryBuf[8:16], e.Offset)
		binary.LittleEndian.PutUint32(entryBuf[16:20], e.BlockNum)
		binary.LittleEndian.PutUint32(entryBuf[20:24], e.PointNum)

		if _, err := file.Write(entryBuf); err != nil {
			return fmt.Errorf("write entry: %w", err)
		}
	}

	return nil
}

// Load reads the time index from disk
func (ti *TimeIndex) Load() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	file, err := os.Open(ti.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No index file yet
		}
		return fmt.Errorf("open time index: %w", err)
	}
	defer file.Close()

	// Read header
	header := make([]byte, 48)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != TimeIndexMagic {
		return fmt.Errorf("invalid time index magic: %x", magic)
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version != TimeIndexVersion {
		return fmt.Errorf("unsupported time index version: %d", version)
	}

	ti.pointCount = binary.LittleEndian.Uint64(header[8:16])
	ti.minTime = int64(binary.LittleEndian.Uint64(header[16:24]))
	ti.maxTime = int64(binary.LittleEndian.Uint64(header[24:32]))
	entryCount := binary.LittleEndian.Uint32(header[32:36])

	// Read entries
	ti.entries = make([]TimeIndexEntry, 0, entryCount)
	entryBuf := make([]byte, 24)

	for i := uint32(0); i < entryCount; i++ {
		if _, err := io.ReadFull(file, entryBuf); err != nil {
			return fmt.Errorf("read entry %d: %w", i, err)
		}

		entry := TimeIndexEntry{
			Timestamp: int64(binary.LittleEndian.Uint64(entryBuf[0:8])),
			Offset:    binary.LittleEndian.Uint64(entryBuf[8:16]),
			BlockNum:  binary.LittleEndian.Uint32(entryBuf[16:20]),
			PointNum:  binary.LittleEndian.Uint32(entryBuf[20:24]),
		}
		ti.entries = append(ti.entries, entry)
	}

	return nil
}

// TimeIndexBuilder helps build time indices during column file writes
type TimeIndexBuilder struct {
	index      *TimeIndex
	pointsSeen uint64
}

// NewTimeIndexBuilder creates a new time index builder
func NewTimeIndexBuilder(path string) *TimeIndexBuilder {
	return &TimeIndexBuilder{
		index: NewTimeIndex(path),
	}
}

// AddPoint records a point, adding sparse index entries as needed
func (b *TimeIndexBuilder) AddPoint(timestamp int64, offset uint64, blockNum, pointNum uint32) {
	b.pointsSeen++

	// Add sparse entry every TimeIndexSparsity points
	if b.pointsSeen%TimeIndexSparsity == 1 {
		b.index.AddEntry(timestamp, offset, blockNum, pointNum)
	} else {
		b.index.UpdateStats(timestamp)
	}
}

// Finish finalizes and returns the time index
func (b *TimeIndexBuilder) Finish() *TimeIndex {
	return b.index
}

// Save persists the built index
func (b *TimeIndexBuilder) Save() error {
	return b.index.Save()
}
