package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"sync"
)

// Index constants
const (
	IndexMagic   = 0x49445845 // "IDXE"
	IndexVersion = 1

	// Bloom filter parameters
	BloomFilterSize = 1024 * 8 // 1KB per filter, 8 bits per byte
	BloomHashCount  = 3
)

// TagIndex is an inverted index mapping tag key-value pairs to series IDs
type TagIndex struct {
	mu sync.RWMutex

	// Primary index: tag key -> tag value -> sorted series IDs
	index map[string]map[string][]uint64

	// Series metadata: series ID -> series info
	series map[uint64]*SeriesEntry

	// Reverse lookup: series key -> series ID
	keyToID map[string]uint64

	// Bloom filters for quick non-existence checks
	blooms map[string]*BloomFilter

	// Next series ID
	nextID uint64
}

// SeriesEntry holds metadata for a series
type SeriesEntry struct {
	ID          uint64
	Measurement string
	Tags        Tags
	Key         string
}

// BloomFilter is a simple bloom filter for quick lookups
type BloomFilter struct {
	bits []byte
}

// NewTagIndex creates a new tag index
func NewTagIndex() *TagIndex {
	return &TagIndex{
		index:   make(map[string]map[string][]uint64),
		series:  make(map[uint64]*SeriesEntry),
		keyToID: make(map[string]uint64),
		blooms:  make(map[string]*BloomFilter),
		nextID:  1,
	}
}

// AddSeries adds a series to the index and returns its ID
func (idx *TagIndex) AddSeries(measurement string, tags Tags) uint64 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Build series key
	key := measurement
	if len(tags) > 0 {
		key = measurement + "," + tags.String()
	}

	// Check if already exists
	if id, ok := idx.keyToID[key]; ok {
		return id
	}

	// Assign new ID
	id := idx.nextID
	idx.nextID++

	// Create series entry
	entry := &SeriesEntry{
		ID:          id,
		Measurement: measurement,
		Tags:        tags,
		Key:         key,
	}
	idx.series[id] = entry
	idx.keyToID[key] = id

	// Index by measurement (special tag key)
	idx.addToIndex("_measurement", measurement, id)

	// Index by each tag
	for k, v := range tags {
		idx.addToIndex(k, v, id)
	}

	return id
}

// addToIndex adds a series ID to the index for a tag key-value pair
func (idx *TagIndex) addToIndex(key, value string, seriesID uint64) {
	if idx.index[key] == nil {
		idx.index[key] = make(map[string][]uint64)
		idx.blooms[key] = NewBloomFilter()
	}

	// Add to bloom filter
	idx.blooms[key].Add(value)

	// Add to inverted index (maintain sorted order)
	ids := idx.index[key][value]
	
	// Binary search for insert position
	pos := sort.Search(len(ids), func(i int) bool {
		return ids[i] >= seriesID
	})

	// Check if already present
	if pos < len(ids) && ids[pos] == seriesID {
		return
	}

	// Insert at position
	ids = append(ids, 0)
	copy(ids[pos+1:], ids[pos:])
	ids[pos] = seriesID
	idx.index[key][value] = ids
}

// GetSeriesID returns the series ID for a given key, or 0 if not found
func (idx *TagIndex) GetSeriesID(key string) uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.keyToID[key]
}

// GetSeries returns the series entry for a given ID
func (idx *TagIndex) GetSeries(id uint64) *SeriesEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.series[id]
}

// Query returns series IDs matching the given tag filters
// Filters are ANDed together
func (idx *TagIndex) Query(filters map[string]string) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(filters) == 0 {
		// Return all series
		result := make([]uint64, 0, len(idx.series))
		for id := range idx.series {
			result = append(result, id)
		}
		sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
		return result
	}

	var result []uint64
	first := true

	for key, value := range filters {
		// Quick bloom filter check
		if bloom, ok := idx.blooms[key]; ok {
			if !bloom.MayContain(value) {
				return nil // Definitely not present
			}
		}

		// Get matching series IDs
		if idx.index[key] == nil {
			return nil
		}
		ids := idx.index[key][value]
		if len(ids) == 0 {
			return nil
		}

		if first {
			result = make([]uint64, len(ids))
			copy(result, ids)
			first = false
		} else {
			// Intersect with previous results
			result = intersectSorted(result, ids)
			if len(result) == 0 {
				return nil
			}
		}
	}

	return result
}

// QueryOr returns series IDs matching ANY of the given tag filters
func (idx *TagIndex) QueryOr(filters map[string]string) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(filters) == 0 {
		return nil
	}

	seen := make(map[uint64]bool)
	var result []uint64

	for key, value := range filters {
		if idx.index[key] == nil {
			continue
		}
		ids := idx.index[key][value]
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				result = append(result, id)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// QueryByMeasurement returns all series for a measurement
func (idx *TagIndex) QueryByMeasurement(measurement string) []uint64 {
	return idx.Query(map[string]string{"_measurement": measurement})
}

// GetTagValues returns all values for a given tag key
func (idx *TagIndex) GetTagValues(key string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.index[key] == nil {
		return nil
	}

	values := make([]string, 0, len(idx.index[key]))
	for v := range idx.index[key] {
		values = append(values, v)
	}
	sort.Strings(values)
	return values
}

// GetTagKeys returns all tag keys in the index
func (idx *TagIndex) GetTagKeys() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := make([]string, 0, len(idx.index))
	for k := range idx.index {
		if k != "_measurement" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// GetMeasurements returns all measurements in the index
func (idx *TagIndex) GetMeasurements() []string {
	return idx.GetTagValues("_measurement")
}

// SeriesCount returns the number of series in the index
func (idx *TagIndex) SeriesCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.series)
}

// Save persists the index to disk
func (idx *TagIndex) Save(path string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], IndexMagic)
	binary.LittleEndian.PutUint16(header[4:6], IndexVersion)
	binary.LittleEndian.PutUint64(header[8:16], idx.nextID)
	if _, err := file.Write(header); err != nil {
		return err
	}

	// Write series count and entries
	if err := writeUint32(file, uint32(len(idx.series))); err != nil {
		return err
	}

	for _, entry := range idx.series {
		if err := writeSeriesEntry(file, entry); err != nil {
			return err
		}
	}

	// Write index
	if err := writeUint32(file, uint32(len(idx.index))); err != nil {
		return err
	}

	for key, valueMap := range idx.index {
		if err := writeString(file, key); err != nil {
			return err
		}
		if err := writeUint32(file, uint32(len(valueMap))); err != nil {
			return err
		}
		for value, ids := range valueMap {
			if err := writeString(file, value); err != nil {
				return err
			}
			if err := writeUint64Slice(file, ids); err != nil {
				return err
			}
		}
	}

	return nil
}

// Load loads the index from disk
func (idx *TagIndex) Load(path string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read header
	header := make([]byte, 16)
	if _, err := io.ReadFull(file, header); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != IndexMagic {
		return fmt.Errorf("invalid index magic: %x", magic)
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version != IndexVersion {
		return fmt.Errorf("unsupported index version: %d", version)
	}

	idx.nextID = binary.LittleEndian.Uint64(header[8:16])

	// Read series
	seriesCount, err := readUint32(file)
	if err != nil {
		return err
	}

	idx.series = make(map[uint64]*SeriesEntry, seriesCount)
	idx.keyToID = make(map[string]uint64, seriesCount)

	for i := uint32(0); i < seriesCount; i++ {
		entry, err := readSeriesEntry(file)
		if err != nil {
			return err
		}
		idx.series[entry.ID] = entry
		idx.keyToID[entry.Key] = entry.ID
	}

	// Read index
	keyCount, err := readUint32(file)
	if err != nil {
		return err
	}

	idx.index = make(map[string]map[string][]uint64, keyCount)
	idx.blooms = make(map[string]*BloomFilter, keyCount)

	for i := uint32(0); i < keyCount; i++ {
		key, err := readString(file)
		if err != nil {
			return err
		}

		valueCount, err := readUint32(file)
		if err != nil {
			return err
		}

		idx.index[key] = make(map[string][]uint64, valueCount)
		idx.blooms[key] = NewBloomFilter()

		for j := uint32(0); j < valueCount; j++ {
			value, err := readString(file)
			if err != nil {
				return err
			}
			ids, err := readUint64Slice(file)
			if err != nil {
				return err
			}
			idx.index[key][value] = ids
			idx.blooms[key].Add(value)
		}
	}

	return nil
}

// Helper functions for intersecting sorted slices
func intersectSorted(a, b []uint64) []uint64 {
	result := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Bloom filter implementation
func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		bits: make([]byte, BloomFilterSize/8),
	}
}

func (bf *BloomFilter) Add(value string) {
	for i := 0; i < BloomHashCount; i++ {
		h := bf.hash(value, i)
		byteIdx := h / 8
		bitIdx := h % 8
		bf.bits[byteIdx] |= 1 << bitIdx
	}
}

func (bf *BloomFilter) MayContain(value string) bool {
	for i := 0; i < BloomHashCount; i++ {
		h := bf.hash(value, i)
		byteIdx := h / 8
		bitIdx := h % 8
		if bf.bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(value string, seed int) uint32 {
	h := fnv.New32a()
	h.Write([]byte{byte(seed)})
	h.Write([]byte(value))
	return h.Sum32() % BloomFilterSize
}

// Serialization helpers
func writeUint32(w io.Writer, v uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	_, err := w.Write(buf)
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func writeString(w io.Writer, s string) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func readString(r io.Reader) (string, error) {
	length, err := readUint32(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeUint64Slice(w io.Writer, s []uint64) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	buf := make([]byte, 8)
	for _, v := range s {
		binary.LittleEndian.PutUint64(buf, v)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func readUint64Slice(r io.Reader) ([]uint64, error) {
	length, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	result := make([]uint64, length)
	buf := make([]byte, 8)
	for i := uint32(0); i < length; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		result[i] = binary.LittleEndian.Uint64(buf)
	}
	return result, nil
}

func writeSeriesEntry(w io.Writer, e *SeriesEntry) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, e.ID)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	if err := writeString(w, e.Measurement); err != nil {
		return err
	}
	if err := writeString(w, e.Key); err != nil {
		return err
	}
	// Write tags
	if err := writeUint32(w, uint32(len(e.Tags))); err != nil {
		return err
	}
	for k, v := range e.Tags {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeString(w, v); err != nil {
			return err
		}
	}
	return nil
}

func readSeriesEntry(r io.Reader) (*SeriesEntry, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	id := binary.LittleEndian.Uint64(buf)

	measurement, err := readString(r)
	if err != nil {
		return nil, err
	}

	key, err := readString(r)
	if err != nil {
		return nil, err
	}

	tagCount, err := readUint32(r)
	if err != nil {
		return nil, err
	}

	tags := make(Tags, tagCount)
	for i := uint32(0); i < tagCount; i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readString(r)
		if err != nil {
			return nil, err
		}
		tags[k] = v
	}

	return &SeriesEntry{
		ID:          id,
		Measurement: measurement,
		Tags:        tags,
		Key:         key,
	}, nil
}
