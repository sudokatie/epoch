package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// WAL constants
const (
	WALMagic          = 0x57414C45 // "WALE"
	WALVersion        = 1
	WALSegmentSize    = 10 * 1024 * 1024 // 10MB default
	WALHeaderSize     = 16
	WALEntryHeaderSize = 13 // type(1) + length(4) + crc(4) + timestamp(4 reserved)
)

// Entry types
const (
	EntryTypeWrite      = 1
	EntryTypeDelete     = 2
	EntryTypeCheckpoint = 3
)

// SyncMode controls when WAL syncs to disk
type SyncMode int

const (
	SyncNone       SyncMode = iota // No explicit sync (OS decides)
	SyncEveryWrite                 // Sync after every write
	SyncEveryN                     // Sync every N writes
	SyncInterval                   // Sync on interval (handled externally)
)

// WALEntry represents a single entry in the WAL
type WALEntry struct {
	Type      uint8
	Timestamp int64
	Data      []byte
}

// WALSegment represents a single WAL segment file
type WALSegment struct {
	path     string
	file     *os.File
	size     int64
	sequence uint64
}

// WAL manages write-ahead logging for durability
type WAL struct {
	mu           sync.Mutex
	dir          string
	segmentSize  int64
	syncMode     SyncMode
	syncEveryN   int
	writeCount   int
	
	segments     []*WALSegment
	current      *WALSegment
	lastSequence uint64
}

// WALConfig holds WAL configuration
type WALConfig struct {
	Dir         string
	SegmentSize int64
	SyncMode    SyncMode
	SyncEveryN  int
}

// NewWAL creates a new WAL instance
func NewWAL(config WALConfig) (*WAL, error) {
	if config.SegmentSize <= 0 {
		config.SegmentSize = WALSegmentSize
	}
	if config.SyncEveryN <= 0 {
		config.SyncEveryN = 100
	}

	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	wal := &WAL{
		dir:         config.Dir,
		segmentSize: config.SegmentSize,
		syncMode:    config.SyncMode,
		syncEveryN:  config.SyncEveryN,
	}

	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		return nil, err
	}

	// Create first segment if none exist
	if len(wal.segments) == 0 {
		if err := wal.rollover(); err != nil {
			return nil, err
		}
	} else {
		// Open the last segment for appending
		wal.current = wal.segments[len(wal.segments)-1]
		if err := wal.openCurrentSegment(); err != nil {
			return nil, err
		}
	}

	return wal, nil
}

// loadSegments discovers and loads existing WAL segments
func (w *WAL) loadSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("read WAL dir: %w", err)
	}

	var segments []*WALSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}

		// Parse sequence number from filename: wal-000001.log
		seqStr := strings.TrimPrefix(name, "wal-")
		seqStr = strings.TrimSuffix(seqStr, ".log")
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		segments = append(segments, &WALSegment{
			path:     filepath.Join(w.dir, name),
			sequence: seq,
			size:     info.Size(),
		})

		if seq > w.lastSequence {
			w.lastSequence = seq
		}
	}

	// Sort by sequence
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].sequence < segments[j].sequence
	})

	w.segments = segments
	return nil
}

// openCurrentSegment opens the current segment for appending
func (w *WAL) openCurrentSegment() error {
	file, err := os.OpenFile(w.current.path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	w.current.file = file

	// Seek to end to get current size
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return err
	}
	w.current.size = size

	return nil
}

// rollover creates a new segment
func (w *WAL) rollover() error {
	// Close current segment if open
	if w.current != nil && w.current.file != nil {
		if err := w.current.file.Sync(); err != nil {
			return err
		}
		if err := w.current.file.Close(); err != nil {
			return err
		}
	}

	// Create new segment
	w.lastSequence++
	name := fmt.Sprintf("wal-%06d.log", w.lastSequence)
	path := filepath.Join(w.dir, name)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}

	// Write segment header
	header := make([]byte, WALHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], WALMagic)
	binary.LittleEndian.PutUint16(header[4:6], WALVersion)
	binary.LittleEndian.PutUint64(header[8:16], w.lastSequence)

	if _, err := file.Write(header); err != nil {
		file.Close()
		return err
	}

	segment := &WALSegment{
		path:     path,
		file:     file,
		sequence: w.lastSequence,
		size:     WALHeaderSize,
	}

	w.segments = append(w.segments, segment)
	w.current = segment

	return nil
}

// Append writes an entry to the WAL
func (w *WAL) Append(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to rollover
	entrySize := int64(WALEntryHeaderSize + len(entry.Data))
	if w.current.size+entrySize > w.segmentSize {
		if err := w.rollover(); err != nil {
			return err
		}
	}

	// Build entry
	buf := make([]byte, WALEntryHeaderSize+len(entry.Data))
	buf[0] = entry.Type
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(entry.Data)))
	// bytes 5-8 reserved for future use (timestamp prefix, etc.)
	copy(buf[WALEntryHeaderSize:], entry.Data)

	// Calculate CRC over type + length + data
	crc := crc32.ChecksumIEEE(buf[:WALEntryHeaderSize-4])
	crc = crc32.Update(crc, crc32.IEEETable, entry.Data)
	binary.LittleEndian.PutUint32(buf[9:13], crc)

	// Write to file
	if _, err := w.current.file.Write(buf); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	w.current.size += int64(len(buf))

	// Handle sync
	w.writeCount++
	switch w.syncMode {
	case SyncEveryWrite:
		if err := w.current.file.Sync(); err != nil {
			return err
		}
	case SyncEveryN:
		if w.writeCount >= w.syncEveryN {
			if err := w.current.file.Sync(); err != nil {
				return err
			}
			w.writeCount = 0
		}
	}

	return nil
}

// AppendWrite is a convenience method for write entries
func (w *WAL) AppendWrite(data []byte) error {
	return w.Append(&WALEntry{
		Type: EntryTypeWrite,
		Data: data,
	})
}

// AppendCheckpoint writes a checkpoint marker
func (w *WAL) AppendCheckpoint() error {
	return w.Append(&WALEntry{
		Type: EntryTypeCheckpoint,
		Data: nil,
	})
}

// Sync forces a sync to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.current != nil && w.current.file != nil {
		return w.current.file.Sync()
	}
	return nil
}

// ReadAll reads all entries from all segments
func (w *WAL) ReadAll() ([]*WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []*WALEntry

	for _, seg := range w.segments {
		segEntries, err := w.readSegment(seg)
		if err != nil {
			return nil, fmt.Errorf("read segment %d: %w", seg.sequence, err)
		}
		entries = append(entries, segEntries...)
	}

	return entries, nil
}

// readSegment reads all entries from a single segment
func (w *WAL) readSegment(seg *WALSegment) ([]*WALEntry, error) {
	file, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read and validate header
	header := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != WALMagic {
		return nil, fmt.Errorf("invalid magic: %x", magic)
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version != WALVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// Read entries
	var entries []*WALEntry
	entryHeader := make([]byte, WALEntryHeaderSize)

	for {
		_, err := io.ReadFull(file, entryHeader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial entry at end - could be crash during write
			break
		}

		entryType := entryHeader[0]
		dataLen := binary.LittleEndian.Uint32(entryHeader[1:5])
		storedCRC := binary.LittleEndian.Uint32(entryHeader[9:13])

		data := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := io.ReadFull(file, data); err != nil {
				// Partial entry
				break
			}
		}

		// Verify CRC
		crc := crc32.ChecksumIEEE(entryHeader[:WALEntryHeaderSize-4])
		crc = crc32.Update(crc, crc32.IEEETable, data)
		if crc != storedCRC {
			// Corrupted entry - stop here
			break
		}

		entries = append(entries, &WALEntry{
			Type: entryType,
			Data: data,
		})
	}

	return entries, nil
}

// Truncate removes all segments up to and including the one containing
// the last checkpoint, keeping only newer segments
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Find segments to remove (all but current)
	if len(w.segments) <= 1 {
		return nil
	}

	toRemove := w.segments[:len(w.segments)-1]
	w.segments = w.segments[len(w.segments)-1:]

	for _, seg := range toRemove {
		if seg.file != nil {
			seg.file.Close()
		}
		if err := os.Remove(seg.path); err != nil {
			return fmt.Errorf("remove segment: %w", err)
		}
	}

	return nil
}

// TruncateBefore removes all segments with sequence numbers before the given one
func (w *WAL) TruncateBefore(sequence uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var kept []*WALSegment
	for _, seg := range w.segments {
		if seg.sequence >= sequence {
			kept = append(kept, seg)
		} else {
			if seg.file != nil {
				seg.file.Close()
			}
			if err := os.Remove(seg.path); err != nil {
				return fmt.Errorf("remove segment %d: %w", seg.sequence, err)
			}
		}
	}

	w.segments = kept
	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.current != nil && w.current.file != nil {
		if err := w.current.file.Sync(); err != nil {
			return err
		}
		if err := w.current.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns WAL statistics
func (w *WAL) Stats() (segmentCount int, totalSize int64, currentSequence uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.segments {
		totalSize += seg.size
	}

	return len(w.segments), totalSize, w.lastSequence
}
