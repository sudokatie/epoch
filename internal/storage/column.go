package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/sudokatie/epoch/internal/compress"
)

// Column file format constants
const (
	ColumnMagic   = 0x45504F43 // "EPOC"
	ColumnVersion = 1

	CompressionNone   = 0
	CompressionGorilla = 1

	BlockMaxPoints = 1000 // Max points per block
)

// ColumnFileHeader is the header at the start of every column file
type ColumnFileHeader struct {
	Magic       uint32
	Version     uint16
	FieldType   uint8
	Compression uint8
	PointCount  uint64
	MinTime     int64
	MaxTime     int64
	BlockCount  uint32
	Reserved    [28]byte // Padding to 64 bytes
}

// BlockHeader describes a single block within the column file
type BlockHeader struct {
	MinTime    int64
	MaxTime    int64
	PointCount uint32
	DataOffset uint64
	DataSize   uint32
}

// ColumnFile handles reading and writing column files
type ColumnFile struct {
	mu        sync.RWMutex
	path      string
	file      *os.File
	header    ColumnFileHeader
	blocks    []BlockHeader
	fieldType FieldType
}

// CreateColumnFile creates a new column file
func CreateColumnFile(path string, fieldType FieldType) (*ColumnFile, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create column file: %w", err)
	}

	cf := &ColumnFile{
		path:      path,
		file:      file,
		fieldType: fieldType,
		header: ColumnFileHeader{
			Magic:       ColumnMagic,
			Version:     ColumnVersion,
			FieldType:   uint8(fieldType),
			Compression: CompressionGorilla,
		},
	}

	// Write initial header (will be updated on close)
	if err := cf.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return cf, nil
}

// OpenColumnFile opens an existing column file for reading
func OpenColumnFile(path string) (*ColumnFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open column file: %w", err)
	}

	cf := &ColumnFile{
		path: path,
		file: file,
	}

	if err := cf.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	if err := cf.readBlockIndex(); err != nil {
		file.Close()
		return nil, err
	}

	cf.fieldType = FieldType(cf.header.FieldType)
	return cf, nil
}

// writeHeader writes the file header
func (cf *ColumnFile) writeHeader() error {
	cf.file.Seek(0, io.SeekStart)

	buf := make([]byte, 64)
	binary.LittleEndian.PutUint32(buf[0:4], cf.header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], cf.header.Version)
	buf[6] = cf.header.FieldType
	buf[7] = cf.header.Compression
	binary.LittleEndian.PutUint64(buf[8:16], cf.header.PointCount)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(cf.header.MinTime))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(cf.header.MaxTime))
	binary.LittleEndian.PutUint32(buf[32:36], cf.header.BlockCount)

	_, err := cf.file.Write(buf)
	return err
}

// readHeader reads the file header
func (cf *ColumnFile) readHeader() error {
	cf.file.Seek(0, io.SeekStart)

	buf := make([]byte, 64)
	if _, err := io.ReadFull(cf.file, buf); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	cf.header.Magic = binary.LittleEndian.Uint32(buf[0:4])
	cf.header.Version = binary.LittleEndian.Uint16(buf[4:6])
	cf.header.FieldType = buf[6]
	cf.header.Compression = buf[7]
	cf.header.PointCount = binary.LittleEndian.Uint64(buf[8:16])
	cf.header.MinTime = int64(binary.LittleEndian.Uint64(buf[16:24]))
	cf.header.MaxTime = int64(binary.LittleEndian.Uint64(buf[24:32]))
	cf.header.BlockCount = binary.LittleEndian.Uint32(buf[32:36])

	if cf.header.Magic != ColumnMagic {
		return fmt.Errorf("invalid magic number: %x", cf.header.Magic)
	}
	if cf.header.Version != ColumnVersion {
		return fmt.Errorf("unsupported version: %d", cf.header.Version)
	}

	return nil
}

// readBlockIndex reads the block index from the end of the file
func (cf *ColumnFile) readBlockIndex() error {
	if cf.header.BlockCount == 0 {
		return nil
	}

	// Block index is at the end of the file
	// Each block header is 32 bytes
	indexSize := int64(cf.header.BlockCount) * 32
	cf.file.Seek(-indexSize, io.SeekEnd)

	cf.blocks = make([]BlockHeader, cf.header.BlockCount)
	buf := make([]byte, 32)

	for i := uint32(0); i < cf.header.BlockCount; i++ {
		if _, err := io.ReadFull(cf.file, buf); err != nil {
			return fmt.Errorf("read block %d header: %w", i, err)
		}

		cf.blocks[i] = BlockHeader{
			MinTime:    int64(binary.LittleEndian.Uint64(buf[0:8])),
			MaxTime:    int64(binary.LittleEndian.Uint64(buf[8:16])),
			PointCount: binary.LittleEndian.Uint32(buf[16:20]),
			DataOffset: binary.LittleEndian.Uint64(buf[20:28]),
			DataSize:   binary.LittleEndian.Uint32(buf[28:32]),
		}
	}

	return nil
}

// AppendBlock writes a block of data to the column file
func (cf *ColumnFile) AppendBlock(timestamps []int64, values interface{}) error {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	if len(timestamps) == 0 {
		return fmt.Errorf("empty block")
	}

	// Compress timestamps
	tsData := compress.CompressTimestamps(timestamps)

	// Compress values based on field type
	var valData []byte
	switch v := values.(type) {
	case []float64:
		valData = compress.CompressFloats(v)
	case []int64:
		valData = compress.CompressIntegers(v)
	case []string:
		valData = compressStrings(v)
	case []bool:
		valData = compressBools(v)
	default:
		return fmt.Errorf("unsupported value type: %T", values)
	}

	// Find write position (before block index if it exists)
	var writePos int64 = 64 // After header
	if len(cf.blocks) > 0 {
		lastBlock := cf.blocks[len(cf.blocks)-1]
		writePos = int64(lastBlock.DataOffset) + int64(lastBlock.DataSize)
	}

	cf.file.Seek(writePos, io.SeekStart)

	// Write timestamp length + data
	tsLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(tsLen, uint32(len(tsData)))
	if _, err := cf.file.Write(tsLen); err != nil {
		return err
	}
	if _, err := cf.file.Write(tsData); err != nil {
		return err
	}

	// Write value length + data
	valLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(valLen, uint32(len(valData)))
	if _, err := cf.file.Write(valLen); err != nil {
		return err
	}
	if _, err := cf.file.Write(valData); err != nil {
		return err
	}

	// Create block header
	dataSize := uint32(4 + len(tsData) + 4 + len(valData))
	block := BlockHeader{
		MinTime:    timestamps[0],
		MaxTime:    timestamps[len(timestamps)-1],
		PointCount: uint32(len(timestamps)),
		DataOffset: uint64(writePos),
		DataSize:   dataSize,
	}
	cf.blocks = append(cf.blocks, block)

	// Update header stats
	cf.header.PointCount += uint64(len(timestamps))
	cf.header.BlockCount = uint32(len(cf.blocks))
	if cf.header.MinTime == 0 || timestamps[0] < cf.header.MinTime {
		cf.header.MinTime = timestamps[0]
	}
	if timestamps[len(timestamps)-1] > cf.header.MaxTime {
		cf.header.MaxTime = timestamps[len(timestamps)-1]
	}

	return nil
}

// ReadBlock reads a specific block by index
func (cf *ColumnFile) ReadBlock(blockIdx int) ([]int64, interface{}, error) {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	if blockIdx < 0 || blockIdx >= len(cf.blocks) {
		return nil, nil, fmt.Errorf("block index out of range: %d", blockIdx)
	}

	block := cf.blocks[blockIdx]
	cf.file.Seek(int64(block.DataOffset), io.SeekStart)

	// Read timestamp length + data
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(cf.file, lenBuf); err != nil {
		return nil, nil, err
	}
	tsLen := binary.LittleEndian.Uint32(lenBuf)

	tsData := make([]byte, tsLen)
	if _, err := io.ReadFull(cf.file, tsData); err != nil {
		return nil, nil, err
	}

	timestamps, err := compress.DecompressTimestamps(tsData)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress timestamps: %w", err)
	}

	// Read value length + data
	if _, err := io.ReadFull(cf.file, lenBuf); err != nil {
		return nil, nil, err
	}
	valLen := binary.LittleEndian.Uint32(lenBuf)

	valData := make([]byte, valLen)
	if _, err := io.ReadFull(cf.file, valData); err != nil {
		return nil, nil, err
	}

	// Decompress values based on field type
	var values interface{}
	switch cf.fieldType {
	case FieldTypeFloat:
		values, err = compress.DecompressFloats(valData)
	case FieldTypeInteger:
		values, err = compress.DecompressIntegers(valData)
	case FieldTypeString:
		values, err = decompressStrings(valData)
	case FieldTypeBoolean:
		values, err = decompressBools(valData)
	default:
		return nil, nil, fmt.Errorf("unsupported field type: %v", cf.fieldType)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("decompress values: %w", err)
	}

	return timestamps, values, nil
}

// ReadTimeRange reads all blocks that overlap with the given time range
func (cf *ColumnFile) ReadTimeRange(minTime, maxTime int64) ([]int64, interface{}, error) {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	var allTimestamps []int64
	var allFloats []float64
	var allInts []int64
	var allStrings []string
	var allBools []bool

	for i, block := range cf.blocks {
		// Skip blocks entirely outside our range
		if block.MaxTime < minTime || block.MinTime > maxTime {
			continue
		}

		timestamps, values, err := cf.readBlockUnlocked(i)
		if err != nil {
			return nil, nil, err
		}

		// Filter to points within range
		for j, ts := range timestamps {
			if ts >= minTime && ts <= maxTime {
				allTimestamps = append(allTimestamps, ts)
				switch v := values.(type) {
				case []float64:
					allFloats = append(allFloats, v[j])
				case []int64:
					allInts = append(allInts, v[j])
				case []string:
					allStrings = append(allStrings, v[j])
				case []bool:
					allBools = append(allBools, v[j])
				}
			}
		}
	}

	var result interface{}
	switch cf.fieldType {
	case FieldTypeFloat:
		result = allFloats
	case FieldTypeInteger:
		result = allInts
	case FieldTypeString:
		result = allStrings
	case FieldTypeBoolean:
		result = allBools
	}

	return allTimestamps, result, nil
}

// readBlockUnlocked reads a block without holding the lock (caller must hold lock)
func (cf *ColumnFile) readBlockUnlocked(blockIdx int) ([]int64, interface{}, error) {
	block := cf.blocks[blockIdx]
	cf.file.Seek(int64(block.DataOffset), io.SeekStart)

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(cf.file, lenBuf); err != nil {
		return nil, nil, err
	}
	tsLen := binary.LittleEndian.Uint32(lenBuf)

	tsData := make([]byte, tsLen)
	if _, err := io.ReadFull(cf.file, tsData); err != nil {
		return nil, nil, err
	}

	timestamps, err := compress.DecompressTimestamps(tsData)
	if err != nil {
		return nil, nil, err
	}

	if _, err := io.ReadFull(cf.file, lenBuf); err != nil {
		return nil, nil, err
	}
	valLen := binary.LittleEndian.Uint32(lenBuf)

	valData := make([]byte, valLen)
	if _, err := io.ReadFull(cf.file, valData); err != nil {
		return nil, nil, err
	}

	var values interface{}
	switch cf.fieldType {
	case FieldTypeFloat:
		values, err = compress.DecompressFloats(valData)
	case FieldTypeInteger:
		values, err = compress.DecompressIntegers(valData)
	case FieldTypeString:
		values, err = decompressStrings(valData)
	case FieldTypeBoolean:
		values, err = decompressBools(valData)
	}

	return timestamps, values, err
}

// Close writes the block index and closes the file
func (cf *ColumnFile) Close() error {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	// Write updated header
	if err := cf.writeHeader(); err != nil {
		return err
	}

	// Write block index at current position
	// (after all block data)
	var indexPos int64 = 64
	if len(cf.blocks) > 0 {
		lastBlock := cf.blocks[len(cf.blocks)-1]
		indexPos = int64(lastBlock.DataOffset) + int64(lastBlock.DataSize)
	}

	cf.file.Seek(indexPos, io.SeekStart)

	buf := make([]byte, 32)
	for _, block := range cf.blocks {
		binary.LittleEndian.PutUint64(buf[0:8], uint64(block.MinTime))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(block.MaxTime))
		binary.LittleEndian.PutUint32(buf[16:20], block.PointCount)
		binary.LittleEndian.PutUint64(buf[20:28], block.DataOffset)
		binary.LittleEndian.PutUint32(buf[28:32], block.DataSize)

		if _, err := cf.file.Write(buf); err != nil {
			return err
		}
	}

	return cf.file.Close()
}

// Stats returns file statistics
func (cf *ColumnFile) Stats() (pointCount uint64, blockCount uint32, minTime, maxTime int64) {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return cf.header.PointCount, cf.header.BlockCount, cf.header.MinTime, cf.header.MaxTime
}

// Block represents a data block with timestamps and values
type Block struct {
	Timestamps []int64
	Values     interface{}
}

// ReadAll reads all blocks from the column file
func (cf *ColumnFile) ReadAll() ([]Block, error) {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	var blocks []Block
	for i := 0; i < int(cf.header.BlockCount); i++ {
		ts, vals, err := cf.readBlockUnlocked(i)
		if err != nil {
			return nil, fmt.Errorf("read block %d: %w", i, err)
		}
		blocks = append(blocks, Block{Timestamps: ts, Values: vals})
	}
	return blocks, nil
}

// WriteBlock writes a pre-formed block to the column file
func (cf *ColumnFile) WriteBlock(block Block) error {
	return cf.AppendBlock(block.Timestamps, block.Values)
}

// String compression helpers

func compressStrings(values []string) []byte {
	// Simple format: count (4 bytes) + length-prefixed strings
	var result []byte

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(values)))
	result = append(result, countBuf...)

	for _, s := range values {
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(s)))
		result = append(result, lenBuf...)
		result = append(result, []byte(s)...)
	}

	return result
}

func decompressStrings(data []byte) ([]string, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("string data too short")
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	result := make([]string, 0, count)

	pos := 4
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("truncated string data")
		}
		strLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(strLen) > len(data) {
			return nil, fmt.Errorf("truncated string data")
		}
		result = append(result, string(data[pos:pos+int(strLen)]))
		pos += int(strLen)
	}

	return result, nil
}

func compressBools(values []bool) []byte {
	// Bit-packed booleans: count (4 bytes) + packed bits
	result := make([]byte, 4+(len(values)+7)/8)
	binary.LittleEndian.PutUint32(result[0:4], uint32(len(values)))

	for i, v := range values {
		if v {
			byteIdx := 4 + i/8
			bitIdx := uint(i % 8)
			result[byteIdx] |= 1 << bitIdx
		}
	}

	return result
}

func decompressBools(data []byte) ([]bool, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("bool data too short")
	}

	count := binary.LittleEndian.Uint32(data[0:4])
	result := make([]bool, count)

	for i := uint32(0); i < count; i++ {
		byteIdx := 4 + i/8
		bitIdx := uint(i % 8)
		if byteIdx < uint32(len(data)) {
			result[i] = (data[byteIdx]>>bitIdx)&1 == 1
		}
	}

	return result, nil
}
