package compress

import (
	"encoding/binary"
	"io"
)

// BitWriter writes individual bits to a byte slice
type BitWriter struct {
	buf     []byte
	current byte
	bitPos  uint8 // bits written to current byte (0-7)
}

// NewBitWriter creates a new bit writer
func NewBitWriter() *BitWriter {
	return &BitWriter{
		buf: make([]byte, 0, 1024),
	}
}

// WriteBit writes a single bit (0 or 1)
func (w *BitWriter) WriteBit(bit bool) {
	if bit {
		w.current |= 1 << (7 - w.bitPos)
	}
	w.bitPos++
	if w.bitPos == 8 {
		w.buf = append(w.buf, w.current)
		w.current = 0
		w.bitPos = 0
	}
}

// WriteBits writes n bits from the least significant bits of v
func (w *BitWriter) WriteBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.WriteBit((v>>i)&1 == 1)
	}
}

// WriteBytes writes full bytes
func (w *BitWriter) WriteBytes(data []byte) {
	for _, b := range data {
		w.WriteBits(uint64(b), 8)
	}
}

// Bytes returns the written bytes, flushing any partial byte
func (w *BitWriter) Bytes() []byte {
	if w.bitPos > 0 {
		return append(w.buf, w.current)
	}
	return w.buf
}

// BitCount returns total bits written
func (w *BitWriter) BitCount() int {
	return len(w.buf)*8 + int(w.bitPos)
}

// BitReader reads individual bits from a byte slice
type BitReader struct {
	data   []byte
	pos    int   // current byte position
	bitPos uint8 // bits read from current byte (0-7)
}

// NewBitReader creates a new bit reader
func NewBitReader(data []byte) *BitReader {
	return &BitReader{data: data}
}

// ReadBit reads a single bit
func (r *BitReader) ReadBit() (bool, error) {
	if r.pos >= len(r.data) {
		return false, io.EOF
	}
	bit := (r.data[r.pos] >> (7 - r.bitPos)) & 1
	r.bitPos++
	if r.bitPos == 8 {
		r.pos++
		r.bitPos = 0
	}
	return bit == 1, nil
}

// ReadBits reads n bits and returns them as a uint64
func (r *BitReader) ReadBits(n uint8) (uint64, error) {
	var v uint64
	for i := uint8(0); i < n; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		v <<= 1
		if bit {
			v |= 1
		}
	}
	return v, nil
}

// ReadBytes reads n full bytes
func (r *BitReader) ReadBytes(n int) ([]byte, error) {
	result := make([]byte, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadBits(8)
		if err != nil {
			return nil, err
		}
		result[i] = byte(v)
	}
	return result, nil
}

// BitsRemaining returns approximate bits remaining
func (r *BitReader) BitsRemaining() int {
	return (len(r.data)-r.pos)*8 - int(r.bitPos)
}

// Helper functions for encoding/decoding integers with bit packing

// EncodeInt64 writes an int64 as 8 bytes big-endian
func EncodeInt64(w *BitWriter, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	w.WriteBytes(buf[:])
}

// DecodeInt64 reads an int64 as 8 bytes big-endian
func DecodeInt64(r *BitReader) (int64, error) {
	bytes, err := r.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

// EncodeUint64 writes a uint64 as 8 bytes big-endian
func EncodeUint64(w *BitWriter, v uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	w.WriteBytes(buf[:])
}

// DecodeUint64 reads a uint64 as 8 bytes big-endian
func DecodeUint64(r *BitReader) (uint64, error) {
	bytes, err := r.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes), nil
}
