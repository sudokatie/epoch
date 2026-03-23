package compress

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestBitWriter(t *testing.T) {
	t.Run("single bits", func(t *testing.T) {
		w := NewBitWriter()
		w.WriteBit(true)
		w.WriteBit(false)
		w.WriteBit(true)
		w.WriteBit(true)
		w.WriteBit(false)
		w.WriteBit(false)
		w.WriteBit(false)
		w.WriteBit(true)

		bytes := w.Bytes()
		if len(bytes) != 1 {
			t.Errorf("expected 1 byte, got %d", len(bytes))
		}
		if bytes[0] != 0b10110001 {
			t.Errorf("expected 0b10110001, got %08b", bytes[0])
		}
	})

	t.Run("multi bits", func(t *testing.T) {
		w := NewBitWriter()
		w.WriteBits(0b11010, 5)
		w.WriteBits(0b111, 3)

		bytes := w.Bytes()
		if len(bytes) != 1 {
			t.Errorf("expected 1 byte, got %d", len(bytes))
		}
		if bytes[0] != 0b11010111 {
			t.Errorf("expected 0b11010111, got %08b", bytes[0])
		}
	})
}

func TestBitReader(t *testing.T) {
	data := []byte{0b10110001, 0b11110000}
	r := NewBitReader(data)

	// Read individual bits
	bits := make([]bool, 8)
	for i := 0; i < 8; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			t.Fatalf("ReadBit error: %v", err)
		}
		bits[i] = bit
	}

	expected := []bool{true, false, true, true, false, false, false, true}
	for i, exp := range expected {
		if bits[i] != exp {
			t.Errorf("bit %d: got %v, want %v", i, bits[i], exp)
		}
	}

	// Read remaining 8 bits as one
	v, err := r.ReadBits(8)
	if err != nil {
		t.Fatalf("ReadBits error: %v", err)
	}
	if v != 0b11110000 {
		t.Errorf("expected 0b11110000, got %08b", v)
	}
}

func TestCompressTimestampsEmpty(t *testing.T) {
	compressed := CompressTimestamps(nil)
	if compressed != nil {
		t.Errorf("expected nil for empty input")
	}

	decompressed, err := DecompressTimestamps(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if decompressed != nil {
		t.Errorf("expected nil for empty input")
	}
}

func TestCompressTimestampsSingle(t *testing.T) {
	timestamps := []int64{1679616000000000000}

	compressed := CompressTimestamps(timestamps)
	decompressed, err := DecompressTimestamps(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if len(decompressed) != 1 {
		t.Fatalf("expected 1 timestamp, got %d", len(decompressed))
	}
	if decompressed[0] != timestamps[0] {
		t.Errorf("got %d, want %d", decompressed[0], timestamps[0])
	}
}

func TestCompressTimestampsSequential(t *testing.T) {
	// Sequential timestamps with 1-second intervals (common case)
	base := int64(1679616000000000000)
	interval := int64(time.Second)
	n := 1000

	timestamps := make([]int64, n)
	for i := 0; i < n; i++ {
		timestamps[i] = base + int64(i)*interval
	}

	compressed := CompressTimestamps(timestamps)
	decompressed, err := DecompressTimestamps(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if len(decompressed) != n {
		t.Fatalf("expected %d timestamps, got %d", n, len(decompressed))
	}

	for i, ts := range decompressed {
		if ts != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts, timestamps[i])
		}
	}

	// Check compression ratio
	uncompressed := n * 8 // 8 bytes per int64
	ratio := float64(uncompressed) / float64(len(compressed))
	t.Logf("Sequential timestamps: %d points, %d bytes compressed, ratio %.2fx", n, len(compressed), ratio)

	// Should achieve good compression for sequential data
	if ratio < 3 {
		t.Errorf("expected compression ratio >= 3x, got %.2fx", ratio)
	}
}

func TestCompressTimestampsIrregular(t *testing.T) {
	// Irregular intervals
	timestamps := []int64{
		1679616000000000000,
		1679616001000000000, // +1s
		1679616001500000000, // +0.5s
		1679616010000000000, // +8.5s
		1679616010100000000, // +0.1s
	}

	compressed := CompressTimestamps(timestamps)
	decompressed, err := DecompressTimestamps(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if len(decompressed) != len(timestamps) {
		t.Fatalf("expected %d timestamps, got %d", len(timestamps), len(decompressed))
	}

	for i, ts := range decompressed {
		if ts != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts, timestamps[i])
		}
	}
}

func TestCompressTimestampsRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	n := 500

	base := int64(1679616000000000000)
	timestamps := make([]int64, n)
	timestamps[0] = base
	for i := 1; i < n; i++ {
		// Random delta between 100ms and 10s
		delta := int64(100000000) + rng.Int63n(int64(10*time.Second))
		timestamps[i] = timestamps[i-1] + delta
	}

	compressed := CompressTimestamps(timestamps)
	decompressed, err := DecompressTimestamps(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, ts := range decompressed {
		if ts != timestamps[i] {
			t.Errorf("timestamp %d: got %d, want %d", i, ts, timestamps[i])
		}
	}
}

func TestCompressFloatsEmpty(t *testing.T) {
	compressed := CompressFloats(nil)
	if compressed != nil {
		t.Errorf("expected nil for empty input")
	}

	decompressed, err := DecompressFloats(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if decompressed != nil {
		t.Errorf("expected nil for empty input")
	}
}

func TestCompressFloatsSingle(t *testing.T) {
	values := []float64{42.5}

	compressed := CompressFloats(values)
	decompressed, err := DecompressFloats(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if len(decompressed) != 1 {
		t.Fatalf("expected 1 value, got %d", len(decompressed))
	}
	if decompressed[0] != values[0] {
		t.Errorf("got %v, want %v", decompressed[0], values[0])
	}
}

func TestCompressFloatsConstant(t *testing.T) {
	// All same value - best case for XOR compression
	n := 1000
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = 42.5
	}

	compressed := CompressFloats(values)
	decompressed, err := DecompressFloats(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, v := range decompressed {
		if v != values[i] {
			t.Errorf("value %d: got %v, want %v", i, v, values[i])
		}
	}

	// Constant values should compress extremely well
	uncompressed := n * 8
	ratio := float64(uncompressed) / float64(len(compressed))
	t.Logf("Constant floats: %d points, %d bytes compressed, ratio %.2fx", n, len(compressed), ratio)

	if ratio < 10 {
		t.Errorf("expected compression ratio >= 10x for constant values, got %.2fx", ratio)
	}
}

func TestCompressFloatsIncrementing(t *testing.T) {
	n := 500
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = float64(i) * 0.1
	}

	compressed := CompressFloats(values)
	decompressed, err := DecompressFloats(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, v := range decompressed {
		if v != values[i] {
			t.Errorf("value %d: got %v, want %v", i, v, values[i])
		}
	}
}

func TestCompressFloatsRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	n := 500
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = rng.Float64() * 100
	}

	compressed := CompressFloats(values)
	decompressed, err := DecompressFloats(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, v := range decompressed {
		if v != values[i] {
			t.Errorf("value %d: got %v, want %v", i, v, values[i])
		}
	}
}

func TestCompressFloatsSpecialValues(t *testing.T) {
	values := []float64{
		0,
		-0,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		-math.MaxFloat64,
	}

	compressed := CompressFloats(values)
	decompressed, err := DecompressFloats(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, v := range decompressed {
		if math.IsNaN(values[i]) {
			if !math.IsNaN(v) {
				t.Errorf("value %d: expected NaN, got %v", i, v)
			}
		} else if math.IsInf(values[i], 0) {
			if !math.IsInf(v, 0) || math.Signbit(v) != math.Signbit(values[i]) {
				t.Errorf("value %d: got %v, want %v", i, v, values[i])
			}
		} else if v != values[i] {
			t.Errorf("value %d: got %v, want %v", i, v, values[i])
		}
	}
}

func TestCompressIntegersEmpty(t *testing.T) {
	compressed := CompressIntegers(nil)
	if compressed != nil {
		t.Errorf("expected nil for empty input")
	}

	decompressed, err := DecompressIntegers(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if decompressed != nil {
		t.Errorf("expected nil for empty input")
	}
}

func TestCompressIntegersSingle(t *testing.T) {
	values := []int64{12345}

	compressed := CompressIntegers(values)
	decompressed, err := DecompressIntegers(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if len(decompressed) != 1 || decompressed[0] != values[0] {
		t.Errorf("got %v, want %v", decompressed, values)
	}
}

func TestCompressIntegersSequential(t *testing.T) {
	n := 1000
	values := make([]int64, n)
	for i := 0; i < n; i++ {
		values[i] = int64(i * 100)
	}

	compressed := CompressIntegers(values)
	decompressed, err := DecompressIntegers(compressed)
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	for i, v := range decompressed {
		if v != values[i] {
			t.Errorf("value %d: got %d, want %d", i, v, values[i])
		}
	}

	// Check compression
	uncompressed := n * 8
	ratio := float64(uncompressed) / float64(len(compressed))
	t.Logf("Sequential integers: %d points, %d bytes compressed, ratio %.2fx", n, len(compressed), ratio)
}

func TestLeadingTrailingZeros(t *testing.T) {
	tests := []struct {
		x        uint64
		leading  int
		trailing int
	}{
		{0, 64, 64},
		{1, 63, 0},
		{2, 62, 1},
		{0x8000000000000000, 0, 63},
		{0xFF, 56, 0},
		{0xFF00, 48, 8},
	}

	for _, tt := range tests {
		leading := countLeadingZeros64(tt.x)
		trailing := countTrailingZeros64(tt.x)
		if leading != tt.leading {
			t.Errorf("leadingZeros(%x) = %d, want %d", tt.x, leading, tt.leading)
		}
		if trailing != tt.trailing {
			t.Errorf("trailingZeros(%x) = %d, want %d", tt.x, trailing, tt.trailing)
		}
	}
}

func BenchmarkCompressTimestamps(b *testing.B) {
	base := int64(1679616000000000000)
	interval := int64(time.Second)
	n := 10000

	timestamps := make([]int64, n)
	for i := 0; i < n; i++ {
		timestamps[i] = base + int64(i)*interval
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressTimestamps(timestamps)
	}
}

func BenchmarkDecompressTimestamps(b *testing.B) {
	base := int64(1679616000000000000)
	interval := int64(time.Second)
	n := 10000

	timestamps := make([]int64, n)
	for i := 0; i < n; i++ {
		timestamps[i] = base + int64(i)*interval
	}

	compressed := CompressTimestamps(timestamps)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecompressTimestamps(compressed)
	}
}

func BenchmarkCompressFloats(b *testing.B) {
	n := 10000
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = 45.2 + float64(i%10)*0.1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressFloats(values)
	}
}

func BenchmarkDecompressFloats(b *testing.B) {
	n := 10000
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = 45.2 + float64(i%10)*0.1
	}

	compressed := CompressFloats(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecompressFloats(compressed)
	}
}
