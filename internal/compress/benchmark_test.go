package compress

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkTimestampCompressSequential(b *testing.B) {
	// Generate sequential timestamps (1 second apart)
	timestamps := make([]int64, 1000)
	base := time.Now().UnixNano()
	for i := range timestamps {
		timestamps[i] = base + int64(i)*int64(time.Second)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressTimestamps(timestamps)
	}

	compressed := CompressTimestamps(timestamps)
	ratio := float64(len(timestamps)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkTimestampCompressRandom(b *testing.B) {
	timestamps := make([]int64, 1000)
	base := time.Now().UnixNano()
	for i := range timestamps {
		timestamps[i] = base + rand.Int63n(1000000000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressTimestamps(timestamps)
	}

	compressed := CompressTimestamps(timestamps)
	ratio := float64(len(timestamps)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkTimestampDecompress(b *testing.B) {
	timestamps := make([]int64, 1000)
	base := time.Now().UnixNano()
	for i := range timestamps {
		timestamps[i] = base + int64(i)*int64(time.Second)
	}

	compressed := CompressTimestamps(timestamps)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecompressTimestamps(compressed)
	}
}

func BenchmarkFloatCompressConstant(b *testing.B) {
	// All same value - should compress extremely well
	values := make([]float64, 1000)
	for i := range values {
		values[i] = 42.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressFloats(values)
	}

	compressed := CompressFloats(values)
	ratio := float64(len(values)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkFloatCompressIncrementing(b *testing.B) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i) * 0.1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressFloats(values)
	}

	compressed := CompressFloats(values)
	ratio := float64(len(values)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkFloatCompressRandom(b *testing.B) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = rand.Float64() * 1000
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressFloats(values)
	}

	compressed := CompressFloats(values)
	ratio := float64(len(values)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkFloatDecompress(b *testing.B) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i) * 0.1
	}

	compressed := CompressFloats(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecompressFloats(compressed)
	}
}

func BenchmarkBitWriterRead(b *testing.B) {
	writer := NewBitWriter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.WriteBits(uint64(i), 16)
	}
}

func BenchmarkRealWorldMetrics(b *testing.B) {
	// Simulate real CPU metrics: mostly stable with occasional spikes
	values := make([]float64, 1000)
	baseValue := 0.5
	for i := range values {
		// Add small random variation
		variation := (rand.Float64() - 0.5) * 0.1
		// Occasional spikes
		if rand.Float32() < 0.05 {
			variation += rand.Float64() * 0.3
		}
		values[i] = baseValue + variation
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressFloats(values)
	}

	compressed := CompressFloats(values)
	ratio := float64(len(values)*8) / float64(len(compressed))
	b.ReportMetric(ratio, "compression_ratio")
}
