package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func BenchmarkEngineWriteSequential(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	engine.CreateDatabase("bench")

	baseTime := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": "server01"},
			Fields:      Fields{"value": NewFloatField(rand.Float64())},
			Timestamp:   baseTime + int64(i),
		}
		engine.Write("bench", point)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
}

func BenchmarkEngineWriteRandom(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	engine.CreateDatabase("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": fmt.Sprintf("server%d", i%100)},
			Fields:      Fields{"value": NewFloatField(rand.Float64())},
			Timestamp:   time.Now().UnixNano() + rand.Int63n(1000000),
		}
		engine.Write("bench", point)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
}

func BenchmarkEngineWriteBatch(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	engine.CreateDatabase("bench")

	batchSize := 1000
	baseTime := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]*DataPoint, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = &DataPoint{
				Measurement: "cpu",
				Tags:        Tags{"host": "server01"},
				Fields:      Fields{"value": NewFloatField(rand.Float64())},
				Timestamp:   baseTime + int64(i*batchSize+j),
			}
		}
		engine.WriteBatch("bench", batch)
	}

	totalPoints := float64(b.N * batchSize)
	b.ReportMetric(totalPoints/b.Elapsed().Seconds(), "points/sec")
}

func BenchmarkEngineQuery(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	engine.CreateDatabase("bench")

	// Write some data first
	baseTime := time.Now().Add(-time.Hour).UnixNano()
	for i := 0; i < 10000; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": fmt.Sprintf("server%d", i%10)},
			Fields:      Fields{"value": NewFloatField(rand.Float64())},
			Timestamp:   baseTime + int64(i)*int64(time.Second),
		}
		engine.Write("bench", point)
	}

	minTime := baseTime
	maxTime := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Query("bench", "cpu", nil, minTime, maxTime, []string{"value"})
	}
}

func BenchmarkEngineQueryWithTags(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	engine.CreateDatabase("bench")

	// Write some data first
	baseTime := time.Now().Add(-time.Hour).UnixNano()
	for i := 0; i < 10000; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": fmt.Sprintf("server%d", i%10)},
			Fields:      Fields{"value": NewFloatField(rand.Float64())},
			Timestamp:   baseTime + int64(i)*int64(time.Second),
		}
		engine.Write("bench", point)
	}

	minTime := baseTime
	maxTime := time.Now().UnixNano()
	tags := map[string]string{"host": "server1"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Query("bench", "cpu", tags, minTime, maxTime, []string{"value"})
	}
}

func BenchmarkWALWriteNoSync(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	wal, _ := NewWAL(WALConfig{
		Dir:       tmpDir,
		SyncMode:  SyncNone,
		SegmentSize: 10 * 1024 * 1024,
	})
	defer wal.Close()

	data := []byte("test entry data for benchmarking purposes, approximately 100 bytes of data per entry to simulate real workload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &WALEntry{
			Type: EntryTypeWrite,
			Data: data,
		}
		wal.Append(entry)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkWALWriteWithSync(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(tmpDir)

	wal, _ := NewWAL(WALConfig{
		Dir:       tmpDir,
		SyncMode:  SyncEveryWrite,
		SegmentSize: 10 * 1024 * 1024,
	})
	defer wal.Close()

	data := []byte("test entry data for benchmarking purposes")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &WALEntry{
			Type: EntryTypeWrite,
			Data: data,
		}
		wal.Append(entry)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}
