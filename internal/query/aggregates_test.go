package query

import (
	"math"
	"testing"
	"time"
)

func TestCountAggregator(t *testing.T) {
	agg := NewCountAggregator()
	now := time.Now()

	agg.Push(1.0, now)
	agg.Push(2.0, now)
	agg.Push(3.0, now)

	if got := agg.Result(); got != 3 {
		t.Errorf("count = %v, want 3", got)
	}

	// NaN values should not be counted
	agg.Reset()
	agg.Push(1.0, now)
	agg.Push(math.NaN(), now)
	agg.Push(2.0, now)

	if got := agg.Result(); got != 2 {
		t.Errorf("count with NaN = %v, want 2", got)
	}
}

func TestSumAggregator(t *testing.T) {
	agg := NewSumAggregator()
	now := time.Now()

	agg.Push(1.0, now)
	agg.Push(2.0, now)
	agg.Push(3.0, now)

	if got := agg.Result(); got != 6.0 {
		t.Errorf("sum = %v, want 6.0", got)
	}

	// Empty should return NaN
	agg.Reset()
	if !math.IsNaN(agg.Result()) {
		t.Error("empty sum should be NaN")
	}
}

func TestMeanAggregator(t *testing.T) {
	agg := NewMeanAggregator()
	now := time.Now()

	agg.Push(1.0, now)
	agg.Push(2.0, now)
	agg.Push(3.0, now)

	if got := agg.Result(); got != 2.0 {
		t.Errorf("mean = %v, want 2.0", got)
	}

	// Empty should return NaN
	agg.Reset()
	if !math.IsNaN(agg.Result()) {
		t.Error("empty mean should be NaN")
	}
}

func TestMinAggregator(t *testing.T) {
	agg := NewMinAggregator()
	now := time.Now()

	agg.Push(5.0, now)
	agg.Push(2.0, now)
	agg.Push(8.0, now)

	if got := agg.Result(); got != 2.0 {
		t.Errorf("min = %v, want 2.0", got)
	}

	// Empty should return NaN
	agg.Reset()
	if !math.IsNaN(agg.Result()) {
		t.Error("empty min should be NaN")
	}
}

func TestMaxAggregator(t *testing.T) {
	agg := NewMaxAggregator()
	now := time.Now()

	agg.Push(5.0, now)
	agg.Push(2.0, now)
	agg.Push(8.0, now)

	if got := agg.Result(); got != 8.0 {
		t.Errorf("max = %v, want 8.0", got)
	}

	// Empty should return NaN
	agg.Reset()
	if !math.IsNaN(agg.Result()) {
		t.Error("empty max should be NaN")
	}
}

func TestFirstAggregator(t *testing.T) {
	agg := NewFirstAggregator()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	agg.Push(5.0, base.Add(2*time.Hour))
	agg.Push(2.0, base.Add(1*time.Hour))  // Earliest
	agg.Push(8.0, base.Add(3*time.Hour))

	if got := agg.Result(); got != 2.0 {
		t.Errorf("first = %v, want 2.0 (value at earliest time)", got)
	}
}

func TestLastAggregator(t *testing.T) {
	agg := NewLastAggregator()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	agg.Push(5.0, base.Add(2*time.Hour))
	agg.Push(2.0, base.Add(1*time.Hour))
	agg.Push(8.0, base.Add(3*time.Hour))  // Latest

	if got := agg.Result(); got != 8.0 {
		t.Errorf("last = %v, want 8.0 (value at latest time)", got)
	}
}

func TestMedianAggregator(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"odd count", []float64{1, 3, 2}, 2.0},
		{"even count", []float64{1, 2, 3, 4}, 2.5},
		{"single", []float64{5}, 5.0},
		{"two values", []float64{1, 3}, 2.0},
	}

	now := time.Now()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := NewMedianAggregator()
			for _, v := range tt.values {
				agg.Push(v, now)
			}
			if got := agg.Result(); got != tt.want {
				t.Errorf("median = %v, want %v", got, tt.want)
			}
		})
	}

	// Empty
	agg := NewMedianAggregator()
	if !math.IsNaN(agg.Result()) {
		t.Error("empty median should be NaN")
	}
}

func TestPercentileAggregator(t *testing.T) {
	now := time.Now()
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	tests := []struct {
		percentile float64
		want       float64
	}{
		{0, 1.0},
		{50, 5.5},
		{100, 10.0},
		{25, 3.25},
		{75, 7.75},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			agg := NewPercentileAggregator(tt.percentile)
			for _, v := range values {
				agg.Push(v, now)
			}
			got := agg.Result()
			if math.Abs(got-tt.want) > 0.01 {
				t.Errorf("percentile(%v) = %v, want %v", tt.percentile, got, tt.want)
			}
		})
	}
}

func TestStddevAggregator(t *testing.T) {
	agg := NewStddevAggregator()
	now := time.Now()

	// Values: 2, 4, 4, 4, 5, 5, 7, 9
	// Mean: 5, Variance: 4, Stddev: 2
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	for _, v := range values {
		agg.Push(v, now)
	}

	got := agg.Result()
	want := 2.138089935299395 // Sample stddev

	if math.Abs(got-want) > 0.0001 {
		t.Errorf("stddev = %v, want %v", got, want)
	}

	// Less than 2 values should return NaN
	agg.Reset()
	agg.Push(1.0, now)
	if !math.IsNaN(agg.Result()) {
		t.Error("stddev with 1 value should be NaN")
	}
}

func TestNewAggregator(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"count", "count"},
		{"COUNT", "count"},
		{"sum", "sum"},
		{"mean", "mean"},
		{"avg", "mean"},
		{"min", "min"},
		{"max", "max"},
		{"first", "first"},
		{"last", "last"},
		{"median", "median"},
		{"stddev", "stddev"},
	}

	for _, tt := range tests {
		agg := NewAggregator(tt.name)
		if agg == nil {
			t.Errorf("NewAggregator(%q) = nil", tt.name)
			continue
		}
		if got := agg.Name(); got != tt.want {
			t.Errorf("NewAggregator(%q).Name() = %q, want %q", tt.name, got, tt.want)
		}
	}

	// Unknown function
	if agg := NewAggregator("unknown"); agg != nil {
		t.Error("expected nil for unknown function")
	}
}

func TestPercentileAggregatorWithArg(t *testing.T) {
	agg := NewAggregator("percentile", 90)
	if agg == nil {
		t.Fatal("expected percentile aggregator")
	}

	now := time.Now()
	for i := 1; i <= 100; i++ {
		agg.Push(float64(i), now)
	}

	got := agg.Result()
	// 90th percentile of 1-100 should be around 90
	if got < 89.0 || got > 91.0 {
		t.Errorf("90th percentile = %v, want ~90", got)
	}
}

func TestTruncateTime(t *testing.T) {
	ts := time.Date(2024, 1, 15, 14, 32, 45, 0, time.UTC)

	tests := []struct {
		interval time.Duration
		want     time.Time
	}{
		{time.Hour, time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC)},
		{5 * time.Minute, time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)},
		{time.Minute, time.Date(2024, 1, 15, 14, 32, 0, 0, time.UTC)},
		{0, ts}, // Zero interval returns original
	}

	for _, tt := range tests {
		got := TruncateTime(ts, tt.interval)
		if !got.Equal(tt.want) {
			t.Errorf("TruncateTime(%v, %v) = %v, want %v", ts, tt.interval, got, tt.want)
		}
	}
}

func TestTimeBucketer(t *testing.T) {
	tb := NewTimeBucketer(time.Hour)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Push values into different buckets
	aggNames := []string{"mean"}

	b1 := tb.GetBucket(base.Add(30*time.Minute), aggNames)
	b1.Aggregators["mean"].Push(10.0, base.Add(30*time.Minute))

	b2 := tb.GetBucket(base.Add(90*time.Minute), aggNames)
	b2.Aggregators["mean"].Push(20.0, base.Add(90*time.Minute))

	// Should have 2 buckets
	if len(tb.Buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(tb.Buckets))
	}

	// Get sorted buckets
	buckets := tb.SortedBuckets()
	if len(buckets) != 2 {
		t.Fatalf("expected 2 sorted buckets, got %d", len(buckets))
	}

	// First bucket should be earlier
	if !buckets[0].Start.Before(buckets[1].Start) {
		t.Error("buckets not sorted by time")
	}

	// Check values
	if got := buckets[0].Aggregators["mean"].Result(); got != 10.0 {
		t.Errorf("first bucket mean = %v, want 10.0", got)
	}

	if got := buckets[1].Aggregators["mean"].Result(); got != 20.0 {
		t.Errorf("second bucket mean = %v, want 20.0", got)
	}
}

func TestTimeBucketSameBucket(t *testing.T) {
	tb := NewTimeBucketer(time.Hour)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	aggNames := []string{"sum"}

	// Multiple values in same bucket
	bucket := tb.GetBucket(base.Add(15*time.Minute), aggNames)
	bucket.Aggregators["sum"].Push(1.0, base.Add(15*time.Minute))
	bucket.Aggregators["sum"].Push(2.0, base.Add(30*time.Minute))
	bucket.Aggregators["sum"].Push(3.0, base.Add(45*time.Minute))

	if len(tb.Buckets) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(tb.Buckets))
	}

	if got := bucket.Aggregators["sum"].Result(); got != 6.0 {
		t.Errorf("bucket sum = %v, want 6.0", got)
	}
}

func TestAggregatorReset(t *testing.T) {
	aggregators := []Aggregator{
		NewCountAggregator(),
		NewSumAggregator(),
		NewMeanAggregator(),
		NewMinAggregator(),
		NewMaxAggregator(),
		NewFirstAggregator(),
		NewLastAggregator(),
		NewMedianAggregator(),
		NewPercentileAggregator(50),
		NewStddevAggregator(),
	}

	now := time.Now()
	for _, agg := range aggregators {
		// Push some values
		agg.Push(1.0, now)
		agg.Push(2.0, now)
		agg.Push(3.0, now)

		// Reset
		agg.Reset()

		// After reset, count should be 0
		if count, ok := agg.(*CountAggregator); ok {
			if count.Result() != 0 {
				t.Errorf("%s: count after reset should be 0", agg.Name())
			}
			continue
		}

		// Other aggregators should return NaN after reset (empty state)
		// Skip first/last which track timestamps
		if agg.Name() == "first" || agg.Name() == "last" {
			continue
		}

		if !math.IsNaN(agg.Result()) {
			t.Errorf("%s: result after reset should be NaN, got %v", agg.Name(), agg.Result())
		}
	}
}
