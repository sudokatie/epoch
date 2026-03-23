package query

import (
	"math"
	"sort"
	"time"
)

// Aggregator computes aggregate values over a stream of data points
type Aggregator interface {
	// Push adds a value to the aggregation
	Push(value float64, timestamp time.Time)
	// Result returns the final aggregated value
	Result() float64
	// Reset clears the aggregator state
	Reset()
	// Name returns the aggregate function name
	Name() string
}

// CountAggregator counts non-null values
type CountAggregator struct {
	count int64
}

func NewCountAggregator() *CountAggregator {
	return &CountAggregator{}
}

func (a *CountAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		a.count++
	}
}

func (a *CountAggregator) Result() float64 {
	return float64(a.count)
}

func (a *CountAggregator) Reset() {
	a.count = 0
}

func (a *CountAggregator) Name() string {
	return "count"
}

// SumAggregator computes the sum of values
type SumAggregator struct {
	sum   float64
	valid bool
}

func NewSumAggregator() *SumAggregator {
	return &SumAggregator{}
}

func (a *SumAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		a.sum += value
		a.valid = true
	}
}

func (a *SumAggregator) Result() float64 {
	if !a.valid {
		return math.NaN()
	}
	return a.sum
}

func (a *SumAggregator) Reset() {
	a.sum = 0
	a.valid = false
}

func (a *SumAggregator) Name() string {
	return "sum"
}

// MeanAggregator computes the arithmetic mean
type MeanAggregator struct {
	sum   float64
	count int64
}

func NewMeanAggregator() *MeanAggregator {
	return &MeanAggregator{}
}

func (a *MeanAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		a.sum += value
		a.count++
	}
}

func (a *MeanAggregator) Result() float64 {
	if a.count == 0 {
		return math.NaN()
	}
	return a.sum / float64(a.count)
}

func (a *MeanAggregator) Reset() {
	a.sum = 0
	a.count = 0
}

func (a *MeanAggregator) Name() string {
	return "mean"
}

// MinAggregator tracks the minimum value
type MinAggregator struct {
	min   float64
	valid bool
}

func NewMinAggregator() *MinAggregator {
	return &MinAggregator{min: math.MaxFloat64}
}

func (a *MinAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		if value < a.min {
			a.min = value
		}
		a.valid = true
	}
}

func (a *MinAggregator) Result() float64 {
	if !a.valid {
		return math.NaN()
	}
	return a.min
}

func (a *MinAggregator) Reset() {
	a.min = math.MaxFloat64
	a.valid = false
}

func (a *MinAggregator) Name() string {
	return "min"
}

// MaxAggregator tracks the maximum value
type MaxAggregator struct {
	max   float64
	valid bool
}

func NewMaxAggregator() *MaxAggregator {
	return &MaxAggregator{max: -math.MaxFloat64}
}

func (a *MaxAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		if value > a.max {
			a.max = value
		}
		a.valid = true
	}
}

func (a *MaxAggregator) Result() float64 {
	if !a.valid {
		return math.NaN()
	}
	return a.max
}

func (a *MaxAggregator) Reset() {
	a.max = -math.MaxFloat64
	a.valid = false
}

func (a *MaxAggregator) Name() string {
	return "max"
}

// FirstAggregator returns the first value by timestamp
type FirstAggregator struct {
	value     float64
	timestamp time.Time
	valid     bool
}

func NewFirstAggregator() *FirstAggregator {
	return &FirstAggregator{}
}

func (a *FirstAggregator) Push(value float64, timestamp time.Time) {
	if math.IsNaN(value) {
		return
	}
	if !a.valid || timestamp.Before(a.timestamp) {
		a.value = value
		a.timestamp = timestamp
		a.valid = true
	}
}

func (a *FirstAggregator) Result() float64 {
	if !a.valid {
		return math.NaN()
	}
	return a.value
}

func (a *FirstAggregator) Reset() {
	a.value = 0
	a.timestamp = time.Time{}
	a.valid = false
}

func (a *FirstAggregator) Name() string {
	return "first"
}

// LastAggregator returns the last value by timestamp
type LastAggregator struct {
	value     float64
	timestamp time.Time
	valid     bool
}

func NewLastAggregator() *LastAggregator {
	return &LastAggregator{}
}

func (a *LastAggregator) Push(value float64, timestamp time.Time) {
	if math.IsNaN(value) {
		return
	}
	if !a.valid || timestamp.After(a.timestamp) {
		a.value = value
		a.timestamp = timestamp
		a.valid = true
	}
}

func (a *LastAggregator) Result() float64 {
	if !a.valid {
		return math.NaN()
	}
	return a.value
}

func (a *LastAggregator) Reset() {
	a.value = 0
	a.timestamp = time.Time{}
	a.valid = false
}

func (a *LastAggregator) Name() string {
	return "last"
}

// MedianAggregator computes the median (50th percentile)
type MedianAggregator struct {
	values []float64
}

func NewMedianAggregator() *MedianAggregator {
	return &MedianAggregator{values: make([]float64, 0)}
}

func (a *MedianAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		a.values = append(a.values, value)
	}
}

func (a *MedianAggregator) Result() float64 {
	if len(a.values) == 0 {
		return math.NaN()
	}

	// Sort values
	sorted := make([]float64, len(a.values))
	copy(sorted, a.values)
	sort.Float64s(sorted)

	n := len(sorted)
	if n%2 == 0 {
		// Average of two middle values
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

func (a *MedianAggregator) Reset() {
	a.values = a.values[:0]
}

func (a *MedianAggregator) Name() string {
	return "median"
}

// PercentileAggregator computes the nth percentile
type PercentileAggregator struct {
	values     []float64
	percentile float64
}

func NewPercentileAggregator(percentile float64) *PercentileAggregator {
	return &PercentileAggregator{
		values:     make([]float64, 0),
		percentile: percentile,
	}
}

func (a *PercentileAggregator) Push(value float64, timestamp time.Time) {
	if !math.IsNaN(value) {
		a.values = append(a.values, value)
	}
}

func (a *PercentileAggregator) Result() float64 {
	if len(a.values) == 0 {
		return math.NaN()
	}

	// Sort values
	sorted := make([]float64, len(a.values))
	copy(sorted, a.values)
	sort.Float64s(sorted)

	// Calculate index
	n := float64(len(sorted))
	idx := (a.percentile / 100.0) * (n - 1)

	// Linear interpolation between indices
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))

	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}

	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

func (a *PercentileAggregator) Reset() {
	a.values = a.values[:0]
}

func (a *PercentileAggregator) Name() string {
	return "percentile"
}

// StddevAggregator computes standard deviation using Welford's algorithm
type StddevAggregator struct {
	count int64
	mean  float64
	m2    float64 // Sum of squared deviations
}

func NewStddevAggregator() *StddevAggregator {
	return &StddevAggregator{}
}

func (a *StddevAggregator) Push(value float64, timestamp time.Time) {
	if math.IsNaN(value) {
		return
	}
	a.count++
	delta := value - a.mean
	a.mean += delta / float64(a.count)
	delta2 := value - a.mean
	a.m2 += delta * delta2
}

func (a *StddevAggregator) Result() float64 {
	if a.count < 2 {
		return math.NaN()
	}
	variance := a.m2 / float64(a.count-1) // Sample standard deviation
	return math.Sqrt(variance)
}

func (a *StddevAggregator) Reset() {
	a.count = 0
	a.mean = 0
	a.m2 = 0
}

func (a *StddevAggregator) Name() string {
	return "stddev"
}

// NewAggregator creates an aggregator by function name
func NewAggregator(name string, args ...float64) Aggregator {
	switch name {
	case "count", "COUNT":
		return NewCountAggregator()
	case "sum", "SUM":
		return NewSumAggregator()
	case "mean", "MEAN", "avg", "AVG":
		return NewMeanAggregator()
	case "min", "MIN":
		return NewMinAggregator()
	case "max", "MAX":
		return NewMaxAggregator()
	case "first", "FIRST":
		return NewFirstAggregator()
	case "last", "LAST":
		return NewLastAggregator()
	case "median", "MEDIAN":
		return NewMedianAggregator()
	case "percentile", "PERCENTILE":
		p := 50.0
		if len(args) > 0 {
			p = args[0]
		}
		return NewPercentileAggregator(p)
	case "stddev", "STDDEV":
		return NewStddevAggregator()
	default:
		return nil
	}
}

// TimeBucket represents a time bucket for GROUP BY time()
type TimeBucket struct {
	Start       time.Time
	End         time.Time
	Aggregators map[string]Aggregator
}

// TruncateTime truncates a timestamp to the given interval
func TruncateTime(t time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		return t
	}
	return t.Truncate(interval)
}

// TimeBucketer groups data points into time buckets
type TimeBucketer struct {
	Interval time.Duration
	Buckets  map[int64]*TimeBucket // Key is bucket start time in nanoseconds
}

// NewTimeBucketer creates a new time bucketer
func NewTimeBucketer(interval time.Duration) *TimeBucketer {
	return &TimeBucketer{
		Interval: interval,
		Buckets:  make(map[int64]*TimeBucket),
	}
}

// GetBucket returns the bucket for a timestamp, creating it if needed
func (tb *TimeBucketer) GetBucket(t time.Time, aggNames []string) *TimeBucket {
	bucketStart := TruncateTime(t, tb.Interval)
	key := bucketStart.UnixNano()

	bucket, exists := tb.Buckets[key]
	if !exists {
		bucket = &TimeBucket{
			Start:       bucketStart,
			End:         bucketStart.Add(tb.Interval),
			Aggregators: make(map[string]Aggregator),
		}
		for _, name := range aggNames {
			bucket.Aggregators[name] = NewAggregator(name)
		}
		tb.Buckets[key] = bucket
	}
	return bucket
}

// SortedBuckets returns buckets sorted by start time
func (tb *TimeBucketer) SortedBuckets() []*TimeBucket {
	keys := make([]int64, 0, len(tb.Buckets))
	for k := range tb.Buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	buckets := make([]*TimeBucket, len(keys))
	for i, k := range keys {
		buckets[i] = tb.Buckets[k]
	}
	return buckets
}
