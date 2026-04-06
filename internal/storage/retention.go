package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Note: RetentionPolicy is defined in types.go

// DefaultRetentionPolicy returns a policy with sensible defaults
func DefaultRetentionPolicy() *RetentionPolicy {
	return &RetentionPolicy{
		Name:              "autogen",
		Duration:          0, // infinite
		ShardDuration:     7 * 24 * time.Hour,
		ReplicationFactor: 1,
		Default:           true,
	}
}

// RetentionManager manages retention policies for a database
type RetentionManager struct {
	mu sync.RWMutex

	// Path to store policy metadata
	path string

	// Policies by name
	policies map[string]*RetentionPolicy

	// Database reference (for shard access)
	database *DatabaseState

	// Engine reference (for downsampling writes)
	engine *Engine

	// Background enforcement
	enforceInterval time.Duration
	stopCh          chan struct{}
	doneCh          chan struct{}

	// Track which shards have been downsampled to avoid duplicates
	downsampledShards map[uint64]bool
}

// RetentionManagerConfig holds configuration for the retention manager
type RetentionManagerConfig struct {
	Path            string
	EnforceInterval time.Duration
	Engine          *Engine
}

// NewRetentionManager creates a new retention manager
func NewRetentionManager(config RetentionManagerConfig, database *DatabaseState) (*RetentionManager, error) {
	rm := &RetentionManager{
		path:              config.Path,
		policies:          make(map[string]*RetentionPolicy),
		database:          database,
		engine:            config.Engine,
		enforceInterval:   config.EnforceInterval,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		downsampledShards: make(map[uint64]bool),
	}

	if rm.enforceInterval == 0 {
		rm.enforceInterval = 30 * time.Minute
	}

	// Load existing policies
	if err := rm.load(); err != nil {
		return nil, err
	}

	// Create default policy if none exist
	if len(rm.policies) == 0 {
		defaultPolicy := DefaultRetentionPolicy()
		rm.policies[defaultPolicy.Name] = defaultPolicy
		if err := rm.save(); err != nil {
			return nil, err
		}
	}

	return rm, nil
}

// load loads policies from disk
func (rm *RetentionManager) load() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	policiesPath := filepath.Join(rm.path, "retention.json")

	data, err := os.ReadFile(policiesPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read retention policies: %w", err)
	}

	var policies []*RetentionPolicy
	if err := json.Unmarshal(data, &policies); err != nil {
		return fmt.Errorf("parse retention policies: %w", err)
	}

	for _, p := range policies {
		rm.policies[p.Name] = p
	}

	return nil
}

// save saves policies to disk
func (rm *RetentionManager) save() error {
	if err := os.MkdirAll(rm.path, 0755); err != nil {
		return fmt.Errorf("create retention dir: %w", err)
	}

	policies := make([]*RetentionPolicy, 0, len(rm.policies))
	for _, p := range rm.policies {
		policies = append(policies, p)
	}

	data, err := json.MarshalIndent(policies, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal retention policies: %w", err)
	}

	policiesPath := filepath.Join(rm.path, "retention.json")
	if err := os.WriteFile(policiesPath, data, 0644); err != nil {
		return fmt.Errorf("write retention policies: %w", err)
	}

	return nil
}

// CreatePolicy creates a new retention policy
func (rm *RetentionManager) CreatePolicy(policy *RetentionPolicy) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.policies[policy.Name]; exists {
		return fmt.Errorf("retention policy %q already exists", policy.Name)
	}

	if err := validatePolicy(policy); err != nil {
		return err
	}

	// If this is marked as default, unset other defaults
	if policy.Default {
		for _, p := range rm.policies {
			p.Default = false
		}
	}

	rm.policies[policy.Name] = policy
	return rm.save()
}

// AlterPolicy modifies an existing retention policy
func (rm *RetentionManager) AlterPolicy(name string, updates *RetentionPolicy) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	existing, exists := rm.policies[name]
	if !exists {
		return fmt.Errorf("retention policy %q not found", name)
	}

	// Apply updates
	if updates.Duration != 0 {
		existing.Duration = updates.Duration
	}
	if updates.ShardDuration != 0 {
		existing.ShardDuration = updates.ShardDuration
	}
	if updates.ReplicationFactor > 0 {
		existing.ReplicationFactor = updates.ReplicationFactor
	}
	if updates.Default {
		// Unset other defaults
		for _, p := range rm.policies {
			p.Default = false
		}
		existing.Default = true
	}

	if err := validatePolicy(existing); err != nil {
		return err
	}

	return rm.save()
}

// DropPolicy removes a retention policy
func (rm *RetentionManager) DropPolicy(name string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	policy, exists := rm.policies[name]
	if !exists {
		return fmt.Errorf("retention policy %q not found", name)
	}

	if policy.Default && len(rm.policies) > 1 {
		return fmt.Errorf("cannot drop default retention policy while other policies exist")
	}

	delete(rm.policies, name)
	return rm.save()
}

// GetPolicy returns a retention policy by name
func (rm *RetentionManager) GetPolicy(name string) (*RetentionPolicy, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	policy, exists := rm.policies[name]
	if !exists {
		return nil, false
	}
	// Return a copy
	copy := *policy
	return &copy, true
}

// GetDefaultPolicy returns the default retention policy
func (rm *RetentionManager) GetDefaultPolicy() *RetentionPolicy {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, p := range rm.policies {
		if p.Default {
			copy := *p
			return &copy
		}
	}

	// Shouldn't happen, but return first policy as fallback
	for _, p := range rm.policies {
		copy := *p
		return &copy
	}

	return nil
}

// ListPolicies returns all retention policies
func (rm *RetentionManager) ListPolicies() []*RetentionPolicy {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	policies := make([]*RetentionPolicy, 0, len(rm.policies))
	for _, p := range rm.policies {
		copy := *p
		policies = append(policies, &copy)
	}
	return policies
}

// StartEnforcement starts the background enforcement goroutine
func (rm *RetentionManager) StartEnforcement() {
	go rm.enforceLoop()
}

// StopEnforcement stops the background enforcement goroutine
func (rm *RetentionManager) StopEnforcement() {
	close(rm.stopCh)
	<-rm.doneCh
}

// enforceLoop runs periodic enforcement
func (rm *RetentionManager) enforceLoop() {
	defer close(rm.doneCh)

	ticker := time.NewTicker(rm.enforceInterval)
	defer ticker.Stop()

	// Run immediately on start
	rm.Enforce()

	for {
		select {
		case <-ticker.C:
			rm.Enforce()
		case <-rm.stopCh:
			return
		}
	}
}

// EnforceResult holds stats from an enforcement run
type EnforceResult struct {
	Dropped            int
	DownsampledShards  int
	DownsampledPoints  int
}

// Enforce enforces all retention policies, downsampling and dropping expired shards
func (rm *RetentionManager) Enforce() int {
	result := rm.EnforceWithResult()
	return result.Dropped
}

// EnforceWithResult enforces retention policies and returns detailed stats
func (rm *RetentionManager) EnforceWithResult() EnforceResult {
	result := EnforceResult{}

	rm.mu.RLock()
	policies := make([]*RetentionPolicy, 0, len(rm.policies))
	for _, p := range rm.policies {
		if p.Duration > 0 {
			policies = append(policies, p)
		}
	}
	rm.mu.RUnlock()

	if len(policies) == 0 {
		return result // No policies with retention limits
	}

	// Get minimum retention duration
	minRetention := policies[0].Duration
	var downsamplePolicy *RetentionPolicy
	for _, p := range policies {
		if p.Duration < minRetention {
			minRetention = p.Duration
		}
		// Find policy with downsampling enabled
		if p.Downsample != nil && p.Downsample.Enabled {
			downsamplePolicy = p
		}
	}

	// Find shards approaching expiration
	now := time.Now()
	cutoff := now.Add(-minRetention)

	// If downsampling is configured, process shards that will expire soon
	if downsamplePolicy != nil && downsamplePolicy.Downsample != nil {
		dsResult := rm.downsampleExpiringShardsWithPolicy(downsamplePolicy, cutoff)
		result.DownsampledShards = dsResult.ShardsProcessed
		result.DownsampledPoints = dsResult.PointsWritten
	}

	// Drop expired shards
	result.Dropped = rm.dropShardsOlderThan(cutoff)

	return result
}

// DownsampleResult holds stats from a downsampling operation
type DownsampleResult struct {
	ShardsProcessed int
	PointsWritten   int
}

// downsampleExpiringShardsWithPolicy downsamples data from shards that will expire
func (rm *RetentionManager) downsampleExpiringShardsWithPolicy(policy *RetentionPolicy, cutoff time.Time) DownsampleResult {
	result := DownsampleResult{}

	if rm.database == nil || rm.engine == nil {
		return result
	}

	ds := policy.Downsample
	if ds == nil || !ds.Enabled || len(ds.AggregateFuncs) == 0 {
		return result
	}

	rm.database.mu.RLock()
	shardsToProcess := make([]*Shard, 0)
	for id, shard := range rm.database.shards {
		info := shard.Info()
		// Process shards that will expire but haven't been downsampled yet
		if info.EndTime.Before(cutoff) {
			rm.mu.RLock()
			alreadyProcessed := rm.downsampledShards[id]
			rm.mu.RUnlock()
			if !alreadyProcessed {
				shardsToProcess = append(shardsToProcess, shard)
			}
		}
	}
	rm.database.mu.RUnlock()

	// Process each shard
	for _, shard := range shardsToProcess {
		info := shard.Info()
		points := rm.downsampleShard(shard, ds)
		if len(points) > 0 {
			// Write downsampled data
			if err := rm.engine.WriteBatch(rm.database.name, points); err == nil {
				result.ShardsProcessed++
				result.PointsWritten += len(points)
			}
		}

		// Mark shard as downsampled
		rm.mu.Lock()
		rm.downsampledShards[info.ID] = true
		rm.mu.Unlock()
	}

	return result
}

// downsampleShard reads data from a shard via the engine and produces downsampled points
func (rm *RetentionManager) downsampleShard(shard *Shard, config *RetentionDownsampleConfig) []*DataPoint {
	if rm.engine == nil {
		return nil
	}

	info := shard.Info()

	// Read all data from the shard through the engine
	startTime := info.StartTime.UnixNano()
	endTime := info.EndTime.UnixNano()

	// Get the shard's buffer data directly
	// Note: This is a simplified approach - for full production use, we'd query through
	// the engine with known measurements
	var downsampledPoints []*DataPoint

	// Read buffer data from shard if available
	// The buffer contains recent writes that may not be flushed yet
	bufferedData := rm.readShardBufferData(shard)
	if len(bufferedData) == 0 {
		return nil
	}

	// Group data by measurement and time buckets, then aggregate
	for measurement, seriesData := range bufferedData {
		points := rm.downsampleBufferData(measurement, seriesData, config, startTime, endTime)
		downsampledPoints = append(downsampledPoints, points...)
	}

	return downsampledPoints
}

// readShardBufferData extracts buffered data from a shard
func (rm *RetentionManager) readShardBufferData(shard *Shard) map[string]map[string][][]interface{} {
	// Returns measurement -> seriesKey -> rows
	// Each row is [timestamp, field1, field2, ...]
	// For now, return empty - this will be populated when we have buffer access
	return make(map[string]map[string][][]interface{})
}

// downsampleBufferData downsamples data from the buffer
func (rm *RetentionManager) downsampleBufferData(measurement string, seriesData map[string][][]interface{}, config *RetentionDownsampleConfig, startTime, endTime int64) []*DataPoint {
	if len(seriesData) == 0 {
		return nil
	}

	destMeasurement := measurement + config.DestMeasurementSuffix
	var points []*DataPoint

	for seriesKey, rows := range seriesData {
		// Group by time buckets
		buckets := make(map[int64][]float64)

		for _, row := range rows {
			if len(row) < 2 {
				continue
			}
			ts, ok := getTimestampValue(row[0])
			if !ok || ts < startTime || ts >= endTime {
				continue
			}

			bucketTime := (ts / int64(config.GroupByInterval)) * int64(config.GroupByInterval)
			val := getFloatValueRetention(row[1])
			buckets[bucketTime] = append(buckets[bucketTime], val)
		}

		// Create aggregated points
		for bucketTime, values := range buckets {
			fields := make(Fields)
			for _, aggFunc := range config.AggregateFuncs {
				aggValue := calculateAggregateRetention(aggFunc, values)
				aggFieldName := fmt.Sprintf("value_%s", aggFunc)
				fields[aggFieldName] = NewFloatField(aggValue)
			}

			// Parse tags from series key
			var tags Tags
			if idx := strings.Index(seriesKey, ","); idx > 0 {
				tagPart := seriesKey[idx+1:]
				tags = parseTags(tagPart)
			}

			if len(fields) > 0 {
				points = append(points, &DataPoint{
					Measurement: destMeasurement,
					Tags:        tags,
					Fields:      fields,
					Timestamp:   bucketTime,
				})
			}
		}
	}

	return points
}

// parseTags parses a tag string like "host=server1,region=us-west"
func parseTags(s string) Tags {
	tags := make(Tags)
	if s == "" {
		return tags
	}
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return tags
}

// calculateAggregateRetention calculates an aggregate value for retention downsampling
func calculateAggregateRetention(fn string, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	switch fn {
	case "mean", "avg":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case "count":
		return float64(len(values))

	case "min":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case "max":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	default:
		// Default to mean
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}
}

// getFloatValueRetention extracts a float from an interface value
func getFloatValueRetention(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

// dropShardsOlderThan drops all shards that ended before the cutoff time
func (rm *RetentionManager) dropShardsOlderThan(cutoff time.Time) int {
	if rm.database == nil {
		return 0
	}

	rm.database.mu.Lock()
	defer rm.database.mu.Unlock()

	dropped := 0
	for id, shard := range rm.database.shards {
		info := shard.Info()
		if info.EndTime.Before(cutoff) {
			// Close the shard
			shard.Close()

			// Remove from disk
			shardDir := filepath.Join(rm.database.path, "shards", fmt.Sprintf("shard_%d", id))
			os.RemoveAll(shardDir)

			// Remove from map
			delete(rm.database.shards, id)

			// Clean up downsampled tracking
			rm.mu.Lock()
			delete(rm.downsampledShards, id)
			rm.mu.Unlock()

			dropped++
		}
	}

	return dropped
}

// GetShardGroupDuration returns the shard duration for a policy
func (rm *RetentionManager) GetShardGroupDuration(policyName string) time.Duration {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if policy, exists := rm.policies[policyName]; exists {
		return policy.ShardDuration
	}

	// Return default
	if def := rm.GetDefaultPolicy(); def != nil {
		return def.ShardDuration
	}

	return 7 * 24 * time.Hour
}

// IsExpired checks if a time is older than the retention duration
func (rm *RetentionManager) IsExpired(policyName string, t time.Time) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	policy, exists := rm.policies[policyName]
	if !exists {
		return false
	}

	if policy.Duration == 0 {
		return false // infinite retention
	}

	return t.Before(time.Now().Add(-policy.Duration))
}

// validatePolicy validates a retention policy
func validatePolicy(p *RetentionPolicy) error {
	if p.Name == "" {
		return fmt.Errorf("retention policy name cannot be empty")
	}

	if p.Duration < 0 {
		return fmt.Errorf("retention policy duration cannot be negative")
	}

	if p.ShardDuration < 0 {
		return fmt.Errorf("shard duration cannot be negative")
	}

	if p.ShardDuration == 0 {
		p.ShardDuration = calculateDefaultShardDuration(p.Duration)
	}

	if p.ReplicationFactor < 0 {
		return fmt.Errorf("replication factor cannot be negative")
	}

	if p.ReplicationFactor == 0 {
		p.ReplicationFactor = 1
	}

	// Shard duration should be reasonable relative to retention
	if p.Duration > 0 && p.ShardDuration > p.Duration {
		return fmt.Errorf("shard duration cannot be longer than retention duration")
	}

	// Validate downsample config if present
	if p.Downsample != nil && p.Downsample.Enabled {
		if err := validateDownsampleConfig(p.Downsample); err != nil {
			return fmt.Errorf("invalid downsample config: %w", err)
		}
	}

	return nil
}

// validateDownsampleConfig validates a downsample configuration
func validateDownsampleConfig(ds *RetentionDownsampleConfig) error {
	if ds.DestMeasurementSuffix == "" {
		return fmt.Errorf("destination measurement suffix cannot be empty")
	}

	if len(ds.AggregateFuncs) == 0 {
		return fmt.Errorf("at least one aggregate function is required")
	}

	if ds.GroupByInterval <= 0 {
		return fmt.Errorf("group by interval must be positive")
	}

	// Validate aggregate functions
	validFuncs := map[string]bool{
		"mean": true, "avg": true, "sum": true, "count": true,
		"min": true, "max": true,
	}
	for _, fn := range ds.AggregateFuncs {
		if !validFuncs[fn] {
			return fmt.Errorf("invalid aggregate function: %s", fn)
		}
	}

	return nil
}

// calculateDefaultShardDuration calculates a reasonable shard duration
func calculateDefaultShardDuration(retention time.Duration) time.Duration {
	if retention == 0 {
		return 7 * 24 * time.Hour // 1 week for infinite retention
	}

	// Use ~1/10th of retention, clamped to reasonable bounds
	shardDuration := retention / 10

	minShard := 1 * time.Hour
	maxShard := 7 * 24 * time.Hour

	if shardDuration < minShard {
		return minShard
	}
	if shardDuration > maxShard {
		return maxShard
	}

	return shardDuration
}
