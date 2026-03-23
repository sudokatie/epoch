package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	// Background enforcement
	enforceInterval time.Duration
	stopCh          chan struct{}
	doneCh          chan struct{}
}

// RetentionManagerConfig holds configuration for the retention manager
type RetentionManagerConfig struct {
	Path            string
	EnforceInterval time.Duration
}

// NewRetentionManager creates a new retention manager
func NewRetentionManager(config RetentionManagerConfig, database *DatabaseState) (*RetentionManager, error) {
	rm := &RetentionManager{
		path:            config.Path,
		policies:        make(map[string]*RetentionPolicy),
		database:        database,
		enforceInterval: config.EnforceInterval,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
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

// Enforce enforces all retention policies, dropping expired shards
func (rm *RetentionManager) Enforce() int {
	rm.mu.RLock()
	policies := make([]*RetentionPolicy, 0, len(rm.policies))
	for _, p := range rm.policies {
		if p.Duration > 0 {
			policies = append(policies, p)
		}
	}
	rm.mu.RUnlock()

	if len(policies) == 0 {
		return 0 // No policies with retention limits
	}

	// Get minimum retention duration
	minRetention := policies[0].Duration
	for _, p := range policies[1:] {
		if p.Duration < minRetention {
			minRetention = p.Duration
		}
	}

	// Find and drop expired shards
	cutoff := time.Now().Add(-minRetention)
	dropped := rm.dropShardsOlderThan(cutoff)

	return dropped
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
