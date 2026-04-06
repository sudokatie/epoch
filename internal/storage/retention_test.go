package storage

import (
	"os"
	"testing"
	"time"
)

func setupRetentionManager(t *testing.T) (*RetentionManager, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "retention-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db := &DatabaseState{
		name:   "testdb",
		path:   tmpDir,
		shards: make(map[uint64]*Shard),
	}

	rm, err := NewRetentionManager(RetentionManagerConfig{
		Path:            tmpDir,
		EnforceInterval: time.Second,
	}, db)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create retention manager: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return rm, cleanup
}

func TestDefaultRetentionPolicy(t *testing.T) {
	policy := DefaultRetentionPolicy()

	if policy.Name != "autogen" {
		t.Errorf("got name %q, want autogen", policy.Name)
	}

	if policy.Duration != 0 {
		t.Errorf("got duration %v, want 0 (infinite)", policy.Duration)
	}

	if policy.ShardDuration != 7*24*time.Hour {
		t.Errorf("got shard duration %v, want 7 days", policy.ShardDuration)
	}

	if policy.ReplicationFactor != 1 {
		t.Errorf("got replication factor %d, want 1", policy.ReplicationFactor)
	}

	if !policy.Default {
		t.Error("expected default=true")
	}
}

func TestNewRetentionManagerCreatesDefault(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	policies := rm.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("got %d policies, want 1", len(policies))
	}

	if policies[0].Name != "autogen" {
		t.Errorf("got policy name %q, want autogen", policies[0].Name)
	}
}

func TestCreatePolicy(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	policy := &RetentionPolicy{
		Name:              "short_term",
		Duration:          24 * time.Hour,
		ShardDuration:     time.Hour,
		ReplicationFactor: 1,
	}

	if err := rm.CreatePolicy(policy); err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	// Verify it was saved
	got, ok := rm.GetPolicy("short_term")
	if !ok {
		t.Fatal("policy not found")
	}

	if got.Duration != 24*time.Hour {
		t.Errorf("got duration %v, want 24h", got.Duration)
	}
}

func TestCreatePolicyDuplicate(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	policy := &RetentionPolicy{
		Name:     "duplicate",
		Duration: 24 * time.Hour,
	}

	if err := rm.CreatePolicy(policy); err != nil {
		t.Fatalf("failed to create first policy: %v", err)
	}

	// Try to create duplicate
	err := rm.CreatePolicy(policy)
	if err == nil {
		t.Error("expected error for duplicate policy")
	}
}

func TestCreatePolicyWithDefault(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create new policy as default
	policy := &RetentionPolicy{
		Name:     "new_default",
		Duration: 48 * time.Hour,
		Default:  true,
	}

	if err := rm.CreatePolicy(policy); err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	// Old default should no longer be default
	autogen, _ := rm.GetPolicy("autogen")
	if autogen.Default {
		t.Error("autogen should no longer be default")
	}

	// New policy should be default
	got := rm.GetDefaultPolicy()
	if got.Name != "new_default" {
		t.Errorf("got default policy %q, want new_default", got.Name)
	}
}

func TestAlterPolicy(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create a policy
	policy := &RetentionPolicy{
		Name:     "mutable",
		Duration: 24 * time.Hour,
	}
	rm.CreatePolicy(policy)

	// Alter it
	updates := &RetentionPolicy{
		Duration: 48 * time.Hour,
	}
	if err := rm.AlterPolicy("mutable", updates); err != nil {
		t.Fatalf("failed to alter policy: %v", err)
	}

	got, _ := rm.GetPolicy("mutable")
	if got.Duration != 48*time.Hour {
		t.Errorf("got duration %v, want 48h", got.Duration)
	}
}

func TestAlterPolicyNotFound(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	err := rm.AlterPolicy("nonexistent", &RetentionPolicy{Duration: time.Hour})
	if err == nil {
		t.Error("expected error for nonexistent policy")
	}
}

func TestDropPolicy(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create a non-default policy
	policy := &RetentionPolicy{
		Name:     "droppable",
		Duration: 24 * time.Hour,
	}
	rm.CreatePolicy(policy)

	// Drop it
	if err := rm.DropPolicy("droppable"); err != nil {
		t.Fatalf("failed to drop policy: %v", err)
	}

	// Verify it's gone
	_, ok := rm.GetPolicy("droppable")
	if ok {
		t.Error("policy should have been dropped")
	}
}

func TestDropDefaultPolicyFails(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create another policy first
	rm.CreatePolicy(&RetentionPolicy{Name: "other", Duration: time.Hour})

	// Try to drop default
	err := rm.DropPolicy("autogen")
	if err == nil {
		t.Error("expected error when dropping default policy with other policies present")
	}
}

func TestDropPolicyNotFound(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	err := rm.DropPolicy("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent policy")
	}
}

func TestListPolicies(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create additional policies
	rm.CreatePolicy(&RetentionPolicy{Name: "p1", Duration: time.Hour})
	rm.CreatePolicy(&RetentionPolicy{Name: "p2", Duration: 2 * time.Hour})
	rm.CreatePolicy(&RetentionPolicy{Name: "p3", Duration: 3 * time.Hour})

	policies := rm.ListPolicies()
	if len(policies) != 4 { // autogen + 3 new
		t.Errorf("got %d policies, want 4", len(policies))
	}

	// Verify all names present
	names := make(map[string]bool)
	for _, p := range policies {
		names[p.Name] = true
	}

	for _, expected := range []string{"autogen", "p1", "p2", "p3"} {
		if !names[expected] {
			t.Errorf("missing policy %q", expected)
		}
	}
}

func TestPolicyPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db := &DatabaseState{
		name:   "testdb",
		path:   tmpDir,
		shards: make(map[uint64]*Shard),
	}

	// Create manager and add policies
	rm1, err := NewRetentionManager(RetentionManagerConfig{Path: tmpDir}, db)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	rm1.CreatePolicy(&RetentionPolicy{Name: "persistent", Duration: time.Hour})

	// Create new manager from same path
	rm2, err := NewRetentionManager(RetentionManagerConfig{Path: tmpDir}, db)
	if err != nil {
		t.Fatalf("failed to reload manager: %v", err)
	}

	// Verify policy exists
	_, ok := rm2.GetPolicy("persistent")
	if !ok {
		t.Error("policy was not persisted")
	}
}

func TestGetDefaultPolicy(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	def := rm.GetDefaultPolicy()
	if def == nil {
		t.Fatal("no default policy")
	}

	if def.Name != "autogen" {
		t.Errorf("got default %q, want autogen", def.Name)
	}
}

func TestIsExpired(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create policy with 1 hour retention
	rm.CreatePolicy(&RetentionPolicy{
		Name:     "short",
		Duration: time.Hour,
	})

	// Recent time should not be expired
	recent := time.Now().Add(-30 * time.Minute)
	if rm.IsExpired("short", recent) {
		t.Error("recent time should not be expired")
	}

	// Old time should be expired
	old := time.Now().Add(-2 * time.Hour)
	if !rm.IsExpired("short", old) {
		t.Error("old time should be expired")
	}

	// Infinite retention (autogen) should never expire
	veryOld := time.Now().Add(-365 * 24 * time.Hour)
	if rm.IsExpired("autogen", veryOld) {
		t.Error("infinite retention should never expire")
	}
}

func TestGetShardGroupDuration(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	rm.CreatePolicy(&RetentionPolicy{
		Name:          "custom",
		Duration:      24 * time.Hour,
		ShardDuration: 2 * time.Hour,
	})

	dur := rm.GetShardGroupDuration("custom")
	if dur != 2*time.Hour {
		t.Errorf("got shard duration %v, want 2h", dur)
	}

	// Unknown policy should return default
	dur = rm.GetShardGroupDuration("unknown")
	if dur != 7*24*time.Hour {
		t.Errorf("got shard duration %v, want 7 days", dur)
	}
}

func TestValidatePolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetentionPolicy
		wantErr bool
	}{
		{
			name:    "empty name",
			policy:  &RetentionPolicy{Name: ""},
			wantErr: true,
		},
		{
			name:    "negative duration",
			policy:  &RetentionPolicy{Name: "test", Duration: -time.Hour},
			wantErr: true,
		},
		{
			name:    "shard longer than retention",
			policy:  &RetentionPolicy{Name: "test", Duration: time.Hour, ShardDuration: 2 * time.Hour},
			wantErr: true,
		},
		{
			name:    "valid policy",
			policy:  &RetentionPolicy{Name: "test", Duration: 24 * time.Hour, ShardDuration: time.Hour},
			wantErr: false,
		},
		{
			name:    "infinite retention",
			policy:  &RetentionPolicy{Name: "test", Duration: 0},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePolicy(tt.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCalculateDefaultShardDuration(t *testing.T) {
	tests := []struct {
		retention time.Duration
		want      time.Duration
	}{
		{0, 7 * 24 * time.Hour},                // infinite -> 1 week
		{10 * time.Hour, time.Hour},            // 10h -> 1h (min clamp)
		{70 * 24 * time.Hour, 7 * 24 * time.Hour}, // 70 days -> 7 days (max clamp)
		{30 * 24 * time.Hour, 3 * 24 * time.Hour}, // 30 days -> 3 days
	}

	for _, tt := range tests {
		got := calculateDefaultShardDuration(tt.retention)
		if got != tt.want {
			t.Errorf("calculateDefaultShardDuration(%v) = %v, want %v", tt.retention, got, tt.want)
		}
	}
}

func TestEnforceDropsExpiredShards(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "enforce-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database with shards
	db := &DatabaseState{
		name:          "testdb",
		path:          tmpDir,
		shards:        make(map[uint64]*Shard),
		shardDuration: time.Hour,
	}

	// Create some shards
	oldTime := time.Now().Add(-48 * time.Hour)
	recentTime := time.Now().Add(-30 * time.Minute)

	// Create old shard
	oldShard, err := NewShard(ShardConfig{
		Dir:       tmpDir,
		ID:        1,
		Database:  "testdb",
		StartTime: oldTime,
		EndTime:   oldTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("failed to create old shard: %v", err)
	}
	db.shards[1] = oldShard

	// Create recent shard
	recentShard, err := NewShard(ShardConfig{
		Dir:       tmpDir,
		ID:        2,
		Database:  "testdb",
		StartTime: recentTime,
		EndTime:   recentTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("failed to create recent shard: %v", err)
	}
	db.shards[2] = recentShard

	// Create retention manager with short retention
	rm, err := NewRetentionManager(RetentionManagerConfig{Path: tmpDir}, db)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Create policy with 1 hour retention
	rm.CreatePolicy(&RetentionPolicy{
		Name:     "short",
		Duration: time.Hour,
		Default:  true,
	})

	// Run enforcement
	dropped := rm.Enforce()

	if dropped != 1 {
		t.Errorf("got %d dropped shards, want 1", dropped)
	}

	// Verify old shard was removed
	if _, ok := db.shards[1]; ok {
		t.Error("old shard should have been removed")
	}

	// Verify recent shard remains
	if _, ok := db.shards[2]; !ok {
		t.Error("recent shard should still exist")
	}
}

func TestEnforceNoRetentionLimit(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Default has infinite retention, should drop nothing
	dropped := rm.Enforce()
	if dropped != 0 {
		t.Errorf("got %d dropped shards, want 0", dropped)
	}
}

func TestRetentionManagerStartStop(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	rm.StartEnforcement()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		rm.StopEnforcement()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("StopEnforcement timed out")
	}
}

func TestDownsampleConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *RetentionDownsampleConfig
		wantErr bool
	}{
		{
			name:    "nil config (disabled)",
			config:  nil,
			wantErr: false,
		},
		{
			name: "valid config",
			config: &RetentionDownsampleConfig{
				Enabled:               true,
				DestMeasurementSuffix: "_hourly",
				AggregateFuncs:        []string{"mean", "min", "max"},
				GroupByInterval:       time.Hour,
			},
			wantErr: false,
		},
		{
			name: "empty suffix",
			config: &RetentionDownsampleConfig{
				Enabled:               true,
				DestMeasurementSuffix: "",
				AggregateFuncs:        []string{"mean"},
				GroupByInterval:       time.Hour,
			},
			wantErr: true,
		},
		{
			name: "no aggregate funcs",
			config: &RetentionDownsampleConfig{
				Enabled:               true,
				DestMeasurementSuffix: "_hourly",
				AggregateFuncs:        []string{},
				GroupByInterval:       time.Hour,
			},
			wantErr: true,
		},
		{
			name: "invalid aggregate func",
			config: &RetentionDownsampleConfig{
				Enabled:               true,
				DestMeasurementSuffix: "_hourly",
				AggregateFuncs:        []string{"mean", "invalid_func"},
				GroupByInterval:       time.Hour,
			},
			wantErr: true,
		},
		{
			name: "zero interval",
			config: &RetentionDownsampleConfig{
				Enabled:               true,
				DestMeasurementSuffix: "_hourly",
				AggregateFuncs:        []string{"mean"},
				GroupByInterval:       0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &RetentionPolicy{
				Name:       "test",
				Duration:   24 * time.Hour,
				Downsample: tt.config,
			}

			err := validatePolicy(policy)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCalculateAggregateRetention(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}

	tests := []struct {
		fn   string
		want float64
	}{
		{"mean", 3.0},
		{"avg", 3.0},
		{"sum", 15.0},
		{"count", 5.0},
		{"min", 1.0},
		{"max", 5.0},
		{"unknown", 3.0}, // defaults to mean
	}

	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			got := calculateAggregateRetention(tt.fn, values)
			if got != tt.want {
				t.Errorf("calculateAggregateRetention(%q, values) = %v, want %v", tt.fn, got, tt.want)
			}
		})
	}
}

func TestCalculateAggregateRetentionEmpty(t *testing.T) {
	got := calculateAggregateRetention("mean", []float64{})
	if got != 0 {
		t.Errorf("got %v, want 0 for empty input", got)
	}
}

func TestCreatePolicyWithDownsample(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	policy := &RetentionPolicy{
		Name:     "with_downsample",
		Duration: 24 * time.Hour,
		Downsample: &RetentionDownsampleConfig{
			Enabled:               true,
			DestMeasurementSuffix: "_hourly",
			AggregateFuncs:        []string{"mean", "min", "max"},
			GroupByInterval:       time.Hour,
		},
	}

	if err := rm.CreatePolicy(policy); err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	// Verify it was saved with downsample config
	got, ok := rm.GetPolicy("with_downsample")
	if !ok {
		t.Fatal("policy not found")
	}

	if got.Downsample == nil {
		t.Fatal("downsample config not saved")
	}

	if !got.Downsample.Enabled {
		t.Error("downsample should be enabled")
	}

	if got.Downsample.DestMeasurementSuffix != "_hourly" {
		t.Errorf("got suffix %q, want _hourly", got.Downsample.DestMeasurementSuffix)
	}

	if len(got.Downsample.AggregateFuncs) != 3 {
		t.Errorf("got %d aggregate funcs, want 3", len(got.Downsample.AggregateFuncs))
	}
}

func TestEnforceWithResult(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Create policy with finite retention
	policy := &RetentionPolicy{
		Name:     "short",
		Duration: time.Hour,
		Default:  true,
	}
	rm.CreatePolicy(policy)

	// Enforce should return result struct
	result := rm.EnforceWithResult()

	// With no shards, nothing to drop or downsample
	if result.Dropped != 0 {
		t.Errorf("got %d dropped, want 0", result.Dropped)
	}
	if result.DownsampledShards != 0 {
		t.Errorf("got %d downsampled shards, want 0", result.DownsampledShards)
	}
}

func TestDownsampledShardsTracking(t *testing.T) {
	rm, cleanup := setupRetentionManager(t)
	defer cleanup()

	// Verify tracking map is initialized
	if rm.downsampledShards == nil {
		t.Fatal("downsampledShards map should be initialized")
	}

	// Track a shard
	rm.mu.Lock()
	rm.downsampledShards[1] = true
	rm.mu.Unlock()

	// Verify it's tracked
	rm.mu.RLock()
	tracked := rm.downsampledShards[1]
	rm.mu.RUnlock()

	if !tracked {
		t.Error("shard 1 should be tracked as downsampled")
	}
}
