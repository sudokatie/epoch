package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func setupDownsampleManager(t *testing.T) (*DownsampleManager, *Engine, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "downsample-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	engine, err := NewEngine(DefaultEngineConfig(tmpDir))
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create engine: %v", err)
	}

	dm, err := NewDownsampleManager(DownsampleConfig{
		Path:     tmpDir,
		Interval: time.Second,
	}, engine)
	if err != nil {
		engine.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create downsample manager: %v", err)
	}

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return dm, engine, cleanup
}

func TestCreateCQ(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "test_cq",
		Database:          "testdb",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
		Interval:          time.Minute,
	}

	if err := dm.CreateCQ(cq); err != nil {
		t.Fatalf("failed to create CQ: %v", err)
	}

	// Verify it was saved
	got, ok := dm.GetCQ("test_cq")
	if !ok {
		t.Fatal("CQ not found")
	}

	if got.Name != "test_cq" {
		t.Errorf("got name %q, want test_cq", got.Name)
	}

	if !got.Enabled {
		t.Error("CQ should be enabled by default")
	}
}

func TestCreateCQDuplicate(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "dup",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
	}

	dm.CreateCQ(cq)

	err := dm.CreateCQ(cq)
	if err == nil {
		t.Error("expected error for duplicate CQ")
	}
}

func TestDropCQ(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "droppable",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
	}

	dm.CreateCQ(cq)

	if err := dm.DropCQ("droppable"); err != nil {
		t.Fatalf("failed to drop CQ: %v", err)
	}

	_, ok := dm.GetCQ("droppable")
	if ok {
		t.Error("CQ should have been dropped")
	}
}

func TestDropCQNotFound(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	err := dm.DropCQ("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent CQ")
	}
}

func TestEnableDisableCQ(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "toggleable",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
	}

	dm.CreateCQ(cq)

	// Disable
	if err := dm.DisableCQ("toggleable"); err != nil {
		t.Fatalf("failed to disable CQ: %v", err)
	}

	got, _ := dm.GetCQ("toggleable")
	if got.Enabled {
		t.Error("CQ should be disabled")
	}

	// Enable
	if err := dm.EnableCQ("toggleable"); err != nil {
		t.Fatalf("failed to enable CQ: %v", err)
	}

	got, _ = dm.GetCQ("toggleable")
	if !got.Enabled {
		t.Error("CQ should be enabled")
	}
}

func TestListCQs(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	baseCQ := func(name, db string) *ContinuousQuery {
		return &ContinuousQuery{
			Name:              name,
			Database:          db,
			SourceMeasurement: "cpu",
			DestMeasurement:   "cpu_1h",
			SourceField:       "value",
			AggregateFunc:     "mean",
			GroupByInterval:   time.Hour,
		}
	}

	dm.CreateCQ(baseCQ("cq1", "db1"))
	dm.CreateCQ(baseCQ("cq2", "db1"))
	dm.CreateCQ(baseCQ("cq3", "db2"))

	// List all
	all := dm.ListCQs()
	if len(all) != 3 {
		t.Errorf("got %d CQs, want 3", len(all))
	}

	// List by database
	db1 := dm.ListCQsForDatabase("db1")
	if len(db1) != 2 {
		t.Errorf("got %d CQs for db1, want 2", len(db1))
	}

	db2 := dm.ListCQsForDatabase("db2")
	if len(db2) != 1 {
		t.Errorf("got %d CQs for db2, want 1", len(db2))
	}
}

func TestCQPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cq-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine, _ := NewEngine(DefaultEngineConfig(tmpDir))
	defer engine.Close()

	// Create manager and add CQ
	dm1, _ := NewDownsampleManager(DownsampleConfig{Path: tmpDir}, engine)
	dm1.CreateCQ(&ContinuousQuery{
		Name:              "persistent",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
	})

	// Create new manager from same path
	dm2, _ := NewDownsampleManager(DownsampleConfig{Path: tmpDir}, engine)

	_, ok := dm2.GetCQ("persistent")
	if !ok {
		t.Error("CQ was not persisted")
	}
}

func TestValidateCQ(t *testing.T) {
	tests := []struct {
		name    string
		cq      *ContinuousQuery
		wantErr bool
	}{
		{
			name:    "empty name",
			cq:      &ContinuousQuery{Name: ""},
			wantErr: true,
		},
		{
			name:    "empty database",
			cq:      &ContinuousQuery{Name: "test", Database: ""},
			wantErr: true,
		},
		{
			name:    "empty source measurement",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: ""},
			wantErr: true,
		},
		{
			name:    "empty dest measurement",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: "cpu", DestMeasurement: ""},
			wantErr: true,
		},
		{
			name:    "empty source field",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: "cpu", DestMeasurement: "out", SourceField: ""},
			wantErr: true,
		},
		{
			name:    "empty aggregate func",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: "cpu", DestMeasurement: "out", SourceField: "v", AggregateFunc: ""},
			wantErr: true,
		},
		{
			name:    "invalid aggregate func",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: "cpu", DestMeasurement: "out", SourceField: "v", AggregateFunc: "invalid", GroupByInterval: time.Hour},
			wantErr: true,
		},
		{
			name:    "zero group by interval",
			cq:      &ContinuousQuery{Name: "test", Database: "db", SourceMeasurement: "cpu", DestMeasurement: "out", SourceField: "v", AggregateFunc: "mean", GroupByInterval: 0},
			wantErr: true,
		},
		{
			name: "valid CQ",
			cq: &ContinuousQuery{
				Name:              "test",
				Database:          "db",
				SourceMeasurement: "cpu",
				DestMeasurement:   "cpu_1h",
				SourceField:       "value",
				AggregateFunc:     "mean",
				GroupByInterval:   time.Hour,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCQ(tt.cq)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCQ() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCalculateAggregate(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}

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
		{"first", 1.0},
		{"last", 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			got := calculateAggregate(tt.fn, values)
			if got != tt.want {
				t.Errorf("calculateAggregate(%q) = %v, want %v", tt.fn, got, tt.want)
			}
		})
	}
}

func TestCalculateAggregateEmpty(t *testing.T) {
	got := calculateAggregate("mean", []float64{})
	if got != 0 {
		t.Errorf("expected 0 for empty values, got %v", got)
	}
}

func TestRunCQWithData(t *testing.T) {
	dm, engine, cleanup := setupDownsampleManager(t)
	defer cleanup()

	// Create database and write data
	engine.CreateDatabase("testdb")

	now := time.Now()
	baseTime := now.Add(-2 * time.Hour).Truncate(time.Hour)

	// Write hourly data
	for i := 0; i < 60; i++ {
		point := &DataPoint{
			Measurement: "cpu",
			Tags:        Tags{"host": "server1"},
			Fields:      Fields{"value": NewFloatField(float64(i))},
			Timestamp:   baseTime.Add(time.Duration(i) * time.Minute).UnixNano(),
		}
		engine.Write("testdb", point)
	}

	// Create CQ
	cq := &ContinuousQuery{
		Name:              "cpu_mean",
		Database:          "testdb",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_hourly",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
		Interval:          time.Minute,
	}
	dm.CreateCQ(cq)

	// Run CQ
	err := dm.RunCQ(context.Background(), "cpu_mean")
	if err != nil {
		t.Fatalf("failed to run CQ: %v", err)
	}

	// Check that status was updated
	status, err := dm.Status("cpu_mean")
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if status.LastRun.IsZero() {
		t.Error("LastRun should be set")
	}

	if status.LastProcessedTime.IsZero() {
		t.Error("LastProcessedTime should be set")
	}
}

func TestRunCQDisabled(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "disabled_cq",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
	}
	dm.CreateCQ(cq)
	dm.DisableCQ("disabled_cq")

	err := dm.RunCQ(context.Background(), "disabled_cq")
	if err == nil {
		t.Error("expected error when running disabled CQ")
	}
}

func TestRunCQNotFound(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	err := dm.RunCQ(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent CQ")
	}
}

func TestDownsampleManagerStartStop(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	dm.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		dm.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop timed out")
	}
}

func TestCQStatus(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	cq := &ContinuousQuery{
		Name:              "status_test",
		Database:          "db",
		SourceMeasurement: "cpu",
		DestMeasurement:   "cpu_1h",
		SourceField:       "value",
		AggregateFunc:     "mean",
		GroupByInterval:   time.Hour,
		Interval:          time.Minute,
	}
	dm.CreateCQ(cq)

	status, err := dm.Status("status_test")
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	if status.Name != "status_test" {
		t.Errorf("got name %q, want status_test", status.Name)
	}

	if !status.Enabled {
		t.Error("expected enabled")
	}

	// NextRun should be around now since it hasn't run yet
	if status.NextRun.Before(time.Now().Add(-time.Minute)) {
		t.Error("NextRun should be recent")
	}
}

func TestCQStatusNotFound(t *testing.T) {
	dm, _, cleanup := setupDownsampleManager(t)
	defer cleanup()

	_, err := dm.Status("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent CQ")
	}
}
