package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	// Init should be idempotent
	Init()
	Init() // Should not panic
}

func TestHandler(t *testing.T) {
	Init()

	handler := Handler()
	if handler == nil {
		t.Fatal("Handler() returned nil")
	}

	// Make a request to the metrics endpoint
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Handler returned status %d, want 200", w.Code)
	}

	// Should contain some metrics
	body := w.Body.String()
	if !strings.Contains(body, "epoch_") {
		t.Error("Metrics response should contain epoch_ metrics")
	}
}

func TestRecordWrite(t *testing.T) {
	Init()

	// Reset metrics for this test
	WriteRequests.Reset()
	WritePoints.Reset()
	WriteLatency.Reset()

	// Record successful write - should not panic
	RecordWrite("testdb", 100, 50*time.Millisecond, true)

	// Record failed write - should not panic
	RecordWrite("testdb", 0, 10*time.Millisecond, false)
}

func TestRecordQuery(t *testing.T) {
	Init()

	QueryRequests.Reset()
	QueryLatency.Reset()
	QueryPointsScanned.Reset()
	QuerySeriesScanned.Reset()

	// Record successful query - should not panic
	RecordQuery("testdb", 100*time.Millisecond, 1000, 10, true)

	// Record failed query - should not panic
	RecordQuery("testdb", 5*time.Millisecond, 0, 0, false)
}

func TestUpdateStorageStats(t *testing.T) {
	Init()

	StorageShards.Reset()
	StorageSeries.Reset()
	StoragePoints.Reset()
	StorageDiskBytes.Reset()

	// Should not panic
	UpdateStorageStats("testdb", 5, 10, 1000, 1000000, 1024*1024*100)
}

func TestUpdateWALStats(t *testing.T) {
	Init()

	WALWrites.Reset()
	WALSyncs.Reset()
	WALSize.Reset()

	// Should not panic
	UpdateWALStats("testdb", 100, 10, 1024*1024)
}

func TestRecordCompaction(t *testing.T) {
	Init()

	// Should not panic
	RecordCompaction(5*time.Second, 1024*1024*50)
}

func TestRecordRetentionDrop(t *testing.T) {
	Init()

	RetentionDroppedShards.Reset()

	// Should not panic
	RecordRetentionDrop("testdb", "autogen", 5)
}

func TestUpdateClusterStats(t *testing.T) {
	Init()

	ClusterNodes.Set(0)
	ClusterLeader.Reset()

	// Should not panic
	UpdateClusterStats(3, true, "node1")
	UpdateClusterStats(3, false, "node2")
}

func TestUpdateReplicationLag(t *testing.T) {
	Init()

	ClusterReplicationLag.Reset()

	// Should not panic
	UpdateReplicationLag("peer1", 1.5)
}

func TestMetricsEndpointContainsAllMetrics(t *testing.T) {
	Init()

	// Record some metrics
	RecordWrite("testdb", 100, time.Millisecond, true)
	RecordQuery("testdb", time.Millisecond, 100, 5, true)
	UpdateStorageStats("testdb", 2, 5, 100, 10000, 1024*1024)
	UpdateWALStats("testdb", 50, 5, 1024*1024)
	RecordCompaction(time.Second, 1024*1024)
	RecordRetentionDrop("testdb", "rp1", 2)
	UpdateClusterStats(3, true, "node1")
	UpdateReplicationLag("peer1", 0.5)

	// Get metrics
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()

	// Check for expected metric names
	expectedMetrics := []string{
		"epoch_write_requests_total",
		"epoch_write_points_total",
		"epoch_write_latency_seconds",
		"epoch_query_requests_total",
		"epoch_query_latency_seconds",
		"epoch_storage_shards",
		"epoch_storage_series",
		"epoch_wal_size_bytes",
		"epoch_compaction_runs_total",
		"epoch_retention_dropped_shards_total",
		"epoch_cluster_nodes",
		"epoch_cluster_is_leader",
		"epoch_cluster_replication_lag_seconds",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(body, metric) {
			t.Errorf("Metrics response missing %s", metric)
		}
	}
}

func TestMetricsReset(t *testing.T) {
	Init()

	// Record some data
	RecordWrite("db1", 100, time.Millisecond, true)

	// Reset
	WriteRequests.Reset()

	// Record again
	RecordWrite("db2", 50, time.Millisecond, true)

	// Should not panic or error
}
