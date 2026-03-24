package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Registry is the global metrics registry
	Registry = prometheus.NewRegistry()

	// Write metrics
	WriteRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_write_requests_total",
			Help: "Total number of write requests",
		},
		[]string{"database", "status"},
	)

	WritePoints = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_write_points_total",
			Help: "Total number of points written",
		},
		[]string{"database"},
	)

	WriteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "epoch_write_latency_seconds",
			Help:    "Write request latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3s
		},
		[]string{"database"},
	)

	// Query metrics
	QueryRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_query_requests_total",
			Help: "Total number of query requests",
		},
		[]string{"database", "status"},
	)

	QueryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "epoch_query_latency_seconds",
			Help:    "Query request latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~30s
		},
		[]string{"database"},
	)

	QueryPointsScanned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_query_points_scanned_total",
			Help: "Total number of points scanned during queries",
		},
		[]string{"database"},
	)

	QuerySeriesScanned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_query_series_scanned_total",
			Help: "Total number of series scanned during queries",
		},
		[]string{"database"},
	)

	// Storage metrics
	StorageShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_storage_shards",
			Help: "Number of shards",
		},
		[]string{"database", "state"},
	)

	StorageSeries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_storage_series",
			Help: "Number of series",
		},
		[]string{"database"},
	)

	StoragePoints = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_storage_points",
			Help: "Number of points stored",
		},
		[]string{"database"},
	)

	StorageDiskBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_storage_disk_bytes",
			Help: "Disk space used in bytes",
		},
		[]string{"database"},
	)

	// WAL metrics
	WALWrites = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_wal_writes_total",
			Help: "Total WAL write operations",
		},
		[]string{"database"},
	)

	WALSyncs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_wal_syncs_total",
			Help: "Total WAL sync operations",
		},
		[]string{"database"},
	)

	WALSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_wal_size_bytes",
			Help: "Current WAL size in bytes",
		},
		[]string{"database"},
	)

	// Compaction metrics
	CompactionRuns = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "epoch_compaction_runs_total",
			Help: "Total compaction runs",
		},
	)

	CompactionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "epoch_compaction_duration_seconds",
			Help:    "Compaction duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~6 minutes
		},
	)

	CompactionBytesCompacted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "epoch_compaction_bytes_total",
			Help: "Total bytes compacted",
		},
	)

	// Retention metrics
	RetentionDroppedShards = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "epoch_retention_dropped_shards_total",
			Help: "Total shards dropped by retention policy",
		},
		[]string{"database", "policy"},
	)

	// Cluster metrics
	ClusterNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "epoch_cluster_nodes",
			Help: "Number of nodes in cluster",
		},
	)

	ClusterLeader = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_cluster_is_leader",
			Help: "Whether this node is the leader (1) or not (0)",
		},
		[]string{"node_id"},
	)

	ClusterReplicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "epoch_cluster_replication_lag_seconds",
			Help: "Replication lag in seconds",
		},
		[]string{"peer"},
	)

	// HTTP metrics
	HTTPRequestsInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "epoch_http_requests_in_flight",
			Help: "Number of HTTP requests currently in flight",
		},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "epoch_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	// Go runtime metrics (automatically collected by default registry)
	// We register the standard Go collector
)

var registerOnce sync.Once

// Init initializes the metrics and registers them
func Init() {
	registerOnce.Do(func() {
		// Register all metrics
		Registry.MustRegister(
			// Write metrics
			WriteRequests,
			WritePoints,
			WriteLatency,
			// Query metrics
			QueryRequests,
			QueryLatency,
			QueryPointsScanned,
			QuerySeriesScanned,
			// Storage metrics
			StorageShards,
			StorageSeries,
			StoragePoints,
			StorageDiskBytes,
			// WAL metrics
			WALWrites,
			WALSyncs,
			WALSize,
			// Compaction metrics
			CompactionRuns,
			CompactionDuration,
			CompactionBytesCompacted,
			// Retention metrics
			RetentionDroppedShards,
			// Cluster metrics
			ClusterNodes,
			ClusterLeader,
			ClusterReplicationLag,
			// HTTP metrics
			HTTPRequestsInFlight,
			HTTPRequestDuration,
		)

		// Register Go runtime metrics
		Registry.MustRegister(prometheus.NewGoCollector())
		Registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	})
}

// Handler returns an HTTP handler for the metrics endpoint
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordWrite records a write operation
func RecordWrite(database string, points int, duration time.Duration, success bool) {
	status := "success"
	if !success {
		status = "error"
	}
	WriteRequests.WithLabelValues(database, status).Inc()
	if success {
		WritePoints.WithLabelValues(database).Add(float64(points))
		WriteLatency.WithLabelValues(database).Observe(duration.Seconds())
	}
}

// RecordQuery records a query operation
func RecordQuery(database string, duration time.Duration, pointsScanned, seriesScanned int64, success bool) {
	status := "success"
	if !success {
		status = "error"
	}
	QueryRequests.WithLabelValues(database, status).Inc()
	if success {
		QueryLatency.WithLabelValues(database).Observe(duration.Seconds())
		QueryPointsScanned.WithLabelValues(database).Add(float64(pointsScanned))
		QuerySeriesScanned.WithLabelValues(database).Add(float64(seriesScanned))
	}
}

// UpdateStorageStats updates storage statistics
func UpdateStorageStats(database string, hotShards, coldShards int, series, points int64, diskBytes int64) {
	StorageShards.WithLabelValues(database, "hot").Set(float64(hotShards))
	StorageShards.WithLabelValues(database, "cold").Set(float64(coldShards))
	StorageSeries.WithLabelValues(database).Set(float64(series))
	StoragePoints.WithLabelValues(database).Set(float64(points))
	StorageDiskBytes.WithLabelValues(database).Set(float64(diskBytes))
}

// UpdateWALStats updates WAL statistics
func UpdateWALStats(database string, writes, syncs int64, sizeBytes int64) {
	WALWrites.WithLabelValues(database).Add(float64(writes))
	WALSyncs.WithLabelValues(database).Add(float64(syncs))
	WALSize.WithLabelValues(database).Set(float64(sizeBytes))
}

// RecordCompaction records a compaction operation
func RecordCompaction(duration time.Duration, bytesCompacted int64) {
	CompactionRuns.Inc()
	CompactionDuration.Observe(duration.Seconds())
	CompactionBytesCompacted.Add(float64(bytesCompacted))
}

// RecordRetentionDrop records shards dropped by retention policy
func RecordRetentionDrop(database, policy string, count int) {
	RetentionDroppedShards.WithLabelValues(database, policy).Add(float64(count))
}

// UpdateClusterStats updates cluster statistics
func UpdateClusterStats(nodeCount int, isLeader bool, nodeID string) {
	ClusterNodes.Set(float64(nodeCount))
	if isLeader {
		ClusterLeader.WithLabelValues(nodeID).Set(1)
	} else {
		ClusterLeader.WithLabelValues(nodeID).Set(0)
	}
}

// UpdateReplicationLag updates replication lag for a peer
func UpdateReplicationLag(peer string, lagSeconds float64) {
	ClusterReplicationLag.WithLabelValues(peer).Set(lagSeconds)
}
