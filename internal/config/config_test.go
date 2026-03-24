package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.BindAddr != "0.0.0.0:8086" {
		t.Errorf("expected bind addr 0.0.0.0:8086, got %s", cfg.Server.BindAddr)
	}

	if cfg.Storage.ShardDuration != 24*time.Hour {
		t.Errorf("expected shard duration 24h, got %v", cfg.Storage.ShardDuration)
	}

	if cfg.Query.QueryTimeout != 30*time.Second {
		t.Errorf("expected query timeout 30s, got %v", cfg.Query.QueryTimeout)
	}

	if cfg.Cluster.Enabled {
		t.Error("expected clustering disabled by default")
	}
}

func TestParse(t *testing.T) {
	yaml := `
server:
  bind_addr: "127.0.0.1:9086"
  max_connections: 500

storage:
  data_dir: /tmp/epoch/data
  wal_dir: /tmp/epoch/wal
  shard_duration: 168h
  wal_fsync: every_write

query:
  max_select_series: 5000
  max_select_points: 500000
  query_timeout: 60s

retention:
  check_interval: 30m

cluster:
  enabled: true
  node_id: test-node
  peers:
    - node2:8088
    - node3:8088
  replication_factor: 3

logging:
  level: debug
  format: text
`

	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if cfg.Server.BindAddr != "127.0.0.1:9086" {
		t.Errorf("expected bind addr 127.0.0.1:9086, got %s", cfg.Server.BindAddr)
	}

	if cfg.Server.MaxConnections != 500 {
		t.Errorf("expected max connections 500, got %d", cfg.Server.MaxConnections)
	}

	if cfg.Storage.DataDir != "/tmp/epoch/data" {
		t.Errorf("expected data dir /tmp/epoch/data, got %s", cfg.Storage.DataDir)
	}

	if cfg.Storage.ShardDuration != 168*time.Hour {
		t.Errorf("expected shard duration 168h, got %v", cfg.Storage.ShardDuration)
	}

	if cfg.Storage.WALFsync != "every_write" {
		t.Errorf("expected wal_fsync every_write, got %s", cfg.Storage.WALFsync)
	}

	if cfg.Query.MaxSelectSeries != 5000 {
		t.Errorf("expected max select series 5000, got %d", cfg.Query.MaxSelectSeries)
	}

	if cfg.Query.QueryTimeout != 60*time.Second {
		t.Errorf("expected query timeout 60s, got %v", cfg.Query.QueryTimeout)
	}

	if cfg.Retention.CheckInterval != 30*time.Minute {
		t.Errorf("expected check interval 30m, got %v", cfg.Retention.CheckInterval)
	}

	if !cfg.Cluster.Enabled {
		t.Error("expected clustering enabled")
	}

	if cfg.Cluster.NodeID != "test-node" {
		t.Errorf("expected node id test-node, got %s", cfg.Cluster.NodeID)
	}

	if len(cfg.Cluster.Peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(cfg.Cluster.Peers))
	}

	if cfg.Cluster.ReplicationFactor != 3 {
		t.Errorf("expected replication factor 3, got %d", cfg.Cluster.ReplicationFactor)
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("expected log level debug, got %s", cfg.Logging.Level)
	}

	if cfg.Logging.Format != "text" {
		t.Errorf("expected log format text, got %s", cfg.Logging.Format)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch.yaml")

	yaml := `
server:
  bind_addr: "0.0.0.0:8086"
storage:
  data_dir: ./data
  shard_duration: 24h
  wal_fsync: every_second
query:
  query_timeout: 30s
logging:
  level: info
  format: json
`

	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.BindAddr != "0.0.0.0:8086" {
		t.Errorf("expected bind addr 0.0.0.0:8086, got %s", cfg.Server.BindAddr)
	}
}

func TestLoadNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid default config",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "empty bind addr",
			modify:  func(c *Config) { c.Server.BindAddr = "" },
			wantErr: true,
		},
		{
			name:    "empty data dir",
			modify:  func(c *Config) { c.Storage.DataDir = "" },
			wantErr: true,
		},
		{
			name:    "shard duration too small",
			modify:  func(c *Config) { c.Storage.ShardDuration = 30 * time.Minute },
			wantErr: true,
		},
		{
			name:    "zero query timeout",
			modify:  func(c *Config) { c.Query.QueryTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "cluster enabled without node id",
			modify:  func(c *Config) { c.Cluster.Enabled = true; c.Cluster.NodeID = "" },
			wantErr: true,
		},
		{
			name:    "invalid replication factor",
			modify:  func(c *Config) { c.Cluster.ReplicationFactor = 0 },
			wantErr: true,
		},
		{
			name:    "invalid wal fsync",
			modify:  func(c *Config) { c.Storage.WALFsync = "invalid" },
			wantErr: true,
		},
		{
			name:    "invalid log level",
			modify:  func(c *Config) { c.Logging.Level = "invalid" },
			wantErr: true,
		},
		{
			name:    "invalid log format",
			modify:  func(c *Config) { c.Logging.Format = "invalid" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWALSyncMode(t *testing.T) {
	tests := []struct {
		fsync string
		want  int
	}{
		{"every_write", 0},
		{"every_second", 1},
		{"none", 2},
		{"unknown", 1}, // defaults to every_second
	}

	for _, tt := range tests {
		t.Run(tt.fsync, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Storage.WALFsync = tt.fsync
			if got := cfg.WALSyncMode(); got != tt.want {
				t.Errorf("WALSyncMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseInvalidYAML(t *testing.T) {
	_, err := Parse([]byte("invalid: yaml: content: ["))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestPartialConfig(t *testing.T) {
	// Only specify some fields, rest should use defaults
	yaml := `
server:
  bind_addr: "localhost:9000"
`

	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Specified field
	if cfg.Server.BindAddr != "localhost:9000" {
		t.Errorf("expected bind addr localhost:9000, got %s", cfg.Server.BindAddr)
	}

	// Default fields
	if cfg.Storage.DataDir != "./data" {
		t.Errorf("expected default data dir ./data, got %s", cfg.Storage.DataDir)
	}

	if cfg.Query.QueryTimeout != 30*time.Second {
		t.Errorf("expected default query timeout 30s, got %v", cfg.Query.QueryTimeout)
	}
}
