package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the complete server configuration
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Storage   StorageConfig   `yaml:"storage"`
	Query     QueryConfig     `yaml:"query"`
	Retention RetentionConfig `yaml:"retention"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// ServerConfig holds HTTP server settings
type ServerConfig struct {
	BindAddr       string `yaml:"bind_addr"`
	MaxConnections int    `yaml:"max_connections"`
}

// StorageConfig holds storage engine settings
type StorageConfig struct {
	DataDir       string        `yaml:"data_dir"`
	WALDir        string        `yaml:"wal_dir"`
	ShardDuration time.Duration `yaml:"shard_duration"`
	WALFsync      string        `yaml:"wal_fsync"` // "every_write", "every_second", "none"
}

// QueryConfig holds query execution settings
type QueryConfig struct {
	MaxSelectSeries int           `yaml:"max_select_series"`
	MaxSelectPoints int           `yaml:"max_select_points"`
	QueryTimeout    time.Duration `yaml:"query_timeout"`
}

// RetentionConfig holds retention policy settings
type RetentionConfig struct {
	CheckInterval time.Duration `yaml:"check_interval"`
}

// ClusterConfig holds clustering settings
type ClusterConfig struct {
	Enabled           bool     `yaml:"enabled"`
	NodeID            string   `yaml:"node_id"`
	Peers             []string `yaml:"peers"`
	ReplicationFactor int      `yaml:"replication_factor"`
}

// LoggingConfig holds logging settings
type LoggingConfig struct {
	Level  string `yaml:"level"`  // "debug", "info", "warn", "error"
	Format string `yaml:"format"` // "json", "text"
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			BindAddr:       "0.0.0.0:8086",
			MaxConnections: 1000,
		},
		Storage: StorageConfig{
			DataDir:       "./data",
			WALDir:        "./data/wal",
			ShardDuration: 24 * time.Hour,
			WALFsync:      "every_second",
		},
		Query: QueryConfig{
			MaxSelectSeries: 10000,
			MaxSelectPoints: 1000000,
			QueryTimeout:    30 * time.Second,
		},
		Retention: RetentionConfig{
			CheckInterval: 1 * time.Hour,
		},
		Cluster: ClusterConfig{
			Enabled:           false,
			NodeID:            "node1",
			Peers:             []string{},
			ReplicationFactor: 2,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// Load reads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	return Parse(data)
}

// Parse parses YAML configuration data
func Parse(data []byte) (*Config, error) {
	cfg := DefaultConfig()

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// Validate checks the configuration for errors
func (c *Config) Validate() error {
	if c.Server.BindAddr == "" {
		return fmt.Errorf("server.bind_addr is required")
	}

	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
	}

	if c.Storage.ShardDuration < time.Hour {
		return fmt.Errorf("storage.shard_duration must be at least 1h")
	}

	if c.Query.QueryTimeout <= 0 {
		return fmt.Errorf("query.query_timeout must be positive")
	}

	if c.Cluster.Enabled && c.Cluster.NodeID == "" {
		return fmt.Errorf("cluster.node_id is required when clustering is enabled")
	}

	if c.Cluster.ReplicationFactor < 1 {
		return fmt.Errorf("cluster.replication_factor must be at least 1")
	}

	validFsync := map[string]bool{"every_write": true, "every_second": true, "none": true}
	if !validFsync[c.Storage.WALFsync] {
		return fmt.Errorf("storage.wal_fsync must be 'every_write', 'every_second', or 'none'")
	}

	validLevel := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevel[c.Logging.Level] {
		return fmt.Errorf("logging.level must be 'debug', 'info', 'warn', or 'error'")
	}

	validFormat := map[string]bool{"json": true, "text": true}
	if !validFormat[c.Logging.Format] {
		return fmt.Errorf("logging.format must be 'json' or 'text'")
	}

	return nil
}

// WALSyncMode returns the WAL sync mode as a typed value
func (c *Config) WALSyncMode() int {
	switch c.Storage.WALFsync {
	case "every_write":
		return 0
	case "every_second":
		return 1
	case "none":
		return 2
	default:
		return 1
	}
}
