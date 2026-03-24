package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sudokatie/epoch/internal/config"
	"github.com/sudokatie/epoch/internal/server"
	"github.com/sudokatie/epoch/internal/storage"
)

// runServer starts the epoch database server
func runServer(bind, dataDir, configPath string) error {
	// Load configuration
	var cfg *config.Config
	var err error

	if configPath != "" {
		cfg, err = config.Load(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		fmt.Printf("Loaded configuration from %s\n", configPath)
	} else {
		cfg = config.DefaultConfig()
		// Override with command line flags
		if bind != "" {
			cfg.Server.BindAddr = bind
		}
		if dataDir != "" {
			cfg.Storage.DataDir = dataDir
			cfg.Storage.WALDir = dataDir + "/wal"
		}
	}

	// Create storage engine
	engineConfig := storage.EngineConfig{
		DataDir:         cfg.Storage.DataDir,
		WALDir:          cfg.Storage.WALDir,
		ShardDuration:   cfg.Storage.ShardDuration,
		RetentionPeriod: 7 * 24 * cfg.Storage.ShardDuration, // Default 7x shard duration
		MaxBufferSize:   10000,
		FlushInterval:   cfg.Storage.ShardDuration / 24, // Flush 24 times per shard duration
	}

	engine, err := storage.NewEngine(engineConfig)
	if err != nil {
		return fmt.Errorf("create storage engine: %w", err)
	}

	// Create HTTP server
	serverConfig := server.Config{
		Addr:         cfg.Server.BindAddr,
		QueryTimeout: cfg.Query.QueryTimeout,
	}

	srv, err := server.New(serverConfig, engine)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
		cancel()
	}()

	// Start server
	fmt.Printf("Starting epoch server on %s\n", cfg.Server.BindAddr)
	fmt.Printf("Data directory: %s\n", cfg.Storage.DataDir)
	fmt.Printf("WAL directory: %s\n", cfg.Storage.WALDir)

	if cfg.Cluster.Enabled {
		fmt.Printf("Cluster mode: enabled (node: %s)\n", cfg.Cluster.NodeID)
	} else {
		fmt.Println("Cluster mode: disabled (standalone)")
	}

	fmt.Println("Press Ctrl+C to stop")

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		// Graceful shutdown
		fmt.Println("Shutting down server...")
		if err := srv.Shutdown(context.Background()); err != nil {
			fmt.Printf("Error during shutdown: %v\n", err)
		}
		if err := engine.Close(); err != nil {
			fmt.Printf("Error closing storage engine: %v\n", err)
		}
		fmt.Println("Server stopped")
		return nil
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
		return nil
	}
}
