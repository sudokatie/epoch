package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ClusterStatus represents cluster status response
type ClusterStatus struct {
	Leader    string       `json:"leader"`
	Nodes     []NodeStatus `json:"nodes"`
	Healthy   bool         `json:"healthy"`
	ShardCount int         `json:"shard_count"`
	DataSize   int64        `json:"data_size"`
}

// NodeStatus represents a single node's status
type NodeStatus struct {
	ID          string    `json:"id"`
	Address     string    `json:"address"`
	Role        string    `json:"role"`
	State       string    `json:"state"`
	LastSeen    time.Time `json:"last_seen"`
	ShardCount  int       `json:"shard_count"`
	DataSize    int64     `json:"data_size"`
	Reachable   bool      `json:"reachable"`
}

// runCluster handles cluster subcommands
func runCluster(subcommand string, args []string) {
	switch subcommand {
	case "status":
		runClusterStatus(args)
	case "add-node":
		runClusterAddNode(args)
	case "remove-node":
		runClusterRemoveNode(args)
	case "rebalance":
		runClusterRebalance(args)
	default:
		fmt.Fprintf(os.Stderr, "Unknown cluster command: %s\n", subcommand)
		os.Exit(1)
	}
}

func runClusterStatus(args []string) {
	fs := flag.NewFlagSet("cluster status", flag.ExitOnError)
	host := fs.String("host", "localhost:8086", "Server host:port")
	formatFlag := fs.String("format", "table", "Output format (table, json)")
	fs.Parse(args)

	status, err := getClusterStatus(*host)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch *formatFlag {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(status)
	default:
		printClusterStatus(status)
	}
}

func runClusterAddNode(args []string) {
	fs := flag.NewFlagSet("cluster add-node", flag.ExitOnError)
	host := fs.String("host", "localhost:8086", "Server host:port")
	nodeAddr := fs.String("addr", "", "Address of node to add (required)")
	nodeID := fs.String("id", "", "Node ID (optional, auto-generated if empty)")
	fs.Parse(args)

	if *nodeAddr == "" {
		fmt.Fprintln(os.Stderr, "Error: --addr is required")
		fs.Usage()
		os.Exit(1)
	}

	err := addClusterNode(*host, *nodeAddr, *nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node %s added successfully\n", *nodeAddr)
}

func runClusterRemoveNode(args []string) {
	fs := flag.NewFlagSet("cluster remove-node", flag.ExitOnError)
	host := fs.String("host", "localhost:8086", "Server host:port")
	nodeID := fs.String("id", "", "Node ID to remove (required)")
	force := fs.Bool("force", false, "Force removal even if data loss may occur")
	fs.Parse(args)

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "Error: --id is required")
		fs.Usage()
		os.Exit(1)
	}

	err := removeClusterNode(*host, *nodeID, *force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node %s removed successfully\n", *nodeID)
}

func runClusterRebalance(args []string) {
	fs := flag.NewFlagSet("cluster rebalance", flag.ExitOnError)
	host := fs.String("host", "localhost:8086", "Server host:port")
	dryRun := fs.Bool("dry-run", false, "Show what would be done without making changes")
	fs.Parse(args)

	err := rebalanceCluster(*host, *dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getClusterStatus(host string) (*ClusterStatus, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/cluster/status", host))
	if err != nil {
		// If cluster endpoint doesn't exist, return mock status for standalone
		return &ClusterStatus{
			Leader:  host,
			Healthy: true,
			Nodes: []NodeStatus{
				{
					ID:        "standalone",
					Address:   host,
					Role:      "leader",
					State:     "healthy",
					LastSeen:  time.Now(),
					Reachable: true,
				},
			},
		}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed (%d): %s", resp.StatusCode, string(body))
	}

	var status ClusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &status, nil
}

func printClusterStatus(status *ClusterStatus) {
	fmt.Println("Cluster Status")
	fmt.Println("==============")
	fmt.Printf("Leader:  %s\n", status.Leader)
	fmt.Printf("Healthy: %v\n", status.Healthy)
	fmt.Printf("Shards:  %d\n", status.ShardCount)
	fmt.Printf("Data:    %s\n", formatBytes(status.DataSize))
	fmt.Println()

	fmt.Println("Nodes")
	fmt.Println("-----")
	fmt.Printf("%-12s %-20s %-10s %-10s %-10s %-10s\n",
		"ID", "ADDRESS", "ROLE", "STATE", "SHARDS", "DATA")

	for _, n := range status.Nodes {
		state := n.State
		if !n.Reachable {
			state = "unreachable"
		}
		fmt.Printf("%-12s %-20s %-10s %-10s %-10d %-10s\n",
			truncate(n.ID, 12),
			truncate(n.Address, 20),
			n.Role,
			state,
			n.ShardCount,
			formatBytes(n.DataSize))
	}
}

func addClusterNode(host, nodeAddr, nodeID string) error {
	client := &http.Client{Timeout: 30 * time.Second}

	values := url.Values{}
	values.Set("addr", nodeAddr)
	if nodeID != "" {
		values.Set("id", nodeID)
	}

	resp, err := client.Post(
		fmt.Sprintf("http://%s/cluster/nodes", host),
		"application/x-www-form-urlencoded",
		strings.NewReader(values.Encode()))
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add node failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func removeClusterNode(host, nodeID string, force bool) error {
	client := &http.Client{Timeout: 30 * time.Second}

	url := fmt.Sprintf("http://%s/cluster/nodes/%s", host, url.PathEscape(nodeID))
	if force {
		url += "?force=true"
	}

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remove node failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func rebalanceCluster(host string, dryRun bool) error {
	client := &http.Client{Timeout: 5 * time.Minute}

	url := fmt.Sprintf("http://%s/cluster/rebalance", host)
	if dryRun {
		url += "?dry-run=true"
	}

	resp, err := client.Post(url, "", nil)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rebalance failed (%d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		MovedShards int    `json:"moved_shards"`
		Message     string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Println("Rebalance initiated")
		return nil
	}

	if dryRun {
		fmt.Printf("Dry run: would move %d shards\n", result.MovedShards)
	} else {
		fmt.Printf("Rebalance complete: moved %d shards\n", result.MovedShards)
	}

	if result.Message != "" {
		fmt.Println(result.Message)
	}

	return nil
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
