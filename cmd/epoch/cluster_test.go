package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetClusterStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		status := ClusterStatus{
			Leader:     "node1",
			Healthy:    true,
			ShardCount: 10,
			DataSize:   1024 * 1024 * 100,
			Nodes: []NodeStatus{
				{
					ID:         "node1",
					Address:    "localhost:8086",
					Role:       "leader",
					State:      "active",
					LastSeen:   time.Now(),
					ShardCount: 5,
					DataSize:   50 * 1024 * 1024,
					Reachable:  true,
				},
				{
					ID:         "node2",
					Address:    "localhost:8087",
					Role:       "follower",
					State:      "active",
					LastSeen:   time.Now(),
					ShardCount: 5,
					DataSize:   50 * 1024 * 1024,
					Reachable:  true,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	status, err := getClusterStatus(host)
	if err != nil {
		t.Fatalf("getClusterStatus error: %v", err)
	}

	if status.Leader != "node1" {
		t.Errorf("Leader = %q, want %q", status.Leader, "node1")
	}
	if !status.Healthy {
		t.Error("expected cluster to be healthy")
	}
	if len(status.Nodes) != 2 {
		t.Errorf("Nodes count = %d, want 2", len(status.Nodes))
	}
}

func TestGetClusterStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	_, err := getClusterStatus(host)
	if err == nil {
		t.Error("expected error from getClusterStatus")
	}
}

func TestAddClusterNode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/nodes" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"node3","message":"node added"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := addClusterNode(host, "localhost:8088", "node3")
	if err != nil {
		t.Errorf("addClusterNode error: %v", err)
	}
}

func TestAddClusterNodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"node already exists"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := addClusterNode(host, "localhost:8088", "node3")
	if err == nil {
		t.Error("expected error from addClusterNode")
	}
}

func TestRemoveClusterNode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/cluster/nodes/") {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"node removed"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := removeClusterNode(host, "node2", false)
	if err != nil {
		t.Errorf("removeClusterNode error: %v", err)
	}
}

func TestRemoveClusterNodeForce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("force") != "true" {
			t.Error("expected force=true")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := removeClusterNode(host, "node2", true)
	if err != nil {
		t.Errorf("removeClusterNode force error: %v", err)
	}
}

func TestRemoveClusterNodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"node not found"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := removeClusterNode(host, "unknown", false)
	if err == nil {
		t.Error("expected error from removeClusterNode")
	}
}

func TestRebalanceCluster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/rebalance" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"rebalance started","shards_moved":5}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := rebalanceCluster(host, false)
	if err != nil {
		t.Errorf("rebalanceCluster error: %v", err)
	}
}

func TestRebalanceClusterDryRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/rebalance" {
			http.NotFound(w, r)
			return
		}
		// Verify dry-run parameter (with hyphen)
		if r.URL.Query().Get("dry-run") != "true" {
			t.Error("expected dry-run=true")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"dry run","shards_to_move":3}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := rebalanceCluster(host, true)
	if err != nil {
		t.Errorf("rebalanceCluster dry run error: %v", err)
	}
}

func TestRebalanceClusterError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"cluster unhealthy"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	err := rebalanceCluster(host, false)
	if err == nil {
		t.Error("expected error from rebalanceCluster")
	}
}

func TestPrintClusterStatus(t *testing.T) {
	status := &ClusterStatus{
		Leader:     "node1",
		Healthy:    true,
		ShardCount: 10,
		DataSize:   1024 * 1024 * 100,
		Nodes: []NodeStatus{
			{
				ID:         "node1",
				Address:    "localhost:8086",
				Role:       "leader",
				State:      "active",
				LastSeen:   time.Now(),
				ShardCount: 5,
				DataSize:   50 * 1024 * 1024,
				Reachable:  true,
			},
		},
	}

	// Just verify it doesn't panic
	printClusterStatus(status)
}

func TestPrintClusterStatusUnhealthy(t *testing.T) {
	status := &ClusterStatus{
		Leader:     "node1",
		Healthy:    false,
		ShardCount: 5,
		DataSize:   50 * 1024 * 1024,
		Nodes: []NodeStatus{
			{
				ID:         "node1",
				Address:    "localhost:8086",
				Role:       "leader",
				State:      "active",
				LastSeen:   time.Now(),
				Reachable:  true,
			},
			{
				ID:         "node2",
				Address:    "localhost:8087",
				Role:       "follower",
				State:      "unreachable",
				LastSeen:   time.Now().Add(-5 * time.Minute),
				Reachable:  false,
			},
		},
	}

	printClusterStatus(status)
}

func TestRunClusterStatusFlags(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := ClusterStatus{
			Leader:  "node1",
			Healthy: true,
		}
		json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	// Test with --help
	runClusterStatus([]string{"--help"})

	// Test with JSON format
	runClusterStatus([]string{"-host", host, "-format", "json"})
}

func TestRunClusterAddNodeFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"node3"}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	// Test with --help
	runClusterAddNode([]string{"--help"})

	// Test adding node
	runClusterAddNode([]string{"-host", host, "-addr", "localhost:8088"})
}

func TestRunClusterRemoveNodeFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	// Test with --help
	runClusterRemoveNode([]string{"--help"})

	// Test removing node
	runClusterRemoveNode([]string{"-host", host, "-id", "node2"})
}

func TestRunClusterRebalanceFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"shards_moved":0}`))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	// Test with --help
	runClusterRebalance([]string{"--help"})

	// Test rebalance with dry run
	runClusterRebalance([]string{"-host", host, "-dry-run"})

	// Test rebalance
	runClusterRebalance([]string{"-host", host})
}

func TestRunClusterSubcommands(t *testing.T) {
	// Test help
	runCluster("help", []string{})

	// Test unknown command (won't exit due to test setup)
	// runCluster("unknown", []string{}) - this would call os.Exit
}

func TestPrintClusterHelp(t *testing.T) {
	// Just verify it doesn't panic
	printClusterHelp()
}

func TestClusterStatusTypes(t *testing.T) {
	// Test JSON marshaling/unmarshaling
	status := ClusterStatus{
		Leader:     "node1",
		Healthy:    true,
		ShardCount: 10,
		DataSize:   1000000,
		Nodes: []NodeStatus{
			{
				ID:         "node1",
				Address:    "localhost:8086",
				Role:       "leader",
				State:      "active",
				LastSeen:   time.Now(),
				ShardCount: 5,
				DataSize:   500000,
				Reachable:  true,
			},
		},
	}

	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded ClusterStatus
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Leader != status.Leader {
		t.Errorf("Leader mismatch: %q vs %q", decoded.Leader, status.Leader)
	}
	if decoded.Healthy != status.Healthy {
		t.Error("Healthy mismatch")
	}
	if len(decoded.Nodes) != len(status.Nodes) {
		t.Errorf("Nodes count mismatch: %d vs %d", len(decoded.Nodes), len(status.Nodes))
	}
}

func TestNodeStatusFields(t *testing.T) {
	node := NodeStatus{
		ID:         "node1",
		Address:    "localhost:8086",
		Role:       "leader",
		State:      "active",
		LastSeen:   time.Now(),
		ShardCount: 5,
		DataSize:   1024 * 1024,
		Reachable:  true,
	}

	if node.ID != "node1" {
		t.Errorf("ID = %q, want %q", node.ID, "node1")
	}
	if node.Role != "leader" {
		t.Errorf("Role = %q, want %q", node.Role, "leader")
	}
	if !node.Reachable {
		t.Error("expected node to be reachable")
	}
}

func TestRunClusterWithMockServer(t *testing.T) {
	// Setup mock server for all cluster operations
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cluster/status":
			json.NewEncoder(w).Encode(ClusterStatus{Leader: "node1", Healthy: true})
		case r.URL.Path == "/cluster/nodes" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusOK)
		case strings.HasPrefix(r.URL.Path, "/cluster/nodes/") && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/cluster/rebalance":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]int{"moved_shards": 0})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	// Test status subcommand
	runCluster("status", []string{"-host", host})

	// Test add-node subcommand
	runCluster("add-node", []string{"-host", host, "-addr", "localhost:8088"})

	// Test remove-node subcommand
	runCluster("remove-node", []string{"-host", host, "-id", "node2"})

	// Test rebalance subcommand
	runCluster("rebalance", []string{"-host", host})

	// Test help subcommand
	runCluster("help", []string{})
	runCluster("-h", []string{})
	runCluster("--help", []string{})
}

func TestTruncateCluster(t *testing.T) {
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"hello", 5, "hello"},
		{"hi", 5, "hi"},
		{"", 5, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "..."},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := truncate(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
		})
	}
}

func TestFormatBytesCluster(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatBytes(tt.input)
			if got != tt.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
