package cluster

import (
	"context"
	"testing"
	"time"
)

func TestCoordinatorCreation(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}

	config := DefaultCoordinatorConfig()
	coord := NewCoordinator(node, config)

	if coord == nil {
		t.Fatal("NewCoordinator returned nil")
	}

	if coord.replicationFactor != config.ReplicationFactor {
		t.Errorf("replicationFactor = %d, want %d", coord.replicationFactor, config.ReplicationFactor)
	}
}

func TestCoordinatorConfigDefaults(t *testing.T) {
	config := DefaultCoordinatorConfig()

	if config.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor = %d, want 3", config.ReplicationFactor)
	}

	if config.WriteConsistency != ConsistencyQuorum {
		t.Errorf("WriteConsistency = %v, want ConsistencyQuorum", config.WriteConsistency)
	}

	if config.ReadConsistency != ConsistencyOne {
		t.Errorf("ReadConsistency = %v, want ConsistencyOne", config.ReadConsistency)
	}

	if config.WriteTimeout != 5*time.Second {
		t.Errorf("WriteTimeout = %v, want 5s", config.WriteTimeout)
	}

	if config.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %v, want 10s", config.ReadTimeout)
	}
}

func TestCoordinatorWithInvalidConfig(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}

	// Test with zero replication factor
	config := CoordinatorConfig{
		ReplicationFactor: 0,
	}

	coord := NewCoordinator(node, config)
	if coord.replicationFactor != 1 {
		t.Errorf("replicationFactor = %d, want 1 (minimum)", coord.replicationFactor)
	}

	// Test with zero timeout
	config = CoordinatorConfig{
		ReplicationFactor: 2,
		WriteTimeout:      0,
		ReadTimeout:       0,
	}

	coord = NewCoordinator(node, config)
	if coord.writeTimeout != 5*time.Second {
		t.Errorf("writeTimeout = %v, want 5s (default)", coord.writeTimeout)
	}
	if coord.readTimeout != 10*time.Second {
		t.Errorf("readTimeout = %v, want 10s (default)", coord.readTimeout)
	}
}

func TestCoordinatorHashKeyGeneration(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	key1 := coord.hashKey("db1", "cpu", map[string]string{"host": "server1"})
	key2 := coord.hashKey("db1", "cpu", map[string]string{"host": "server1"})
	key3 := coord.hashKey("db1", "cpu", map[string]string{"host": "server2"})

	// Same inputs should produce same key
	if key1 != key2 {
		t.Errorf("same inputs produced different keys: %q != %q", key1, key2)
	}

	// Different inputs should produce different keys
	if key1 == key3 {
		t.Error("different inputs produced same key")
	}
}

func TestCoordinatorHashKeyUint64(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	hash1 := coord.hashKeyUint64("db1", "cpu", map[string]string{"host": "server1"})
	hash2 := coord.hashKeyUint64("db1", "cpu", map[string]string{"host": "server1"})
	hash3 := coord.hashKeyUint64("db1", "cpu", map[string]string{"host": "server2"})

	// Same inputs should produce same hash
	if hash1 != hash2 {
		t.Errorf("same inputs produced different hashes: %d != %d", hash1, hash2)
	}

	// Different inputs should produce different hashes
	if hash1 == hash3 {
		t.Error("different inputs produced same hash")
	}
}

func TestCoordinatorAddRemoveNode(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	// Add nodes
	coord.AddNode("data-node-1")
	coord.AddNode("data-node-2")
	coord.AddNode("data-node-3")

	// Get nodes for a key
	nodes := coord.GetNodesForKey("test-key")
	if len(nodes) == 0 {
		t.Error("GetNodesForKey returned empty slice after adding nodes")
	}

	// Remove a node
	coord.RemoveNode("data-node-2")

	// Should still work
	nodes = coord.GetNodesForKey("test-key")
	// Check nodes don't include removed node
	for _, n := range nodes {
		if n == "data-node-2" {
			t.Error("removed node still returned in GetNodesForKey")
		}
	}
}

func TestCoordinatorRequiredAcks(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	tests := []struct {
		totalNodes int
		level      ConsistencyLevel
		want       int
	}{
		{3, ConsistencyOne, 1},
		{3, ConsistencyQuorum, 2},
		{3, ConsistencyAll, 3},
		{5, ConsistencyQuorum, 3},
		{7, ConsistencyQuorum, 4},
		{1, ConsistencyAll, 1},
	}

	for _, tt := range tests {
		got := coord.requiredAcks(tt.totalNodes, tt.level)
		if got != tt.want {
			t.Errorf("requiredAcks(%d, %v) = %d, want %d", tt.totalNodes, tt.level, got, tt.want)
		}
	}
}

func TestCoordinatorConsistencySettings(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	// Set write consistency
	coord.SetWriteConsistency(ConsistencyAll)
	config := coord.GetConfig()
	if config.WriteConsistency != ConsistencyAll {
		t.Errorf("WriteConsistency = %v, want ConsistencyAll", config.WriteConsistency)
	}

	// Set read consistency
	coord.SetReadConsistency(ConsistencyQuorum)
	config = coord.GetConfig()
	if config.ReadConsistency != ConsistencyQuorum {
		t.Errorf("ReadConsistency = %v, want ConsistencyQuorum", config.ReadConsistency)
	}
}

func TestCoordinatorSetReplicationFactor(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	coord.SetReplicationFactor(5)
	config := coord.GetConfig()
	if config.ReplicationFactor != 5 {
		t.Errorf("ReplicationFactor = %d, want 5", config.ReplicationFactor)
	}

	// Invalid value should be ignored
	coord.SetReplicationFactor(0)
	config = coord.GetConfig()
	if config.ReplicationFactor != 5 {
		t.Errorf("ReplicationFactor = %d, want 5 (unchanged)", config.ReplicationFactor)
	}

	coord.SetReplicationFactor(-1)
	config = coord.GetConfig()
	if config.ReplicationFactor != 5 {
		t.Errorf("ReplicationFactor = %d, want 5 (unchanged)", config.ReplicationFactor)
	}
}

func TestConsistencyLevelToString(t *testing.T) {
	tests := []struct {
		level ConsistencyLevel
		want  string
	}{
		{ConsistencyOne, "one"},
		{ConsistencyQuorum, "quorum"},
		{ConsistencyAll, "all"},
		{ConsistencyLevel(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.want {
			t.Errorf("ConsistencyLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestCoordinatorWriteWithNoAvailableNodes(t *testing.T) {
	node := &Node{
		info:  NodeInfo{ID: "node1"},
		peers: make(map[string]*Peer),
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	req := &WriteRequest{
		Database:    "testdb",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]interface{}{"value": 42.0},
		Timestamp:   time.Now().UnixNano(),
	}

	ctx := context.Background()
	_, err := coord.Write(ctx, req)
	if err == nil {
		t.Error("Write should fail with no available nodes")
	}
}

func TestCoordinatorGetReplicaNodesEmpty(t *testing.T) {
	node := &Node{
		info:  NodeInfo{ID: "node1"},
		peers: make(map[string]*Peer),
	}
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	nodes := coord.getReplicaNodes("test-key")
	if len(nodes) != 0 {
		t.Errorf("getReplicaNodes with no peers should return empty, got %d", len(nodes))
	}
}

func TestCoordinatorGetConfig(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}
	config := CoordinatorConfig{
		ReplicationFactor: 3,
		WriteConsistency:  ConsistencyAll,
		ReadConsistency:   ConsistencyQuorum,
		WriteTimeout:      10 * time.Second,
		ReadTimeout:       20 * time.Second,
	}
	coord := NewCoordinator(node, config)

	got := coord.GetConfig()

	if got.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor = %d, want 3", got.ReplicationFactor)
	}
	if got.WriteConsistency != ConsistencyAll {
		t.Errorf("WriteConsistency = %v, want ConsistencyAll", got.WriteConsistency)
	}
	if got.ReadConsistency != ConsistencyQuorum {
		t.Errorf("ReadConsistency = %v, want ConsistencyQuorum", got.ReadConsistency)
	}
	if got.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want 10s", got.WriteTimeout)
	}
	if got.ReadTimeout != 20*time.Second {
		t.Errorf("ReadTimeout = %v, want 20s", got.ReadTimeout)
	}
}

