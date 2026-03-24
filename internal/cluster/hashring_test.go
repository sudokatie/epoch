package cluster

import (
	"fmt"
	"testing"
)

func TestNewHashRing(t *testing.T) {
	hr := NewHashRing(DefaultHashRingConfig())

	if hr == nil {
		t.Fatal("NewHashRing returned nil")
	}

	if hr.virtualNodes != 150 {
		t.Errorf("virtualNodes = %d, want 150", hr.virtualNodes)
	}

	if hr.replicationFactor != 2 {
		t.Errorf("replicationFactor = %d, want 2", hr.replicationFactor)
	}
}

func TestHashRingAddRemoveNode(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 10, ReplicationFactor: 2})

	// Add nodes
	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	if hr.NodeCount() != 3 {
		t.Errorf("NodeCount = %d, want 3", hr.NodeCount())
	}

	// Adding same node again should be no-op
	hr.AddNode("node1")
	if hr.NodeCount() != 3 {
		t.Errorf("NodeCount after duplicate = %d, want 3", hr.NodeCount())
	}

	// Remove node
	hr.RemoveNode("node2")
	if hr.NodeCount() != 2 {
		t.Errorf("NodeCount after remove = %d, want 2", hr.NodeCount())
	}

	// Remove non-existent node should be no-op
	hr.RemoveNode("node99")
	if hr.NodeCount() != 2 {
		t.Errorf("NodeCount after remove non-existent = %d, want 2", hr.NodeCount())
	}
}

func TestHashRingGetNode(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 100, ReplicationFactor: 2})

	// Empty ring
	if node := hr.GetNode("test"); node != "" {
		t.Errorf("GetNode on empty ring = %q, want empty", node)
	}

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	// Should consistently return the same node for the same key
	node1 := hr.GetNode("mykey")
	node2 := hr.GetNode("mykey")

	if node1 != node2 {
		t.Errorf("GetNode not consistent: %q != %q", node1, node2)
	}

	if node1 == "" {
		t.Error("GetNode returned empty string")
	}
}

func TestHashRingGetNodes(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 100, ReplicationFactor: 3})

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	// Get 2 nodes for replication
	nodes := hr.GetNodes("testkey", 2)
	if len(nodes) != 2 {
		t.Errorf("GetNodes(2) returned %d nodes, want 2", len(nodes))
	}

	// Check no duplicates
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Errorf("GetNodes returned duplicate node: %s", n)
		}
		seen[n] = true
	}

	// Get more nodes than available
	nodes = hr.GetNodes("testkey", 10)
	if len(nodes) != 3 {
		t.Errorf("GetNodes(10) returned %d nodes, want 3", len(nodes))
	}

	// Empty ring
	hr2 := NewHashRing(DefaultHashRingConfig())
	nodes = hr2.GetNodes("testkey", 2)
	if nodes != nil {
		t.Errorf("GetNodes on empty ring = %v, want nil", nodes)
	}
}

func TestHashRingGetNodesForShard(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 100, ReplicationFactor: 2})

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	nodes := hr.GetNodesForShard("mydb", "cpu", 12345)
	if len(nodes) != 2 {
		t.Errorf("GetNodesForShard returned %d nodes, want 2", len(nodes))
	}

	// Should be consistent
	nodes2 := hr.GetNodesForShard("mydb", "cpu", 12345)
	if len(nodes) != len(nodes2) {
		t.Error("GetNodesForShard not consistent")
	}
	for i := range nodes {
		if nodes[i] != nodes2[i] {
			t.Errorf("GetNodesForShard[%d] = %s, want %s", i, nodes2[i], nodes[i])
		}
	}
}

func TestHashRingDistribution(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 150, ReplicationFactor: 1})

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	// Generate many keys and check distribution
	distribution := make(map[string]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := hr.GetNode(key)
		distribution[node]++
	}

	// Each node should get roughly 1/3 of keys (with some variance)
	expected := numKeys / 3
	tolerance := expected / 5 // 20% tolerance

	for node, count := range distribution {
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("Node %s got %d keys, expected ~%d (tolerance %d)",
				node, count, expected, tolerance)
		}
	}
}

func TestHashRingRebalance(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 150, ReplicationFactor: 1})

	hr.AddNode("node1")
	hr.AddNode("node2")

	// Record initial assignments
	initialAssignments := make(map[string]string)
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		initialAssignments[key] = hr.GetNode(key)
	}

	// Add a third node
	hr.AddNode("node3")

	// Count how many keys moved
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		newNode := hr.GetNode(key)
		if newNode != initialAssignments[key] {
			moved++
		}
	}

	// Roughly 1/3 of keys should move to the new node
	expectedMoved := numKeys / 3
	tolerance := expectedMoved / 2 // 50% tolerance for this test

	if moved < expectedMoved-tolerance || moved > expectedMoved+tolerance {
		t.Errorf("After adding node, %d keys moved, expected ~%d", moved, expectedMoved)
	}
}

func TestHashRingNodes(t *testing.T) {
	hr := NewHashRing(DefaultHashRingConfig())

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	nodes := hr.Nodes()
	if len(nodes) != 3 {
		t.Errorf("Nodes() returned %d, want 3", len(nodes))
	}

	// Check all nodes are present
	nodeSet := make(map[string]bool)
	for _, n := range nodes {
		nodeSet[n] = true
	}

	for _, expected := range []string{"node1", "node2", "node3"} {
		if !nodeSet[expected] {
			t.Errorf("Nodes() missing %s", expected)
		}
	}
}

func TestHashRingReplicationFactor(t *testing.T) {
	hr := NewHashRing(HashRingConfig{VirtualNodes: 100, ReplicationFactor: 2})

	if hr.GetReplicationFactor() != 2 {
		t.Errorf("GetReplicationFactor = %d, want 2", hr.GetReplicationFactor())
	}

	hr.SetReplicationFactor(3)
	if hr.GetReplicationFactor() != 3 {
		t.Errorf("GetReplicationFactor after set = %d, want 3", hr.GetReplicationFactor())
	}
}

func TestDefaultHashRingConfig(t *testing.T) {
	cfg := DefaultHashRingConfig()

	if cfg.VirtualNodes != 150 {
		t.Errorf("VirtualNodes = %d, want 150", cfg.VirtualNodes)
	}

	if cfg.ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d, want 2", cfg.ReplicationFactor)
	}
}
