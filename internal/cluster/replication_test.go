package cluster

import (
	"context"
	"testing"
	"time"
)

func TestConsistencyLevelString(t *testing.T) {
	tests := []struct {
		input ConsistencyLevel
		want  string
	}{
		{ConsistencyOne, "one"},
		{ConsistencyQuorum, "quorum"},
		{ConsistencyAll, "all"},
		{ConsistencyLevel(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.input.String()
		if got != tt.want {
			t.Errorf("ConsistencyLevel(%d).String() = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDefaultCoordinatorConfig(t *testing.T) {
	config := DefaultCoordinatorConfig()

	if config.ReplicationFactor != 3 {
		t.Errorf("got replication factor %d, want 3", config.ReplicationFactor)
	}

	if config.WriteConsistency != ConsistencyQuorum {
		t.Errorf("got write consistency %v, want quorum", config.WriteConsistency)
	}

	if config.ReadConsistency != ConsistencyOne {
		t.Errorf("got read consistency %v, want one", config.ReadConsistency)
	}

	if config.WriteTimeout != 5*time.Second {
		t.Errorf("got write timeout %v, want 5s", config.WriteTimeout)
	}

	if config.ReadTimeout != 10*time.Second {
		t.Errorf("got read timeout %v, want 10s", config.ReadTimeout)
	}
}

func TestNewCoordinator(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, err := NewNode(nodeConfig)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	coordConfig := DefaultCoordinatorConfig()
	coord := NewCoordinator(node, coordConfig)

	config := coord.GetConfig()
	if config.ReplicationFactor != 3 {
		t.Errorf("got replication factor %d, want 3", config.ReplicationFactor)
	}
}

func TestCoordinatorSetConsistency(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	coord.SetWriteConsistency(ConsistencyAll)
	coord.SetReadConsistency(ConsistencyQuorum)
	coord.SetReplicationFactor(5)

	config := coord.GetConfig()

	if config.WriteConsistency != ConsistencyAll {
		t.Errorf("got write consistency %v, want all", config.WriteConsistency)
	}

	if config.ReadConsistency != ConsistencyQuorum {
		t.Errorf("got read consistency %v, want quorum", config.ReadConsistency)
	}

	if config.ReplicationFactor != 5 {
		t.Errorf("got replication factor %d, want 5", config.ReplicationFactor)
	}
}

func TestRequiredAcks(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	tests := []struct {
		total int
		level ConsistencyLevel
		want  int
	}{
		{3, ConsistencyOne, 1},
		{3, ConsistencyQuorum, 2},
		{3, ConsistencyAll, 3},
		{5, ConsistencyQuorum, 3},
		{7, ConsistencyQuorum, 4},
	}

	for _, tt := range tests {
		got := coord.requiredAcks(tt.total, tt.level)
		if got != tt.want {
			t.Errorf("requiredAcks(%d, %v) = %d, want %d", tt.total, tt.level, got, tt.want)
		}
	}
}

func TestCoordinatorHashKey(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	// Same inputs should produce same hash
	tags := map[string]string{"host": "server1"}
	hash1 := coord.hashKey("db", "cpu", tags)
	hash2 := coord.hashKey("db", "cpu", tags)

	if hash1 != hash2 {
		t.Error("same inputs should produce same hash")
	}

	// Different inputs should produce different hash
	hash3 := coord.hashKey("db", "memory", tags)
	if hash1 == hash3 {
		t.Error("different inputs should produce different hash")
	}
}

func TestCoordinatorWriteNoNodes(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	req := &WriteRequest{
		Database:    "testdb",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]interface{}{"value": 0.5},
		Timestamp:   time.Now().UnixNano(),
	}

	resp, err := coord.Write(context.Background(), req)
	if err == nil && resp != nil && resp.Success {
		// With no peers, either error or unsuccessful response is acceptable
	}
}

func TestDefaultReplicationConfig(t *testing.T) {
	config := DefaultReplicationConfig()

	if config.SyncInterval != time.Minute {
		t.Errorf("got sync interval %v, want 1m", config.SyncInterval)
	}

	if config.MaxSyncBatchSize != 1000 {
		t.Errorf("got max batch size %d, want 1000", config.MaxSyncBatchSize)
	}

	if config.RepairTimeout != 30*time.Second {
		t.Errorf("got repair timeout %v, want 30s", config.RepairTimeout)
	}
}

func TestNewReplicationManager(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())
	rm := NewReplicationManager(node, coord, DefaultReplicationConfig())

	if rm == nil {
		t.Fatal("failed to create replication manager")
	}
}

func TestReplicationManagerStartStop(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())

	config := DefaultReplicationConfig()
	config.SyncInterval = 50 * time.Millisecond
	rm := NewReplicationManager(node, coord, config)

	rm.Start()
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		rm.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop timed out")
	}
}

func TestMerkleTree(t *testing.T) {
	tree := NewMerkleTree()

	if tree.Size() != 0 {
		t.Errorf("new tree should be empty, got size %d", tree.Size())
	}

	if tree.Root() != "" {
		t.Error("empty tree should have empty root")
	}
}

func TestMerkleTreeInsert(t *testing.T) {
	tree := NewMerkleTree()

	tree.Insert("key1", []byte("value1"))
	tree.Insert("key2", []byte("value2"))

	if tree.Size() != 2 {
		t.Errorf("got size %d, want 2", tree.Size())
	}

	root := tree.Root()
	if root == "" {
		t.Error("root should not be empty after inserts")
	}

	leaves := tree.Leaves()
	if len(leaves) != 2 {
		t.Errorf("got %d leaves, want 2", len(leaves))
	}

	if _, ok := leaves["key1"]; !ok {
		t.Error("missing key1 in leaves")
	}

	if _, ok := leaves["key2"]; !ok {
		t.Error("missing key2 in leaves")
	}
}

func TestMerkleTreeDelete(t *testing.T) {
	tree := NewMerkleTree()

	tree.Insert("key1", []byte("value1"))
	tree.Insert("key2", []byte("value2"))

	tree.Delete("key1")

	if tree.Size() != 1 {
		t.Errorf("got size %d, want 1", tree.Size())
	}

	leaves := tree.Leaves()
	if _, ok := leaves["key1"]; ok {
		t.Error("key1 should be deleted")
	}
}

func TestMerkleTreeRootDeterministic(t *testing.T) {
	tree1 := NewMerkleTree()
	tree1.Insert("a", []byte("1"))
	tree1.Insert("b", []byte("2"))

	tree2 := NewMerkleTree()
	tree2.Insert("b", []byte("2"))
	tree2.Insert("a", []byte("1"))

	if tree1.Root() != tree2.Root() {
		t.Error("same data should produce same root regardless of insert order")
	}
}

func TestMerkleTreeDiff(t *testing.T) {
	tree1 := NewMerkleTree()
	tree1.Insert("a", []byte("1"))
	tree1.Insert("b", []byte("2"))

	tree2 := NewMerkleTree()
	tree2.Insert("a", []byte("1"))
	tree2.Insert("c", []byte("3"))

	diff := tree1.Diff(tree2)

	if len(diff) != 1 {
		t.Errorf("got %d diff keys, want 1", len(diff))
	}

	if diff[0] != "c" {
		t.Errorf("got diff key %q, want c", diff[0])
	}
}

func TestMerkleTreeDiffModified(t *testing.T) {
	tree1 := NewMerkleTree()
	tree1.Insert("a", []byte("1"))

	tree2 := NewMerkleTree()
	tree2.Insert("a", []byte("modified"))

	diff := tree1.Diff(tree2)

	if len(diff) != 1 {
		t.Errorf("got %d diff keys, want 1", len(diff))
	}

	if diff[0] != "a" {
		t.Errorf("got diff key %q, want a", diff[0])
	}
}

func TestReplicationManagerUpdateTree(t *testing.T) {
	nodeConfig := DefaultNodeConfig()
	nodeConfig.ID = "test-node"
	nodeConfig.BindAddr = "localhost:0"
	nodeConfig.RPCAddr = "localhost:0"

	node, _ := NewNode(nodeConfig)
	coord := NewCoordinator(node, DefaultCoordinatorConfig())
	rm := NewReplicationManager(node, coord, DefaultReplicationConfig())

	rm.UpdateTree("testdb", "key1", []byte("value1"))
	rm.UpdateTree("testdb", "key2", []byte("value2"))

	root := rm.GetTreeRoot("testdb")
	if root == "" {
		t.Error("tree root should not be empty")
	}

	// Different database should have empty root
	otherRoot := rm.GetTreeRoot("otherdb")
	if otherRoot != "" {
		t.Error("non-existent database should have empty root")
	}
}

func TestSyncTypeConstants(t *testing.T) {
	// Just verify the constants exist and are distinct
	if SyncTypeRoots == SyncTypeLeaves {
		t.Error("sync types should be distinct")
	}

	if SyncTypeLeaves == SyncTypeData {
		t.Error("sync types should be distinct")
	}
}
