package cluster

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestRaftStateAddNode(t *testing.T) {
	state := NewRaftState()

	payload, _ := json.Marshal(addNodePayload{
		ID:      "node1",
		Addr:    "localhost:8086",
		RPCAddr: "localhost:8087",
		Type:    NodeTypeData,
	})

	cmd := &RaftCommand{
		Type:    CmdAddNode,
		Payload: payload,
	}

	if err := state.Apply(cmd); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	node, ok := state.GetNode("node1")
	if !ok {
		t.Fatal("node not found")
	}

	if node.Addr != "localhost:8086" {
		t.Errorf("expected address localhost:8086, got %s", node.Addr)
	}
}

func TestRaftStateRemoveNode(t *testing.T) {
	state := NewRaftState()

	// Add node
	addPayload, _ := json.Marshal(addNodePayload{
		ID:   "node1",
		Addr: "localhost:8086",
	})
	state.Apply(&RaftCommand{Type: CmdAddNode, Payload: addPayload})

	// Remove node
	removePayload, _ := json.Marshal(removeNodePayload{ID: "node1"})
	if err := state.Apply(&RaftCommand{Type: CmdRemoveNode, Payload: removePayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	_, ok := state.GetNode("node1")
	if ok {
		t.Error("node should be removed")
	}
}

func TestRaftStateCreateDatabase(t *testing.T) {
	state := NewRaftState()

	payload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	cmd := &RaftCommand{Type: CmdCreateDatabase, Payload: payload}

	if err := state.Apply(cmd); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	db, ok := state.GetDatabase("testdb")
	if !ok {
		t.Fatal("database not found")
	}

	if db.DefaultRP != "autogen" {
		t.Errorf("expected default RP 'autogen', got %s", db.DefaultRP)
	}

	if _, ok := db.RetentionPolicies["autogen"]; !ok {
		t.Error("autogen retention policy not created")
	}
}

func TestRaftStateCreateDatabaseDuplicate(t *testing.T) {
	state := NewRaftState()

	payload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	cmd := &RaftCommand{Type: CmdCreateDatabase, Payload: payload}

	state.Apply(cmd)

	// Try to create again
	if err := state.Apply(cmd); err == nil {
		t.Error("expected error for duplicate database")
	}
}

func TestRaftStateDropDatabase(t *testing.T) {
	state := NewRaftState()

	// Create
	createPayload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	state.Apply(&RaftCommand{Type: CmdCreateDatabase, Payload: createPayload})

	// Drop
	dropPayload, _ := json.Marshal(dropDatabasePayload{Name: "testdb"})
	if err := state.Apply(&RaftCommand{Type: CmdDropDatabase, Payload: dropPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	_, ok := state.GetDatabase("testdb")
	if ok {
		t.Error("database should be dropped")
	}
}

func TestRaftStateRetentionPolicy(t *testing.T) {
	state := NewRaftState()

	// Create database
	dbPayload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	state.Apply(&RaftCommand{Type: CmdCreateDatabase, Payload: dbPayload})

	// Create RP
	rpPayload, _ := json.Marshal(createRPPayload{
		Database:          "testdb",
		Name:              "weekly",
		Duration:          7 * 24 * time.Hour,
		ShardDuration:     24 * time.Hour,
		ReplicationFactor: 2,
		Default:           true,
	})

	if err := state.Apply(&RaftCommand{Type: CmdCreateRetentionPolicy, Payload: rpPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	db, _ := state.GetDatabase("testdb")
	if db.DefaultRP != "weekly" {
		t.Errorf("expected default RP 'weekly', got %s", db.DefaultRP)
	}

	rp, ok := db.RetentionPolicies["weekly"]
	if !ok {
		t.Fatal("retention policy not found")
	}

	if rp.ReplicationFactor != 2 {
		t.Errorf("expected replication factor 2, got %d", rp.ReplicationFactor)
	}
}

func TestRaftStateAlterRetentionPolicy(t *testing.T) {
	state := NewRaftState()

	// Create database
	dbPayload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	state.Apply(&RaftCommand{Type: CmdCreateDatabase, Payload: dbPayload})

	// Alter autogen
	newDuration := 30 * 24 * time.Hour
	alterPayload, _ := json.Marshal(alterRPPayload{
		Database: "testdb",
		Name:     "autogen",
		Duration: &newDuration,
	})

	if err := state.Apply(&RaftCommand{Type: CmdAlterRetentionPolicy, Payload: alterPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	db, _ := state.GetDatabase("testdb")
	rp := db.RetentionPolicies["autogen"]

	if rp.Duration != 30*24*time.Hour {
		t.Errorf("expected duration 30d, got %v", rp.Duration)
	}
}

func TestRaftStateContinuousQuery(t *testing.T) {
	state := NewRaftState()

	// Create CQ
	cqPayload, _ := json.Marshal(createCQPayload{
		Name:     "cpu_hourly",
		Database: "testdb",
		Query:    "SELECT mean(value) INTO cpu_hourly FROM cpu GROUP BY time(1h)",
		Interval: time.Hour,
	})

	if err := state.Apply(&RaftCommand{Type: CmdCreateContinuousQuery, Payload: cqPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	if len(state.ContinuousQueries) != 1 {
		t.Errorf("expected 1 CQ, got %d", len(state.ContinuousQueries))
	}

	// Drop CQ
	dropPayload, _ := json.Marshal(dropCQPayload{
		Name:     "cpu_hourly",
		Database: "testdb",
	})

	if err := state.Apply(&RaftCommand{Type: CmdDropContinuousQuery, Payload: dropPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	if len(state.ContinuousQueries) != 0 {
		t.Errorf("expected 0 CQs, got %d", len(state.ContinuousQueries))
	}
}

func TestRaftStateShardAssignment(t *testing.T) {
	state := NewRaftState()

	// Assign shard
	assignPayload, _ := json.Marshal(assignShardPayload{
		ShardID: 1,
		NodeIDs: []string{"node1", "node2"},
	})

	if err := state.Apply(&RaftCommand{Type: CmdAssignShard, Payload: assignPayload}); err != nil {
		t.Fatalf("apply error: %v", err)
	}

	nodes := state.GetShardNodes(1)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	// Unassign
	unassignPayload, _ := json.Marshal(unassignShardPayload{ShardID: 1})
	state.Apply(&RaftCommand{Type: CmdUnassignShard, Payload: unassignPayload})

	nodes = state.GetShardNodes(1)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestRaftStateSnapshotRestore(t *testing.T) {
	state := NewRaftState()

	// Add some data
	dbPayload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	state.Apply(&RaftCommand{Type: CmdCreateDatabase, Payload: dbPayload})

	nodePayload, _ := json.Marshal(addNodePayload{
		ID:   "node1",
		Addr: "localhost:8086",
	})
	state.Apply(&RaftCommand{Type: CmdAddNode, Payload: nodePayload})

	// Snapshot
	data, err := state.Snapshot()
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}

	// Restore to new state
	state2 := NewRaftState()
	if err := state2.Restore(data); err != nil {
		t.Fatalf("restore error: %v", err)
	}

	// Verify
	if _, ok := state2.GetDatabase("testdb"); !ok {
		t.Error("database not restored")
	}
	if _, ok := state2.GetNode("node1"); !ok {
		t.Error("node not restored")
	}
}

func TestSimpleRaftConsensus(t *testing.T) {
	state := NewRaftState()
	consensus := NewSimpleRaftConsensus(state)

	// Not leader initially
	if consensus.IsLeader() {
		t.Error("should not be leader initially")
	}

	// Submit should fail when not leader
	payload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	cmd := &RaftCommand{Type: CmdCreateDatabase, Payload: payload}

	if err := consensus.Submit(cmd); err == nil {
		t.Error("expected error when not leader")
	}

	// Become leader
	consensus.BecomeLeader("node1")

	if !consensus.IsLeader() {
		t.Error("should be leader")
	}
	if consensus.LeaderID() != "node1" {
		t.Errorf("expected leader 'node1', got %s", consensus.LeaderID())
	}

	// Submit should succeed
	if err := consensus.Submit(cmd); err != nil {
		t.Fatalf("submit error: %v", err)
	}

	// Verify state was updated
	if _, ok := state.GetDatabase("testdb"); !ok {
		t.Error("database not created")
	}
}

func TestRaftConsensusSnapshot(t *testing.T) {
	state := NewRaftState()
	consensus := NewSimpleRaftConsensus(state)
	consensus.BecomeLeader("node1")

	// Add data
	payload, _ := json.Marshal(createDatabasePayload{Name: "testdb"})
	consensus.Submit(&RaftCommand{Type: CmdCreateDatabase, Payload: payload})

	// Snapshot
	var buf bytes.Buffer
	if err := consensus.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot error: %v", err)
	}

	// Restore
	state2 := NewRaftState()
	consensus2 := NewSimpleRaftConsensus(state2)

	if err := consensus2.Restore(&buf); err != nil {
		t.Fatalf("restore error: %v", err)
	}

	if _, ok := consensus2.State().GetDatabase("testdb"); !ok {
		t.Error("database not restored")
	}
}

func TestRaftConsensusLeaderChange(t *testing.T) {
	state := NewRaftState()
	consensus := NewSimpleRaftConsensus(state)

	var leaderChanges []bool
	consensus.OnLeaderChange(func(isLeader bool) {
		leaderChanges = append(leaderChanges, isLeader)
	})

	consensus.BecomeLeader("node1")
	consensus.SetLeader("node2")

	if len(leaderChanges) != 2 {
		t.Errorf("expected 2 leader changes, got %d", len(leaderChanges))
	}
	if leaderChanges[0] != true {
		t.Error("first change should be true (became leader)")
	}
	if leaderChanges[1] != false {
		t.Error("second change should be false (lost leadership)")
	}
}
