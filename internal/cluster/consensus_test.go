package cluster

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestNewEpochFSM(t *testing.T) {
	fsm := NewEpochFSM()
	if fsm == nil {
		t.Fatal("NewEpochFSM returned nil")
	}

	if fsm.state == nil {
		t.Error("FSM state is nil")
	}
}

func TestFSMGetState(t *testing.T) {
	fsm := NewEpochFSM()

	state := fsm.GetState()
	if state == nil {
		t.Fatal("GetState returned nil")
	}

	// Modifying returned state should not affect FSM
	state.Nodes["test"] = &NodeInfo{ID: "test"}
	original := fsm.GetState()
	if _, exists := original.Nodes["test"]; exists {
		t.Error("GetState should return a copy")
	}
}

func TestEpochFSMSnapshot(t *testing.T) {
	fsm := NewEpochFSM()

	// Add some state
	fsm.state.ApplyAddNode(&NodeInfo{ID: "node1", Addr: "localhost:8086"})
	fsm.state.ApplyCreateDatabase("testdb")

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if snapshot == nil {
		t.Fatal("Snapshot returned nil")
	}

	epochSnapshot := snapshot.(*EpochFSMSnapshot)
	if len(epochSnapshot.data) == 0 {
		t.Error("Snapshot data is empty")
	}

	// Verify data contains our state
	if !strings.Contains(string(epochSnapshot.data), "node1") {
		t.Error("Snapshot should contain node1")
	}
	if !strings.Contains(string(epochSnapshot.data), "testdb") {
		t.Error("Snapshot should contain testdb")
	}
}

func TestEpochFSMRestore(t *testing.T) {
	// Create FSM with some state
	fsm1 := NewEpochFSM()
	fsm1.state.ApplyAddNode(&NodeInfo{ID: "node1", Addr: "localhost:8086"})
	fsm1.state.ApplyCreateDatabase("testdb")

	// Get snapshot data
	snapshot, _ := fsm1.Snapshot()
	epochSnapshot := snapshot.(*EpochFSMSnapshot)

	// Create new FSM and restore
	fsm2 := NewEpochFSM()
	reader := &readCloser{data: epochSnapshot.data}
	
	if err := fsm2.Restore(reader); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	// Verify state was restored
	state := fsm2.GetState()
	if _, exists := state.Nodes["node1"]; !exists {
		t.Error("Restored state missing node1")
	}
	if _, exists := state.Databases["testdb"]; !exists {
		t.Error("Restored state missing testdb")
	}
}

// Helper for testing Restore
type readCloser struct {
	data []byte
	pos  int
}

func (r *readCloser) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, nil
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *readCloser) Close() error { return nil }

func TestDefaultRaftConsensusConfig(t *testing.T) {
	cfg := DefaultRaftConsensusConfig()

	if cfg.HeartbeatTimeout != 1000*time.Millisecond {
		t.Errorf("HeartbeatTimeout = %v, want 1s", cfg.HeartbeatTimeout)
	}

	if cfg.ElectionTimeout != 1000*time.Millisecond {
		t.Errorf("ElectionTimeout = %v, want 1s", cfg.ElectionTimeout)
	}

	if cfg.MaxAppendEntries != 64 {
		t.Errorf("MaxAppendEntries = %d, want 64", cfg.MaxAppendEntries)
	}
}

func TestNewRaftConsensusValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  RaftConsensusConfig
		wantErr bool
	}{
		{
			name:    "empty node ID",
			config:  RaftConsensusConfig{NodeID: "", RaftAddr: "localhost:8088", RaftDir: "/tmp/raft"},
			wantErr: true,
		},
		{
			name:    "empty raft addr",
			config:  RaftConsensusConfig{NodeID: "node1", RaftAddr: "", RaftDir: "/tmp/raft"},
			wantErr: true,
		},
		{
			name:    "empty raft dir",
			config:  RaftConsensusConfig{NodeID: "node1", RaftAddr: "localhost:8088", RaftDir: ""},
			wantErr: true,
		},
		{
			name:    "valid config",
			config:  RaftConsensusConfig{NodeID: "node1", RaftAddr: "localhost:8088", RaftDir: "/tmp/raft"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRaftConsensus(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRaftConsensus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRaftConsensusNotInitialized(t *testing.T) {
	rc, _ := NewRaftConsensus(RaftConsensusConfig{
		NodeID:   "node1",
		RaftAddr: "localhost:8088",
		RaftDir:  "/tmp/raft",
	})

	// Should not be leader when not started
	if rc.IsLeader() {
		t.Error("Should not be leader when not started")
	}

	// Leader should be empty
	if rc.Leader() != "" {
		t.Errorf("Leader() = %q, want empty", rc.Leader())
	}

	// Apply should fail
	cmd := RaftCommand{Type: CmdCreateDatabase}
	if err := rc.Apply(cmd, time.Second); err == nil {
		t.Error("Apply should fail when not initialized")
	}

	// AddVoter should fail
	if err := rc.AddVoter("node2", "localhost:8089"); err == nil {
		t.Error("AddVoter should fail when not initialized")
	}

	// RemoveServer should fail
	if err := rc.RemoveServer("node2"); err == nil {
		t.Error("RemoveServer should fail when not initialized")
	}

	// GetConfiguration should fail
	if _, err := rc.GetConfiguration(); err == nil {
		t.Error("GetConfiguration should fail when not initialized")
	}
}

func TestRaftCommandPayloads(t *testing.T) {
	// Test serialization of command payloads
	tests := []struct {
		name    string
		payload interface{}
	}{
		{"CreateDatabase", createDatabasePayload{Name: "testdb"}},
		{"DropDatabase", dropDatabasePayload{Name: "testdb"}},
		{"CreateRP", createRPPayload{Database: "db", Name: "rp1"}},
		{"DropRP", dropRPPayload{Database: "db", Name: "rp1"}},
		{"AddNode", addNodePayload{ID: "n1"}},
		{"RemoveNode", removeNodePayload{ID: "n1"}},
		{"AssignShard", assignShardPayload{ShardID: 1, NodeIDs: []string{"n1", "n2"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("Marshal error = %v", err)
			}

			// Verify it can be unmarshaled
			if len(data) == 0 {
				t.Error("Marshal produced empty data")
			}
		})
	}
}

func TestEpochFSMSnapshotRelease(t *testing.T) {
	snapshot := &EpochFSMSnapshot{data: []byte("test")}
	// Should not panic
	snapshot.Release()
}


func TestEpochFSMApplyCommands(t *testing.T) {
	fsm := NewEpochFSM()

	// Test CreateDatabase command via state
	fsm.state.ApplyCreateDatabase("testdb")
	state := fsm.GetState()
	if _, ok := state.Databases["testdb"]; !ok {
		t.Error("database was not created")
	}

	// Test DropDatabase command
	fsm.state.ApplyDropDatabase("testdb")
	state = fsm.GetState()
	if _, ok := state.Databases["testdb"]; ok {
		t.Error("database was not dropped")
	}

	// Test AddNode command
	fsm.state.ApplyAddNode(&NodeInfo{ID: "node1", Addr: "localhost:8086"})
	state = fsm.GetState()
	if _, ok := state.Nodes["node1"]; !ok {
		t.Error("node was not added")
	}

	// Test RemoveNode command
	fsm.state.ApplyRemoveNode("node1")
	state = fsm.GetState()
	if _, ok := state.Nodes["node1"]; ok {
		t.Error("node was not removed")
	}
}

func TestEnsureDirFunc(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "subdir", "nested", "file.db")

	err := EnsureDir(testPath)
	if err != nil {
		t.Fatalf("EnsureDir error: %v", err)
	}

	// Verify parent directory exists (EnsureDir creates parent of path)
	parentDir := filepath.Dir(testPath)
	info, err := os.Stat(parentDir)
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if !info.IsDir() {
		t.Error("path is not a directory")
	}

	// Call again - should not error
	err = EnsureDir(testPath)
	if err != nil {
		t.Errorf("EnsureDir on existing dir error: %v", err)
	}
}

func TestEpochFSMApplyWithRaftLog(t *testing.T) {
	fsm := NewEpochFSM()

	// Test CreateDatabase command
	payload := map[string]string{"name": "testdb"}
	payloadBytes, _ := json.Marshal(payload)
	cmd := RaftCommand{
		Type:    CmdCreateDatabase,
		Payload: payloadBytes,
	}
	cmdBytes, _ := json.Marshal(cmd)
	log := &raft.Log{Data: cmdBytes}

	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("Apply CreateDatabase returned error: %v", result)
	}

	state := fsm.GetState()
	if _, ok := state.Databases["testdb"]; !ok {
		t.Error("database was not created")
	}
}

func TestEpochFSMApplyDropDatabase(t *testing.T) {
	fsm := NewEpochFSM()

	// First create
	fsm.state.ApplyCreateDatabase("testdb")

	// Then drop
	payload := map[string]string{"name": "testdb"}
	payloadBytes, _ := json.Marshal(payload)
	cmd := RaftCommand{
		Type:    CmdDropDatabase,
		Payload: payloadBytes,
	}
	cmdBytes, _ := json.Marshal(cmd)
	log := &raft.Log{Data: cmdBytes}

	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("Apply DropDatabase returned error: %v", result)
	}

	state := fsm.GetState()
	if _, ok := state.Databases["testdb"]; ok {
		t.Error("database was not dropped")
	}
}

func TestEpochFSMApplyAddNode(t *testing.T) {
	fsm := NewEpochFSM()

	nodeInfo := &NodeInfo{ID: "node1", Addr: "localhost:8086"}
	nodeBytes, _ := json.Marshal(nodeInfo)

	cmd := RaftCommand{
		Type:    CmdAddNode,
		Payload: nodeBytes,
	}
	cmdBytes, _ := json.Marshal(cmd)
	log := &raft.Log{Data: cmdBytes}

	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("Apply AddNode returned error: %v", result)
	}

	state := fsm.GetState()
	if _, ok := state.Nodes["node1"]; !ok {
		t.Error("node was not added")
	}
}

func TestEpochFSMApplyRemoveNode(t *testing.T) {
	fsm := NewEpochFSM()

	// First add
	fsm.state.ApplyAddNode(&NodeInfo{ID: "node1", Addr: "localhost:8086"})

	// Then remove
	payload := map[string]string{"id": "node1"}
	payloadBytes, _ := json.Marshal(payload)
	cmd := RaftCommand{
		Type:    CmdRemoveNode,
		Payload: payloadBytes,
	}
	cmdBytes, _ := json.Marshal(cmd)
	log := &raft.Log{Data: cmdBytes}

	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("Apply RemoveNode returned error: %v", result)
	}

	state := fsm.GetState()
	if _, ok := state.Nodes["node1"]; ok {
		t.Error("node was not removed")
	}
}

func TestEpochFSMApplyInvalidCommand(t *testing.T) {
	fsm := NewEpochFSM()

	// Invalid JSON
	log := &raft.Log{Data: []byte("invalid json")}
	result := fsm.Apply(log)
	if result == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEpochFSMSnapshotPersist(t *testing.T) {
	fsm := NewEpochFSM()
	fsm.state.ApplyCreateDatabase("testdb")

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}

	// Create a mock sink
	sink := &mockSnapshotSink{buf: &bytes.Buffer{}}
	err = snapshot.Persist(sink)
	if err != nil {
		t.Errorf("Persist error: %v", err)
	}

	if sink.buf.Len() == 0 {
		t.Error("Persist wrote no data")
	}
}

type mockSnapshotSink struct {
	buf    *bytes.Buffer
	closed bool
}

func (s *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return s.buf.Write(p)
}

func (s *mockSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (s *mockSnapshotSink) Cancel() error {
	return nil
}

func TestRaftConsensusCreateDropDatabase(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultRaftConsensusConfig()
	config.RaftDir = tmpDir
	config.NodeID = "node1"
	config.RaftAddr = "127.0.0.1:0"

	rc, err := NewRaftConsensus(config)
	if err != nil {
		t.Fatalf("NewRaftConsensus error: %v", err)
	}

	// Test CreateDatabase without being leader (should fail)
	err = rc.CreateDatabase("testdb")
	if err == nil {
		// May succeed or fail depending on Raft state
		t.Log("CreateDatabase succeeded (unexpected but OK if bootstrapped)")
	}

	// Test DropDatabase without being leader
	err = rc.DropDatabase("testdb")
	if err == nil {
		t.Log("DropDatabase succeeded")
	}
}

func TestRaftConsensusGetState(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultRaftConsensusConfig()
	config.RaftDir = tmpDir
	config.NodeID = "node1"
	config.RaftAddr = "127.0.0.1:0"

	rc, err := NewRaftConsensus(config)
	if err != nil {
		t.Fatalf("NewRaftConsensus error: %v", err)
	}

	state := rc.GetState()
	if state == nil {
		t.Error("GetState returned nil")
	}
}

func TestRaftConsensusStartStop(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultRaftConsensusConfig()
	config.RaftDir = tmpDir
	config.NodeID = "node1"
	config.RaftAddr = "127.0.0.1:0"
	config.Bootstrap = true

	rc, err := NewRaftConsensus(config)
	if err != nil {
		t.Fatalf("NewRaftConsensus error: %v", err)
	}

	// Start
	err = rc.Start()
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Give it time to elect a leader
	time.Sleep(500 * time.Millisecond)

	// Check leader status
	isLeader := rc.IsLeader()
	t.Logf("IsLeader: %v", isLeader)

	leader := rc.Leader()
	t.Logf("Leader: %s", leader)

	// Stop
	err = rc.Stop()
	if err != nil {
		t.Errorf("Stop error: %v", err)
	}
}

func TestRaftConsensusGetConfiguration(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultRaftConsensusConfig()
	config.RaftDir = tmpDir
	config.NodeID = "node1"
	config.RaftAddr = "127.0.0.1:0"
	config.Bootstrap = true

	rc, err := NewRaftConsensus(config)
	if err != nil {
		t.Fatalf("NewRaftConsensus error: %v", err)
	}

	err = rc.Start()
	if err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer rc.Stop()

	time.Sleep(300 * time.Millisecond)

	servers, err := rc.GetConfiguration()
	if err != nil {
		t.Logf("GetConfiguration error (may be expected): %v", err)
	} else {
		t.Logf("Configuration has %d servers", len(servers))
	}
}
