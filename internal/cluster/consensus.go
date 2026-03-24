package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// RaftConsensus wraps hashicorp/raft for cluster consensus
type RaftConsensus struct {
	mu sync.RWMutex

	// Raft instance
	raft *raft.Raft

	// Local node info
	nodeID   string
	raftAddr string
	raftDir  string

	// State machine
	fsm *EpochFSM

	// Transport
	transport *raft.NetworkTransport

	// Configuration
	config RaftConsensusConfig
}

// RaftConsensusConfig holds Raft configuration
type RaftConsensusConfig struct {
	NodeID           string
	RaftAddr         string
	RaftDir          string
	Bootstrap        bool
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int
	SnapshotInterval time.Duration
	SnapshotThreshold uint64
}

// DefaultRaftConsensusConfig returns sensible defaults
func DefaultRaftConsensusConfig() RaftConsensusConfig {
	return RaftConsensusConfig{
		HeartbeatTimeout:  1000 * time.Millisecond,
		ElectionTimeout:   1000 * time.Millisecond,
		CommitTimeout:     50 * time.Millisecond,
		MaxAppendEntries:  64,
		SnapshotInterval:  120 * time.Second,
		SnapshotThreshold: 8192,
	}
}

// EpochFSM implements raft.FSM for Epoch cluster state
type EpochFSM struct {
	mu sync.RWMutex

	// Cluster state
	state *RaftState

	// Callbacks for state changes
	onDatabaseCreated func(name string)
	onDatabaseDropped func(name string)
	onPolicyChanged   func(database, policy string)
}

// NewEpochFSM creates a new FSM
func NewEpochFSM() *EpochFSM {
	return &EpochFSM{
		state: NewRaftState(),
	}
}

// Apply applies a Raft log entry to the FSM
func (f *EpochFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd RaftCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	// Use the RaftState's Apply method which handles all command types
	return f.state.Apply(&cmd)
}

// Snapshot returns an FSMSnapshot for the current state
func (f *EpochFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy the state
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}

	return &EpochFSMSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *EpochFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	var state RaftState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.state = &state
	return nil
}

// GetState returns a copy of the current state
func (f *EpochFSM) GetState() *RaftState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return a copy
	data, _ := json.Marshal(f.state)
	var copy RaftState
	json.Unmarshal(data, &copy)
	return &copy
}

// EpochFSMSnapshot implements raft.FSMSnapshot
type EpochFSMSnapshot struct {
	data []byte
}

// Persist writes the snapshot to the sink
func (s *EpochFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is called when the snapshot is no longer needed
func (s *EpochFSMSnapshot) Release() {}

// NewRaftConsensus creates a new Raft consensus instance
func NewRaftConsensus(config RaftConsensusConfig) (*RaftConsensus, error) {
	// Validate config
	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if config.RaftAddr == "" {
		return nil, fmt.Errorf("raft address is required")
	}
	if config.RaftDir == "" {
		return nil, fmt.Errorf("raft directory is required")
	}

	// Create FSM
	fsm := NewEpochFSM()

	rc := &RaftConsensus{
		nodeID:   config.NodeID,
		raftAddr: config.RaftAddr,
		raftDir:  config.RaftDir,
		fsm:      fsm,
		config:   config,
	}

	return rc, nil
}

// Start initializes and starts the Raft instance
func (rc *RaftConsensus) Start() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Create raft config
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(rc.nodeID)
	raftConfig.HeartbeatTimeout = rc.config.HeartbeatTimeout
	raftConfig.ElectionTimeout = rc.config.ElectionTimeout
	raftConfig.CommitTimeout = rc.config.CommitTimeout
	raftConfig.MaxAppendEntries = rc.config.MaxAppendEntries
	raftConfig.SnapshotInterval = rc.config.SnapshotInterval
	raftConfig.SnapshotThreshold = rc.config.SnapshotThreshold

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", rc.raftAddr)
	if err != nil {
		return fmt.Errorf("resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(rc.raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("create transport: %w", err)
	}
	rc.transport = transport

	// Create log store and stable store (using file-based stores)
	logStore, err := raft.NewFileSnapshotStore(rc.raftDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("create snapshot store: %w", err)
	}

	// For simplicity, use in-memory stores (in production, use bolt or similar)
	stableStore := raft.NewInmemStore()
	logStoreRaft := raft.NewInmemStore()

	// Create raft instance
	r, err := raft.NewRaft(raftConfig, rc.fsm, logStoreRaft, stableStore, logStore, transport)
	if err != nil {
		return fmt.Errorf("create raft: %w", err)
	}
	rc.raft = r

	// Bootstrap if needed
	if rc.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(rc.nodeID),
					Address: raft.ServerAddress(rc.raftAddr),
				},
			},
		}
		rc.raft.BootstrapCluster(configuration)
	}

	return nil
}

// Stop stops the Raft instance
func (rc *RaftConsensus) Stop() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.raft != nil {
		future := rc.raft.Shutdown()
		if err := future.Error(); err != nil {
			return err
		}
	}

	if rc.transport != nil {
		rc.transport.Close()
	}

	return nil
}

// IsLeader returns true if this node is the leader
func (rc *RaftConsensus) IsLeader() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return false
	}
	return rc.raft.State() == raft.Leader
}

// Leader returns the address of the current leader
func (rc *RaftConsensus) Leader() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return ""
	}
	addr, _ := rc.raft.LeaderWithID()
	return string(addr)
}

// Apply applies a command to the cluster
func (rc *RaftConsensus) Apply(cmd RaftCommand, timeout time.Duration) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	if rc.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rc.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return err
	}

	// Check if the FSM returned an error
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// AddVoter adds a voting member to the cluster
func (rc *RaftConsensus) AddVoter(nodeID, addr string) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := rc.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

// RemoveServer removes a member from the cluster
func (rc *RaftConsensus) RemoveServer(nodeID string) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := rc.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// GetState returns the current cluster state
func (rc *RaftConsensus) GetState() *RaftState {
	return rc.fsm.GetState()
}

// GetConfiguration returns the current Raft configuration
func (rc *RaftConsensus) GetConfiguration() ([]raft.Server, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.raft == nil {
		return nil, fmt.Errorf("raft not initialized")
	}

	future := rc.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Configuration().Servers, nil
}

// CreateDatabase creates a database via consensus
func (rc *RaftConsensus) CreateDatabase(name string) error {
	payload, _ := json.Marshal(createDatabasePayload{Name: name})
	cmd := RaftCommand{
		Type:    CmdCreateDatabase,
		Payload: payload,
	}
	return rc.Apply(cmd, 5*time.Second)
}

// DropDatabase drops a database via consensus
func (rc *RaftConsensus) DropDatabase(name string) error {
	payload, _ := json.Marshal(dropDatabasePayload{Name: name})
	cmd := RaftCommand{
		Type:    CmdDropDatabase,
		Payload: payload,
	}
	return rc.Apply(cmd, 5*time.Second)
}

// EnsureDir creates directory if it doesn't exist
func EnsureDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0755)
}
