package cluster

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// RaftState represents the replicated cluster state
type RaftState struct {
	mu sync.RWMutex

	// Cluster membership
	Nodes map[string]*NodeInfo `json:"nodes"`

	// Databases and their retention policies
	Databases map[string]*DatabaseMeta `json:"databases"`

	// Continuous queries
	ContinuousQueries map[string]*CQMeta `json:"continuous_queries"`

	// Shard assignments: shardID -> list of node IDs
	ShardAssignments map[uint64][]string `json:"shard_assignments"`

	// Current term
	Term uint64 `json:"term"`

	// Last applied index
	LastIndex uint64 `json:"last_index"`
}

// RaftNodeInfo holds node metadata for Raft state (uses existing NodeInfo)
// Note: We reference the existing NodeInfo from node.go

// DatabaseMeta holds database metadata
type DatabaseMeta struct {
	Name              string                     `json:"name"`
	RetentionPolicies map[string]*RPMeta         `json:"retention_policies"`
	DefaultRP         string                     `json:"default_rp"`
	CreatedAt         time.Time                  `json:"created_at"`
}

// RPMeta holds retention policy metadata
type RPMeta struct {
	Name              string        `json:"name"`
	Duration          time.Duration `json:"duration"`
	ShardDuration     time.Duration `json:"shard_duration"`
	ReplicationFactor int           `json:"replication_factor"`
	Default           bool          `json:"default"`
}

// CQMeta holds continuous query metadata
type CQMeta struct {
	Name        string        `json:"name"`
	Database    string        `json:"database"`
	Query       string        `json:"query"`
	Interval    time.Duration `json:"interval"`
	Enabled     bool          `json:"enabled"`
}

// RaftCommand represents a command to be applied to the state machine
type RaftCommand struct {
	Type    RaftCommandType `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RaftCommandType identifies the command type
type RaftCommandType int

const (
	CmdAddNode RaftCommandType = iota
	CmdRemoveNode
	CmdCreateDatabase
	CmdDropDatabase
	CmdCreateRetentionPolicy
	CmdAlterRetentionPolicy
	CmdDropRetentionPolicy
	CmdCreateContinuousQuery
	CmdDropContinuousQuery
	CmdAssignShard
	CmdUnassignShard
)

// NewRaftState creates a new Raft state
func NewRaftState() *RaftState {
	return &RaftState{
		Nodes:             make(map[string]*NodeInfo),
		Databases:         make(map[string]*DatabaseMeta),
		ContinuousQueries: make(map[string]*CQMeta),
		ShardAssignments:  make(map[uint64][]string),
	}
}

// Apply applies a command to the state machine
func (s *RaftState) Apply(cmd *RaftCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.LastIndex++

	switch cmd.Type {
	case CmdAddNode:
		return s.applyAddNode(cmd.Payload)
	case CmdRemoveNode:
		return s.applyRemoveNode(cmd.Payload)
	case CmdCreateDatabase:
		return s.applyCreateDatabase(cmd.Payload)
	case CmdDropDatabase:
		return s.applyDropDatabase(cmd.Payload)
	case CmdCreateRetentionPolicy:
		return s.applyCreateRP(cmd.Payload)
	case CmdAlterRetentionPolicy:
		return s.applyAlterRP(cmd.Payload)
	case CmdDropRetentionPolicy:
		return s.applyDropRP(cmd.Payload)
	case CmdCreateContinuousQuery:
		return s.applyCreateCQ(cmd.Payload)
	case CmdDropContinuousQuery:
		return s.applyDropCQ(cmd.Payload)
	case CmdAssignShard:
		return s.applyAssignShard(cmd.Payload)
	case CmdUnassignShard:
		return s.applyUnassignShard(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// Snapshot returns a snapshot of the state
func (s *RaftState) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s)
}

// Restore restores state from a snapshot
func (s *RaftState) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var restored RaftState
	if err := json.Unmarshal(data, &restored); err != nil {
		return err
	}

	s.Nodes = restored.Nodes
	s.Databases = restored.Databases
	s.ContinuousQueries = restored.ContinuousQueries
	s.ShardAssignments = restored.ShardAssignments
	s.Term = restored.Term
	s.LastIndex = restored.LastIndex

	return nil
}

// Command implementations

type addNodePayload struct {
	ID      string   `json:"id"`
	Addr    string   `json:"addr"`
	RPCAddr string   `json:"rpc_addr"`
	Type    NodeType `json:"type"`
}

func (s *RaftState) applyAddNode(payload json.RawMessage) error {
	var p addNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	s.Nodes[p.ID] = &NodeInfo{
		ID:       p.ID,
		Addr:     p.Addr,
		RPCAddr:  p.RPCAddr,
		Type:     p.Type,
		State:    NodeStateReady,
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
	}
	return nil
}

type removeNodePayload struct {
	ID string `json:"id"`
}

func (s *RaftState) applyRemoveNode(payload json.RawMessage) error {
	var p removeNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	delete(s.Nodes, p.ID)
	return nil
}

type createDatabasePayload struct {
	Name string `json:"name"`
}

func (s *RaftState) applyCreateDatabase(payload json.RawMessage) error {
	var p createDatabasePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	if _, exists := s.Databases[p.Name]; exists {
		return fmt.Errorf("database %q already exists", p.Name)
	}

	s.Databases[p.Name] = &DatabaseMeta{
		Name:              p.Name,
		RetentionPolicies: make(map[string]*RPMeta),
		DefaultRP:         "autogen",
		CreatedAt:         time.Now(),
	}

	// Create default retention policy
	s.Databases[p.Name].RetentionPolicies["autogen"] = &RPMeta{
		Name:              "autogen",
		Duration:          0, // infinite
		ShardDuration:     7 * 24 * time.Hour,
		ReplicationFactor: 1,
		Default:           true,
	}

	return nil
}

type dropDatabasePayload struct {
	Name string `json:"name"`
}

func (s *RaftState) applyDropDatabase(payload json.RawMessage) error {
	var p dropDatabasePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	delete(s.Databases, p.Name)
	return nil
}

type createRPPayload struct {
	Database          string        `json:"database"`
	Name              string        `json:"name"`
	Duration          time.Duration `json:"duration"`
	ShardDuration     time.Duration `json:"shard_duration"`
	ReplicationFactor int           `json:"replication_factor"`
	Default           bool          `json:"default"`
}

func (s *RaftState) applyCreateRP(payload json.RawMessage) error {
	var p createRPPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	db, exists := s.Databases[p.Database]
	if !exists {
		return fmt.Errorf("database %q not found", p.Database)
	}

	if _, exists := db.RetentionPolicies[p.Name]; exists {
		return fmt.Errorf("retention policy %q already exists", p.Name)
	}

	db.RetentionPolicies[p.Name] = &RPMeta{
		Name:              p.Name,
		Duration:          p.Duration,
		ShardDuration:     p.ShardDuration,
		ReplicationFactor: p.ReplicationFactor,
		Default:           p.Default,
	}

	if p.Default {
		db.DefaultRP = p.Name
		// Unset default on others
		for name, rp := range db.RetentionPolicies {
			if name != p.Name {
				rp.Default = false
			}
		}
	}

	return nil
}

type alterRPPayload struct {
	Database          string         `json:"database"`
	Name              string         `json:"name"`
	Duration          *time.Duration `json:"duration,omitempty"`
	ShardDuration     *time.Duration `json:"shard_duration,omitempty"`
	ReplicationFactor *int           `json:"replication_factor,omitempty"`
	Default           *bool          `json:"default,omitempty"`
}

func (s *RaftState) applyAlterRP(payload json.RawMessage) error {
	var p alterRPPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	db, exists := s.Databases[p.Database]
	if !exists {
		return fmt.Errorf("database %q not found", p.Database)
	}

	rp, exists := db.RetentionPolicies[p.Name]
	if !exists {
		return fmt.Errorf("retention policy %q not found", p.Name)
	}

	if p.Duration != nil {
		rp.Duration = *p.Duration
	}
	if p.ShardDuration != nil {
		rp.ShardDuration = *p.ShardDuration
	}
	if p.ReplicationFactor != nil {
		rp.ReplicationFactor = *p.ReplicationFactor
	}
	if p.Default != nil && *p.Default {
		db.DefaultRP = p.Name
		rp.Default = true
		for name, other := range db.RetentionPolicies {
			if name != p.Name {
				other.Default = false
			}
		}
	}

	return nil
}

type dropRPPayload struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

func (s *RaftState) applyDropRP(payload json.RawMessage) error {
	var p dropRPPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	db, exists := s.Databases[p.Database]
	if !exists {
		return fmt.Errorf("database %q not found", p.Database)
	}

	delete(db.RetentionPolicies, p.Name)

	if db.DefaultRP == p.Name {
		db.DefaultRP = ""
	}

	return nil
}

type createCQPayload struct {
	Name     string        `json:"name"`
	Database string        `json:"database"`
	Query    string        `json:"query"`
	Interval time.Duration `json:"interval"`
}

func (s *RaftState) applyCreateCQ(payload json.RawMessage) error {
	var p createCQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	key := p.Database + "." + p.Name
	s.ContinuousQueries[key] = &CQMeta{
		Name:     p.Name,
		Database: p.Database,
		Query:    p.Query,
		Interval: p.Interval,
		Enabled:  true,
	}

	return nil
}

type dropCQPayload struct {
	Name     string `json:"name"`
	Database string `json:"database"`
}

func (s *RaftState) applyDropCQ(payload json.RawMessage) error {
	var p dropCQPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	key := p.Database + "." + p.Name
	delete(s.ContinuousQueries, key)
	return nil
}

type assignShardPayload struct {
	ShardID uint64   `json:"shard_id"`
	NodeIDs []string `json:"node_ids"`
}

func (s *RaftState) applyAssignShard(payload json.RawMessage) error {
	var p assignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	s.ShardAssignments[p.ShardID] = p.NodeIDs
	return nil
}

type unassignShardPayload struct {
	ShardID uint64 `json:"shard_id"`
}

func (s *RaftState) applyUnassignShard(payload json.RawMessage) error {
	var p unassignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	delete(s.ShardAssignments, p.ShardID)
	return nil
}

// Read operations (don't need consensus)

// GetNode returns node info
func (s *RaftState) GetNode(id string) (*NodeInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.Nodes[id]
	return n, ok
}

// GetNodes returns all nodes
func (s *RaftState) GetNodes() []*NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetDatabase returns database metadata
func (s *RaftState) GetDatabase(name string) (*DatabaseMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.Databases[name]
	return db, ok
}

// GetDatabases returns all databases
func (s *RaftState) GetDatabases() []*DatabaseMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dbs := make([]*DatabaseMeta, 0, len(s.Databases))
	for _, db := range s.Databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// GetShardNodes returns the nodes assigned to a shard
func (s *RaftState) GetShardNodes(shardID uint64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ShardAssignments[shardID]
}

// ApplyAddNode applies an add node command (public wrapper)
func (s *RaftState) ApplyAddNode(node *NodeInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Nodes[node.ID] = node
	return nil
}

// ApplyRemoveNode applies a remove node command (public wrapper)
func (s *RaftState) ApplyRemoveNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Nodes, nodeID)
	return nil
}

// ApplyCreateDatabase applies a create database command (public wrapper)
func (s *RaftState) ApplyCreateDatabase(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.Databases[name]; exists {
		return fmt.Errorf("database %s already exists", name)
	}
	s.Databases[name] = &DatabaseMeta{
		Name:              name,
		RetentionPolicies: make(map[string]*RPMeta),
		CreatedAt:         time.Now(),
	}
	return nil
}

// ApplyDropDatabase applies a drop database command (public wrapper)
func (s *RaftState) ApplyDropDatabase(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Databases, name)
	return nil
}

// ApplyCreateRetentionPolicy applies a create RP command (public wrapper)
func (s *RaftState) ApplyCreateRetentionPolicy(database string, policy *RPMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, exists := s.Databases[database]
	if !exists {
		return fmt.Errorf("database %s not found", database)
	}
	db.RetentionPolicies[policy.Name] = policy
	if policy.Default {
		db.DefaultRP = policy.Name
	}
	return nil
}

// ApplyDropRetentionPolicy applies a drop RP command (public wrapper)
func (s *RaftState) ApplyDropRetentionPolicy(database, policyName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, exists := s.Databases[database]
	if !exists {
		return fmt.Errorf("database %s not found", database)
	}
	delete(db.RetentionPolicies, policyName)
	return nil
}

// ApplyAssignShard applies a shard assignment command (public wrapper)
func (s *RaftState) ApplyAssignShard(shardID uint64, nodeIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ShardAssignments[shardID] = nodeIDs
	return nil
}

// SimpleRaftConsensus provides a simple consensus interface for testing/standalone
type SimpleRaftConsensus struct {
	mu sync.RWMutex

	state    *RaftState
	isLeader bool
	leaderID string

	// Log for commands
	log []RaftCommand

	// Callbacks
	onLeaderChange func(isLeader bool)
}

// NewSimpleRaftConsensus creates a new simple Raft consensus instance
func NewSimpleRaftConsensus(state *RaftState) *SimpleRaftConsensus {
	return &SimpleRaftConsensus{
		state: state,
		log:   make([]RaftCommand, 0),
	}
}

// IsLeader returns whether this node is the leader
func (r *SimpleRaftConsensus) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeader
}

// LeaderID returns the current leader's ID
func (r *SimpleRaftConsensus) LeaderID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// Submit submits a command for consensus
func (r *SimpleRaftConsensus) Submit(cmd *RaftCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isLeader {
		return fmt.Errorf("not leader, leader is %s", r.leaderID)
	}

	if err := r.state.Apply(cmd); err != nil {
		return err
	}

	r.log = append(r.log, *cmd)
	return nil
}

// State returns the replicated state
func (r *SimpleRaftConsensus) State() *RaftState {
	return r.state
}

// BecomeLeader makes this node the leader
func (r *SimpleRaftConsensus) BecomeLeader(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.isLeader = true
	r.leaderID = nodeID

	if r.onLeaderChange != nil {
		r.onLeaderChange(true)
	}
}

// SetLeader sets the leader
func (r *SimpleRaftConsensus) SetLeader(leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.isLeader = false
	r.leaderID = leaderID

	if r.onLeaderChange != nil {
		r.onLeaderChange(false)
	}
}

// OnLeaderChange sets the leader change callback
func (r *SimpleRaftConsensus) OnLeaderChange(fn func(isLeader bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onLeaderChange = fn
}

// Snapshot writes state snapshot to writer
func (r *SimpleRaftConsensus) Snapshot(w interface{ Write([]byte) (int, error) }) error {
	data, err := r.state.Snapshot()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// Restore restores state from reader
func (r *SimpleRaftConsensus) Restore(rd interface{ Read([]byte) (int, error) }) error {
	data := make([]byte, 1024*1024) // 1MB buffer
	n, err := rd.Read(data)
	if err != nil && n == 0 {
		return err
	}
	return r.state.Restore(data[:n])
}
