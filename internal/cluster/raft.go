package cluster

import (
	"encoding/json"
	"fmt"
	"io"
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

// RaftConsensus provides the consensus interface
type RaftConsensus struct {
	mu sync.RWMutex

	state    *RaftState
	isLeader bool
	leaderID string

	// Log for commands (simplified - in production use hashicorp/raft)
	log []RaftCommand

	// Callbacks
	onLeaderChange func(isLeader bool)
}

// NewRaftConsensus creates a new Raft consensus instance
func NewRaftConsensus(state *RaftState) *RaftConsensus {
	return &RaftConsensus{
		state: state,
		log:   make([]RaftCommand, 0),
	}
}

// IsLeader returns whether this node is the leader
func (r *RaftConsensus) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeader
}

// LeaderID returns the current leader's ID
func (r *RaftConsensus) LeaderID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// Submit submits a command for consensus
// Returns error if not leader or consensus fails
func (r *RaftConsensus) Submit(cmd *RaftCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isLeader {
		return fmt.Errorf("not leader, leader is %s", r.leaderID)
	}

	// Apply locally
	if err := r.state.Apply(cmd); err != nil {
		return err
	}

	// Append to log (in production, this would replicate to followers)
	r.log = append(r.log, *cmd)

	return nil
}

// State returns the replicated state
func (r *RaftConsensus) State() *RaftState {
	return r.state
}

// BecomeLeader makes this node the leader (for testing/standalone)
func (r *RaftConsensus) BecomeLeader(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.isLeader = true
	r.leaderID = nodeID

	if r.onLeaderChange != nil {
		r.onLeaderChange(true)
	}
}

// SetLeader sets the leader (for followers)
func (r *RaftConsensus) SetLeader(leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.isLeader = false
	r.leaderID = leaderID

	if r.onLeaderChange != nil {
		r.onLeaderChange(false)
	}
}

// OnLeaderChange sets the leader change callback
func (r *RaftConsensus) OnLeaderChange(fn func(isLeader bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onLeaderChange = fn
}

// Snapshot creates a snapshot for persistence
func (r *RaftConsensus) Snapshot(w io.Writer) error {
	data, err := r.state.Snapshot()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// Restore restores from a snapshot
func (r *RaftConsensus) Restore(rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	return r.state.Restore(data)
}
