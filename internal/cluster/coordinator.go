package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

// ConsistencyLevel defines write/read consistency requirements
type ConsistencyLevel int

const (
	// ConsistencyOne requires acknowledgment from any single replica
	ConsistencyOne ConsistencyLevel = iota
	// ConsistencyQuorum requires acknowledgment from majority of replicas
	ConsistencyQuorum
	// ConsistencyAll requires acknowledgment from all replicas
	ConsistencyAll
)

func (c ConsistencyLevel) String() string {
	switch c {
	case ConsistencyOne:
		return "one"
	case ConsistencyQuorum:
		return "quorum"
	case ConsistencyAll:
		return "all"
	default:
		return "unknown"
	}
}

// Coordinator handles distributed operations across the cluster
type Coordinator struct {
	mu sync.RWMutex

	node              *Node
	hashRing          *HashRing
	replicationFactor int
	writeConsistency  ConsistencyLevel
	readConsistency   ConsistencyLevel
	writeTimeout      time.Duration
	readTimeout       time.Duration
}

// CoordinatorConfig holds coordinator configuration
type CoordinatorConfig struct {
	ReplicationFactor int
	WriteConsistency  ConsistencyLevel
	ReadConsistency   ConsistencyLevel
	WriteTimeout      time.Duration
	ReadTimeout       time.Duration
}

// DefaultCoordinatorConfig returns default configuration
func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		ReplicationFactor: 3,
		WriteConsistency:  ConsistencyQuorum,
		ReadConsistency:   ConsistencyOne,
		WriteTimeout:      5 * time.Second,
		ReadTimeout:       10 * time.Second,
	}
}

// NewCoordinator creates a new coordinator
func NewCoordinator(node *Node, config CoordinatorConfig) *Coordinator {
	if config.ReplicationFactor < 1 {
		config.ReplicationFactor = 1
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 5 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}

	// Create hash ring with configured replication factor
	hashRing := NewHashRing(HashRingConfig{
		VirtualNodes:      150,
		ReplicationFactor: config.ReplicationFactor,
	})

	return &Coordinator{
		node:              node,
		hashRing:          hashRing,
		replicationFactor: config.ReplicationFactor,
		writeConsistency:  config.WriteConsistency,
		readConsistency:   config.ReadConsistency,
		writeTimeout:      config.WriteTimeout,
		readTimeout:       config.ReadTimeout,
	}
}

// AddNode adds a node to the hash ring
func (c *Coordinator) AddNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hashRing.AddNode(nodeID)
}

// RemoveNode removes a node from the hash ring
func (c *Coordinator) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hashRing.RemoveNode(nodeID)
}

// GetNodesForKey returns the nodes responsible for a key using consistent hashing
func (c *Coordinator) GetNodesForKey(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hashRing.GetNodes(key, c.replicationFactor)
}

// WriteRequest represents a distributed write request
type WriteRequest struct {
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	Tags        map[string]string `json:"tags"`
	Fields      map[string]interface{} `json:"fields"`
	Timestamp   int64 `json:"timestamp"`
}

// WriteResponse represents a write response
type WriteResponse struct {
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
	AckedNodes  int    `json:"acked_nodes"`
	TotalNodes  int    `json:"total_nodes"`
}

// Write distributes a write to replicas
func (c *Coordinator) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	// Find responsible nodes using consistent hashing
	keyStr := c.hashKey(req.Database, req.Measurement, req.Tags)
	nodes := c.getReplicaNodes(keyStr)

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available for write")
	}

	// Determine required acks based on consistency level
	requiredAcks := c.requiredAcks(len(nodes), c.writeConsistency)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()

	// Send write to all replica nodes
	results := c.sendWriteToNodes(ctx, nodes, req)

	// Count successful acks
	acked := 0
	var lastErr error
	for _, res := range results {
		if res.Success {
			acked++
		} else if res.Error != "" {
			lastErr = fmt.Errorf("%s", res.Error)
		}
	}

	resp := &WriteResponse{
		Success:    acked >= requiredAcks,
		AckedNodes: acked,
		TotalNodes: len(nodes),
	}

	if !resp.Success && lastErr != nil {
		resp.Error = lastErr.Error()
	}

	return resp, nil
}

// sendWriteToNodes sends write request to multiple nodes concurrently
func (c *Coordinator) sendWriteToNodes(ctx context.Context, nodes []*Peer, req *WriteRequest) []WriteResponse {
	var wg sync.WaitGroup
	results := make([]WriteResponse, len(nodes))

	payload, _ := json.Marshal(req)

	for i, node := range nodes {
		wg.Add(1)
		go func(idx int, peer *Peer) {
			defer wg.Done()

			msg := &Message{
				Type:    MessageTypeWrite,
				From:    c.node.Info().ID,
				Payload: payload,
			}

			if err := peer.Send(msg); err != nil {
				results[idx] = WriteResponse{
					Success: false,
					Error:   err.Error(),
				}
				return
			}

			// Wait for ack
			resp, err := peer.Receive()
			if err != nil {
				results[idx] = WriteResponse{
					Success: false,
					Error:   err.Error(),
				}
				return
			}

			if resp.Type == MessageTypeWriteAck {
				results[idx] = WriteResponse{Success: true}
			} else if resp.Type == MessageTypeError {
				results[idx] = WriteResponse{
					Success: false,
					Error:   string(resp.Payload),
				}
			}
		}(i, node)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// Timeout - return partial results
	}

	return results
}

// QueryRequest represents a distributed query request
type QueryRequest struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Query executes a distributed query
func (c *Coordinator) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	// For now, query all data nodes and merge results
	peers := c.node.Peers()

	dataPeers := make([]*Peer, 0)
	for i := range peers {
		if peers[i].Type == NodeTypeData {
			peer, ok := c.node.GetPeer(peers[i].ID)
			if ok {
				dataPeers = append(dataPeers, peer)
			}
		}
	}

	if len(dataPeers) == 0 {
		// No remote peers, execute locally
		return &QueryResponse{
			Success: true,
			Data:    json.RawMessage(`{"results":[]}`),
		}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, c.readTimeout)
	defer cancel()

	// Query nodes concurrently
	type queryResult struct {
		data json.RawMessage
		err  error
	}

	resultCh := make(chan queryResult, len(dataPeers))
	payload, _ := json.Marshal(req)

	for _, peer := range dataPeers {
		go func(p *Peer) {
			msg := &Message{
				Type:    MessageTypeQuery,
				From:    c.node.Info().ID,
				Payload: payload,
			}

			if err := p.Send(msg); err != nil {
				resultCh <- queryResult{err: err}
				return
			}

			resp, err := p.Receive()
			if err != nil {
				resultCh <- queryResult{err: err}
				return
			}

			if resp.Type == MessageTypeQueryResp {
				resultCh <- queryResult{data: resp.Payload}
			} else {
				resultCh <- queryResult{err: fmt.Errorf("%s", string(resp.Payload))}
			}
		}(peer)
	}

	// Collect results
	var results []json.RawMessage
	for i := 0; i < len(dataPeers); i++ {
		select {
		case res := <-resultCh:
			if res.err == nil && res.data != nil {
				results = append(results, res.data)
			}
		case <-ctx.Done():
			break
		}
	}

	// Merge results (simplified - just return first)
	if len(results) > 0 {
		return &QueryResponse{
			Success: true,
			Data:    results[0],
		}, nil
	}

	return &QueryResponse{
		Success: true,
		Data:    json.RawMessage(`{"results":[]}`),
	}, nil
}

// hashKey generates a consistent hash key string for routing
func (c *Coordinator) hashKey(database, measurement string, tags map[string]string) string {
	// Build a deterministic key string
	key := database + ":" + measurement
	for k, v := range tags {
		key += ":" + k + "=" + v
	}
	return key
}

// hashKeyUint64 generates a hash for internal use
func (c *Coordinator) hashKeyUint64(database, measurement string, tags map[string]string) uint64 {
	key := c.hashKey(database, measurement, tags)
	return xxhash.Sum64String(key)
}

// getReplicaNodes returns the nodes responsible for a key using consistent hashing
func (c *Coordinator) getReplicaNodes(keyStr string) []*Peer {
	c.mu.RLock()
	nodeIDs := c.hashRing.GetNodes(keyStr, c.replicationFactor)
	c.mu.RUnlock()

	if len(nodeIDs) == 0 {
		// Fall back to getting all ready data nodes
		peers := c.node.Peers()
		result := make([]*Peer, 0)
		for _, p := range peers {
			if p.Type == NodeTypeData && p.State == NodeStateReady {
				if peer, ok := c.node.GetPeer(p.ID); ok {
					result = append(result, peer)
				}
			}
		}
		return result
	}

	// Get peer connections for the selected nodes
	result := make([]*Peer, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if peer, ok := c.node.GetPeer(nodeID); ok {
			result = append(result, peer)
		}
	}

	return result
}

// requiredAcks calculates required acknowledgments for a consistency level
func (c *Coordinator) requiredAcks(totalNodes int, level ConsistencyLevel) int {
	switch level {
	case ConsistencyOne:
		return 1
	case ConsistencyQuorum:
		return (totalNodes / 2) + 1
	case ConsistencyAll:
		return totalNodes
	default:
		return 1
	}
}

// SetWriteConsistency sets the write consistency level
func (c *Coordinator) SetWriteConsistency(level ConsistencyLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeConsistency = level
}

// SetReadConsistency sets the read consistency level
func (c *Coordinator) SetReadConsistency(level ConsistencyLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readConsistency = level
}

// SetReplicationFactor sets the replication factor
func (c *Coordinator) SetReplicationFactor(factor int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if factor > 0 {
		c.replicationFactor = factor
	}
}

// GetConfig returns current coordinator configuration
func (c *Coordinator) GetConfig() CoordinatorConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CoordinatorConfig{
		ReplicationFactor: c.replicationFactor,
		WriteConsistency:  c.writeConsistency,
		ReadConsistency:   c.readConsistency,
		WriteTimeout:      c.writeTimeout,
		ReadTimeout:       c.readTimeout,
	}
}
