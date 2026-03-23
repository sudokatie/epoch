package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// NodeType represents the type of node in the cluster
type NodeType int

const (
	// NodeTypeData is a node that stores data
	NodeTypeData NodeType = iota
	// NodeTypeMeta is a node that stores cluster metadata
	NodeTypeMeta
	// NodeTypeCoordinator is a node that coordinates queries
	NodeTypeCoordinator
)

func (t NodeType) String() string {
	switch t {
	case NodeTypeData:
		return "data"
	case NodeTypeMeta:
		return "meta"
	case NodeTypeCoordinator:
		return "coordinator"
	default:
		return "unknown"
	}
}

// NodeState represents the state of a node
type NodeState int

const (
	// NodeStateJoining is when a node is joining the cluster
	NodeStateJoining NodeState = iota
	// NodeStateReady is when a node is ready to serve
	NodeStateReady
	// NodeStateLeaving is when a node is leaving the cluster
	NodeStateLeaving
	// NodeStateFailed is when a node has failed
	NodeStateFailed
)

func (s NodeState) String() string {
	switch s {
	case NodeStateJoining:
		return "joining"
	case NodeStateReady:
		return "ready"
	case NodeStateLeaving:
		return "leaving"
	case NodeStateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// NodeInfo contains information about a cluster node
type NodeInfo struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"`
	RPCAddr   string    `json:"rpc_addr"`
	Type      NodeType  `json:"type"`
	State     NodeState `json:"state"`
	JoinedAt  time.Time `json:"joined_at"`
	LastSeen  time.Time `json:"last_seen"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// Node represents a local node in the cluster
type Node struct {
	mu sync.RWMutex

	// Node information
	info NodeInfo

	// Configuration
	config NodeConfig

	// Peer connections
	peers map[string]*Peer

	// RPC server
	rpcListener net.Listener
	rpcServer   *RPCServer

	// Lifecycle
	stopCh chan struct{}
	doneCh chan struct{}
}

// NodeConfig holds node configuration
type NodeConfig struct {
	// ID is the unique identifier for this node
	ID string
	// BindAddr is the address to bind for peer communication
	BindAddr string
	// RPCAddr is the address for RPC communication
	RPCAddr string
	// Type is the node type
	Type NodeType
	// Peers is a list of initial peer addresses
	Peers []string
	// Tags are optional metadata tags
	Tags map[string]string
	// HeartbeatInterval is how often to send heartbeats
	HeartbeatInterval time.Duration
	// HeartbeatTimeout is when to consider a peer failed
	HeartbeatTimeout time.Duration
}

// DefaultNodeConfig returns default node configuration
func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		BindAddr:          ":7946",
		RPCAddr:           ":7947",
		Type:              NodeTypeData,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
	}
}

// NewNode creates a new cluster node
func NewNode(config NodeConfig) (*Node, error) {
	if config.ID == "" {
		config.ID = generateNodeID()
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = time.Second
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 5 * time.Second
	}

	n := &Node{
		info: NodeInfo{
			ID:       config.ID,
			Addr:     config.BindAddr,
			RPCAddr:  config.RPCAddr,
			Type:     config.Type,
			State:    NodeStateJoining,
			JoinedAt: time.Now(),
			LastSeen: time.Now(),
			Tags:     config.Tags,
		},
		config: config,
		peers:  make(map[string]*Peer),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	return n, nil
}

// Start starts the node
func (n *Node) Start() error {
	// Start RPC server
	listener, err := net.Listen("tcp", n.config.RPCAddr)
	if err != nil {
		return fmt.Errorf("failed to start RPC listener: %w", err)
	}
	n.rpcListener = listener

	n.rpcServer = NewRPCServer(n)
	go n.rpcServer.Serve(listener)

	// Connect to initial peers
	for _, addr := range n.config.Peers {
		go n.connectToPeer(addr)
	}

	// Start heartbeat loop
	go n.heartbeatLoop()

	// Mark as ready
	n.mu.Lock()
	n.info.State = NodeStateReady
	n.mu.Unlock()

	return nil
}

// Stop stops the node
func (n *Node) Stop() error {
	close(n.stopCh)

	// Mark as leaving
	n.mu.Lock()
	n.info.State = NodeStateLeaving
	n.mu.Unlock()

	// Notify peers
	n.broadcastLeave()

	// Close RPC listener
	if n.rpcListener != nil {
		n.rpcListener.Close()
	}

	// Close peer connections
	n.mu.Lock()
	for _, peer := range n.peers {
		peer.Close()
	}
	n.mu.Unlock()

	<-n.doneCh
	return nil
}

// Info returns node information
func (n *Node) Info() NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.info
}

// Peers returns information about known peers
func (n *Node) Peers() []NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	infos := make([]NodeInfo, 0, len(n.peers))
	for _, peer := range n.peers {
		infos = append(infos, peer.Info())
	}
	return infos
}

// GetPeer returns a peer by ID
func (n *Node) GetPeer(id string) (*Peer, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer, ok := n.peers[id]
	return peer, ok
}

// connectToPeer connects to a peer at the given address
func (n *Node) connectToPeer(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}

	// Send join message
	join := &Message{
		Type: MessageTypeJoin,
		From: n.info.ID,
		Payload: mustMarshal(n.info),
	}

	if err := WriteMessage(conn, join); err != nil {
		conn.Close()
		return fmt.Errorf("failed to send join message: %w", err)
	}

	// Read response
	resp, err := ReadMessage(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to read join response: %w", err)
	}

	if resp.Type != MessageTypeJoinAck {
		conn.Close()
		return fmt.Errorf("unexpected response type: %d", resp.Type)
	}

	// Parse peer info
	var peerInfo NodeInfo
	if err := json.Unmarshal(resp.Payload, &peerInfo); err != nil {
		conn.Close()
		return fmt.Errorf("failed to parse peer info: %w", err)
	}

	// Create peer
	peer := NewPeer(peerInfo, conn)

	n.mu.Lock()
	n.peers[peerInfo.ID] = peer
	n.mu.Unlock()

	// Start handling peer messages
	go n.handlePeer(peer)

	return nil
}

// handlePeer handles messages from a peer
func (n *Node) handlePeer(peer *Peer) {
	defer func() {
		peer.Close()
		n.mu.Lock()
		delete(n.peers, peer.Info().ID)
		n.mu.Unlock()
	}()

	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		msg, err := peer.Receive()
		if err != nil {
			return
		}

		n.handleMessage(peer, msg)
	}
}

// handleMessage handles a message from a peer
func (n *Node) handleMessage(peer *Peer, msg *Message) {
	switch msg.Type {
	case MessageTypeHeartbeat:
		// Update last seen
		peer.UpdateLastSeen()
		// Send heartbeat ack
		peer.Send(&Message{
			Type: MessageTypeHeartbeatAck,
			From: n.info.ID,
		})

	case MessageTypeHeartbeatAck:
		peer.UpdateLastSeen()

	case MessageTypeLeave:
		// Peer is leaving
		n.mu.Lock()
		delete(n.peers, peer.Info().ID)
		n.mu.Unlock()

	default:
		// Handle via RPC server
		if n.rpcServer != nil {
			n.rpcServer.HandleMessage(peer, msg)
		}
	}
}

// heartbeatLoop sends periodic heartbeats to peers
func (n *Node) heartbeatLoop() {
	defer close(n.doneCh)

	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.sendHeartbeats()
			n.checkPeerHealth()
		case <-n.stopCh:
			return
		}
	}
}

// sendHeartbeats sends heartbeat to all peers
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	peers := make([]*Peer, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()

	msg := &Message{
		Type: MessageTypeHeartbeat,
		From: n.info.ID,
	}

	for _, peer := range peers {
		peer.Send(msg)
	}
}

// checkPeerHealth checks for failed peers
func (n *Node) checkPeerHealth() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	for id, peer := range n.peers {
		if now.Sub(peer.LastSeen()) > n.config.HeartbeatTimeout {
			peer.Close()
			delete(n.peers, id)
		}
	}
}

// broadcastLeave notifies all peers that this node is leaving
func (n *Node) broadcastLeave() {
	n.mu.RLock()
	peers := make([]*Peer, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()

	msg := &Message{
		Type: MessageTypeLeave,
		From: n.info.ID,
	}

	for _, peer := range peers {
		peer.Send(msg)
	}
}

// generateNodeID generates a unique node ID
func generateNodeID() string {
	return fmt.Sprintf("node-%d", time.Now().UnixNano())
}

// mustMarshal marshals to JSON, panicking on error
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
