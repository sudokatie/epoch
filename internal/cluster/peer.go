package cluster

import (
	"net"
	"sync"
	"time"
)

// Peer represents a connection to a remote node
type Peer struct {
	mu sync.RWMutex

	info     NodeInfo
	conn     net.Conn
	lastSeen time.Time
	sendMu   sync.Mutex
}

// NewPeer creates a new peer
func NewPeer(info NodeInfo, conn net.Conn) *Peer {
	return &Peer{
		info:     info,
		conn:     conn,
		lastSeen: time.Now(),
	}
}

// Info returns peer information
func (p *Peer) Info() NodeInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info
}

// LastSeen returns when the peer was last seen
func (p *Peer) LastSeen() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastSeen
}

// UpdateLastSeen updates the last seen time
func (p *Peer) UpdateLastSeen() {
	p.mu.Lock()
	p.lastSeen = time.Now()
	p.mu.Unlock()
}

// Send sends a message to the peer
func (p *Peer) Send(msg *Message) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	return WriteMessage(p.conn, msg)
}

// Receive receives a message from the peer
func (p *Peer) Receive() (*Message, error) {
	return ReadMessage(p.conn)
}

// Close closes the peer connection
func (p *Peer) Close() error {
	return p.conn.Close()
}

// Addr returns the peer's address
func (p *Peer) Addr() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info.Addr
}

// ID returns the peer's ID
func (p *Peer) ID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info.ID
}

// State returns the peer's state
func (p *Peer) State() NodeState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info.State
}

// Type returns the peer's type
func (p *Peer) Type() NodeType {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info.Type
}

// IsReady returns true if the peer is ready
func (p *Peer) IsReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.info.State == NodeStateReady
}

// UpdateInfo updates the peer's info
func (p *Peer) UpdateInfo(info NodeInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.info = info
}
