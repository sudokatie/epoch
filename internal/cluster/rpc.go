package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

// MessageType represents the type of cluster message
type MessageType uint8

const (
	// MessageTypeJoin is sent when a node joins
	MessageTypeJoin MessageType = iota + 1
	// MessageTypeJoinAck is the acknowledgment of a join
	MessageTypeJoinAck
	// MessageTypeLeave is sent when a node leaves
	MessageTypeLeave
	// MessageTypeHeartbeat is a keepalive message
	MessageTypeHeartbeat
	// MessageTypeHeartbeatAck is the acknowledgment of a heartbeat
	MessageTypeHeartbeatAck
	// MessageTypeWrite is a write request
	MessageTypeWrite
	// MessageTypeWriteAck is the acknowledgment of a write
	MessageTypeWriteAck
	// MessageTypeQuery is a query request
	MessageTypeQuery
	// MessageTypeQueryResp is a query response
	MessageTypeQueryResp
	// MessageTypeError is an error response
	MessageTypeError
)

func (t MessageType) String() string {
	switch t {
	case MessageTypeJoin:
		return "join"
	case MessageTypeJoinAck:
		return "join_ack"
	case MessageTypeLeave:
		return "leave"
	case MessageTypeHeartbeat:
		return "heartbeat"
	case MessageTypeHeartbeatAck:
		return "heartbeat_ack"
	case MessageTypeWrite:
		return "write"
	case MessageTypeWriteAck:
		return "write_ack"
	case MessageTypeQuery:
		return "query"
	case MessageTypeQueryResp:
		return "query_resp"
	case MessageTypeError:
		return "error"
	default:
		return "unknown"
	}
}

// Message is a cluster communication message
type Message struct {
	Type    MessageType `json:"type"`
	From    string      `json:"from"`
	ID      uint64      `json:"id,omitempty"`
	Payload []byte      `json:"payload,omitempty"`
}

// WriteMessage writes a message to a connection
func WriteMessage(conn net.Conn, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Write length prefix (4 bytes)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}

// ReadMessage reads a message from a connection
func ReadMessage(conn net.Conn) (*Message, error) {
	// Read length prefix
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}

	length := binary.BigEndian.Uint32(lenBuf)
	if length > 10*1024*1024 { // 10MB max
		return nil, fmt.Errorf("message too large: %d", length)
	}

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}

	return &msg, nil
}

// RPCServer handles RPC requests
type RPCServer struct {
	mu sync.RWMutex

	node     *Node
	handlers map[MessageType]MessageHandler
}

// MessageHandler handles a specific message type
type MessageHandler func(peer *Peer, msg *Message) *Message

// NewRPCServer creates a new RPC server
func NewRPCServer(node *Node) *RPCServer {
	return &RPCServer{
		node:     node,
		handlers: make(map[MessageType]MessageHandler),
	}
}

// RegisterHandler registers a handler for a message type
func (s *RPCServer) RegisterHandler(msgType MessageType, handler MessageHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[msgType] = handler
}

// Serve starts serving RPC requests
func (s *RPCServer) Serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		go s.handleConnection(conn)
	}
}

// handleConnection handles a new connection
func (s *RPCServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read first message (should be join)
	msg, err := ReadMessage(conn)
	if err != nil {
		return
	}

	if msg.Type != MessageTypeJoin {
		// Not a join message, close connection
		return
	}

	// Parse peer info
	var peerInfo NodeInfo
	if err := json.Unmarshal(msg.Payload, &peerInfo); err != nil {
		return
	}

	// Send join ack with our info
	ack := &Message{
		Type:    MessageTypeJoinAck,
		From:    s.node.info.ID,
		Payload: mustMarshal(s.node.info),
	}

	if err := WriteMessage(conn, ack); err != nil {
		return
	}

	// Create peer
	peer := NewPeer(peerInfo, conn)

	s.node.mu.Lock()
	s.node.peers[peerInfo.ID] = peer
	s.node.mu.Unlock()

	// Handle peer messages
	s.node.handlePeer(peer)
}

// HandleMessage handles a message received from a peer
func (s *RPCServer) HandleMessage(peer *Peer, msg *Message) {
	s.mu.RLock()
	handler, ok := s.handlers[msg.Type]
	s.mu.RUnlock()

	if !ok {
		// No handler, send error
		peer.Send(&Message{
			Type:    MessageTypeError,
			From:    s.node.info.ID,
			ID:      msg.ID,
			Payload: []byte("unknown message type"),
		})
		return
	}

	// Call handler
	resp := handler(peer, msg)
	if resp != nil {
		resp.ID = msg.ID
		peer.Send(resp)
	}
}

// RPCClient is a client for making RPC calls
type RPCClient struct {
	mu sync.Mutex

	peer   *Peer
	nextID uint64
	pending map[uint64]chan *Message
}

// NewRPCClient creates a new RPC client for a peer
func NewRPCClient(peer *Peer) *RPCClient {
	c := &RPCClient{
		peer:    peer,
		nextID:  1,
		pending: make(map[uint64]chan *Message),
	}

	// Start response handler
	go c.handleResponses()

	return c
}

// Call makes an RPC call and waits for a response
func (c *RPCClient) Call(msgType MessageType, payload []byte) (*Message, error) {
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	respCh := make(chan *Message, 1)
	c.pending[id] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	// Send request
	msg := &Message{
		Type:    msgType,
		From:    "",  // Will be set by peer
		ID:      id,
		Payload: payload,
	}

	if err := c.peer.Send(msg); err != nil {
		return nil, err
	}

	// Wait for response
	resp := <-respCh
	if resp.Type == MessageTypeError {
		return nil, fmt.Errorf("%s", resp.Payload)
	}

	return resp, nil
}

// handleResponses handles incoming responses
func (c *RPCClient) handleResponses() {
	for {
		msg, err := c.peer.Receive()
		if err != nil {
			return
		}

		c.mu.Lock()
		if ch, ok := c.pending[msg.ID]; ok {
			ch <- msg
		}
		c.mu.Unlock()
	}
}

// Close closes the client
func (c *RPCClient) Close() error {
	return c.peer.Close()
}
