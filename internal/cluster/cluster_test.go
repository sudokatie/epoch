package cluster

import (
	"net"
	"testing"
	"time"
)

func TestNodeTypeString(t *testing.T) {
	tests := []struct {
		input NodeType
		want  string
	}{
		{NodeTypeData, "data"},
		{NodeTypeMeta, "meta"},
		{NodeTypeCoordinator, "coordinator"},
		{NodeType(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.input.String()
		if got != tt.want {
			t.Errorf("NodeType(%d).String() = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		input NodeState
		want  string
	}{
		{NodeStateJoining, "joining"},
		{NodeStateReady, "ready"},
		{NodeStateLeaving, "leaving"},
		{NodeStateFailed, "failed"},
		{NodeState(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.input.String()
		if got != tt.want {
			t.Errorf("NodeState(%d).String() = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMessageTypeString(t *testing.T) {
	tests := []struct {
		input MessageType
		want  string
	}{
		{MessageTypeJoin, "join"},
		{MessageTypeJoinAck, "join_ack"},
		{MessageTypeLeave, "leave"},
		{MessageTypeHeartbeat, "heartbeat"},
		{MessageTypeHeartbeatAck, "heartbeat_ack"},
		{MessageTypeWrite, "write"},
		{MessageTypeWriteAck, "write_ack"},
		{MessageTypeQuery, "query"},
		{MessageTypeQueryResp, "query_resp"},
		{MessageTypeError, "error"},
		{MessageType(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.input.String()
		if got != tt.want {
			t.Errorf("MessageType(%d).String() = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDefaultNodeConfig(t *testing.T) {
	config := DefaultNodeConfig()

	if config.BindAddr != ":7946" {
		t.Errorf("got bind addr %q, want :7946", config.BindAddr)
	}

	if config.RPCAddr != ":7947" {
		t.Errorf("got rpc addr %q, want :7947", config.RPCAddr)
	}

	if config.Type != NodeTypeData {
		t.Errorf("got type %v, want data", config.Type)
	}

	if config.HeartbeatInterval != time.Second {
		t.Errorf("got heartbeat interval %v, want 1s", config.HeartbeatInterval)
	}

	if config.HeartbeatTimeout != 5*time.Second {
		t.Errorf("got heartbeat timeout %v, want 5s", config.HeartbeatTimeout)
	}
}

func TestNewNode(t *testing.T) {
	config := DefaultNodeConfig()
	config.ID = "test-node"
	config.BindAddr = "localhost:0"
	config.RPCAddr = "localhost:0"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	info := node.Info()

	if info.ID != "test-node" {
		t.Errorf("got id %q, want test-node", info.ID)
	}

	if info.State != NodeStateJoining {
		t.Errorf("got state %v, want joining", info.State)
	}

	if info.Type != NodeTypeData {
		t.Errorf("got type %v, want data", info.Type)
	}
}

func TestNodeGeneratesID(t *testing.T) {
	config := DefaultNodeConfig()
	config.BindAddr = "localhost:0"
	config.RPCAddr = "localhost:0"

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	info := node.Info()
	if info.ID == "" {
		t.Error("expected generated ID")
	}
}

func TestWriteReadMessage(t *testing.T) {
	// Create a pipe for testing
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	msg := &Message{
		Type:    MessageTypeHeartbeat,
		From:    "test-node",
		ID:      123,
		Payload: []byte("test payload"),
	}

	// Write from client
	go func() {
		WriteMessage(client, msg)
	}()

	// Read from server
	received, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	if received.Type != msg.Type {
		t.Errorf("got type %v, want %v", received.Type, msg.Type)
	}

	if received.From != msg.From {
		t.Errorf("got from %q, want %q", received.From, msg.From)
	}

	if received.ID != msg.ID {
		t.Errorf("got id %d, want %d", received.ID, msg.ID)
	}

	if string(received.Payload) != string(msg.Payload) {
		t.Errorf("got payload %q, want %q", received.Payload, msg.Payload)
	}
}

func TestPeer(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	info := NodeInfo{
		ID:    "peer-1",
		Addr:  "localhost:7946",
		Type:  NodeTypeData,
		State: NodeStateReady,
	}

	peer := NewPeer(info, client)

	if peer.ID() != "peer-1" {
		t.Errorf("got id %q, want peer-1", peer.ID())
	}

	if peer.Addr() != "localhost:7946" {
		t.Errorf("got addr %q, want localhost:7946", peer.Addr())
	}

	if peer.Type() != NodeTypeData {
		t.Errorf("got type %v, want data", peer.Type())
	}

	if peer.State() != NodeStateReady {
		t.Errorf("got state %v, want ready", peer.State())
	}

	if !peer.IsReady() {
		t.Error("expected peer to be ready")
	}
}

func TestPeerUpdateLastSeen(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	info := NodeInfo{ID: "peer-1"}
	peer := NewPeer(info, client)

	before := peer.LastSeen()
	time.Sleep(10 * time.Millisecond)
	peer.UpdateLastSeen()
	after := peer.LastSeen()

	if !after.After(before) {
		t.Error("expected last seen to be updated")
	}
}

func TestPeerSendReceive(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	info := NodeInfo{ID: "peer-1"}
	peer := NewPeer(info, client)

	msg := &Message{
		Type: MessageTypeHeartbeat,
		From: "test",
	}

	// Send from peer
	go func() {
		peer.Send(msg)
	}()

	// Read from server
	received, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	if received.Type != MessageTypeHeartbeat {
		t.Errorf("got type %v, want heartbeat", received.Type)
	}
}

func TestPeerUpdateInfo(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	info := NodeInfo{ID: "peer-1", State: NodeStateJoining}
	peer := NewPeer(info, client)

	if peer.State() != NodeStateJoining {
		t.Errorf("got state %v, want joining", peer.State())
	}

	newInfo := NodeInfo{ID: "peer-1", State: NodeStateReady}
	peer.UpdateInfo(newInfo)

	if peer.State() != NodeStateReady {
		t.Errorf("got state %v, want ready", peer.State())
	}
}

func TestRPCServerRegisterHandler(t *testing.T) {
	config := DefaultNodeConfig()
	config.ID = "test"
	config.BindAddr = "localhost:0"
	config.RPCAddr = "localhost:0"

	node, _ := NewNode(config)
	server := NewRPCServer(node)

	handler := func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeWriteAck}
	}

	server.RegisterHandler(MessageTypeWrite, handler)

	// Verify handler was registered
	server.mu.RLock()
	_, ok := server.handlers[MessageTypeWrite]
	server.mu.RUnlock()

	if !ok {
		t.Error("handler was not registered")
	}
}

func TestNodeStartStop(t *testing.T) {
	config := DefaultNodeConfig()
	config.ID = "test-node"
	config.BindAddr = "localhost:0"
	config.RPCAddr = "localhost:0"
	config.HeartbeatInterval = 50 * time.Millisecond

	node, err := NewNode(config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	// Give it time to run
	time.Sleep(100 * time.Millisecond)

	info := node.Info()
	if info.State != NodeStateReady {
		t.Errorf("got state %v, want ready", info.State)
	}

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		node.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop timed out")
	}
}

func TestTwoNodeCluster(t *testing.T) {
	// Create first node
	config1 := DefaultNodeConfig()
	config1.ID = "node-1"
	config1.BindAddr = "localhost:0"
	config1.RPCAddr = "localhost:17947"
	config1.HeartbeatInterval = 50 * time.Millisecond

	node1, err := NewNode(config1)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}

	if err := node1.Start(); err != nil {
		t.Fatalf("failed to start node1: %v", err)
	}
	defer node1.Stop()

	// Create second node connecting to first
	config2 := DefaultNodeConfig()
	config2.ID = "node-2"
	config2.BindAddr = "localhost:0"
	config2.RPCAddr = "localhost:17948"
	config2.Peers = []string{"localhost:17947"}
	config2.HeartbeatInterval = 50 * time.Millisecond

	node2, err := NewNode(config2)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}

	if err := node2.Start(); err != nil {
		t.Fatalf("failed to start node2: %v", err)
	}
	defer node2.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Check peers
	peers1 := node1.Peers()
	peers2 := node2.Peers()

	if len(peers1) != 1 {
		t.Errorf("node1 has %d peers, want 1", len(peers1))
	}

	if len(peers2) != 1 {
		t.Errorf("node2 has %d peers, want 1", len(peers2))
	}
}
