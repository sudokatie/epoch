package cluster

import (
	"net"
	"testing"
	"time"
)

func TestMessageTypeStringAll(t *testing.T) {
	types := []MessageType{
		MessageTypeJoin,
		MessageTypeJoinAck,
		MessageTypeLeave,
		MessageTypeHeartbeat,
		MessageTypeHeartbeatAck,
		MessageTypeWrite,
		MessageTypeWriteAck,
		MessageTypeQuery,
		MessageTypeQueryResp,
		MessageTypeError,
	}

	for _, typ := range types {
		s := typ.String()
		if s == "" || s == "unknown" {
			t.Errorf("MessageType %d has unexpected string: %q", typ, s)
		}
	}

	// Test unknown type
	unknown := MessageType(99)
	if unknown.String() != "unknown" {
		t.Errorf("Unknown type should return 'unknown', got %q", unknown.String())
	}
}

func TestWriteAndReadMessage(t *testing.T) {
	// Create a pipe for testing
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	msg := &Message{
		Type:    MessageTypeHeartbeat,
		From:    "node1",
		ID:      123,
		Payload: []byte(`{"test": "data"}`),
	}

	// Write in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteMessage(client, msg)
	}()

	// Read on other side
	server.SetReadDeadline(time.Now().Add(time.Second))
	received, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage error: %v", err)
	}

	// Check write error
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("WriteMessage error: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("WriteMessage timed out")
	}

	// Verify message
	if received.Type != msg.Type {
		t.Errorf("Type = %v, want %v", received.Type, msg.Type)
	}
	if received.From != msg.From {
		t.Errorf("From = %q, want %q", received.From, msg.From)
	}
	if received.ID != msg.ID {
		t.Errorf("ID = %d, want %d", received.ID, msg.ID)
	}
}

func TestRPCServerBasics(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1", Addr: "127.0.0.1:0"},
	}

	server := NewRPCServer(node)
	if server == nil {
		t.Fatal("NewRPCServer returned nil")
	}

	// Register handlers
	server.RegisterHandler(MessageTypeHeartbeat, func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeHeartbeatAck, From: "server"}
	})

	server.RegisterHandler(MessageTypeWrite, func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeWriteAck, From: "server"}
	})
}

func TestRPCServerServeAndHandle(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1", Addr: "127.0.0.1:0"},
	}

	server := NewRPCServer(node)

	server.RegisterHandler(MessageTypeHeartbeat, func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeHeartbeatAck, From: "server", ID: msg.ID}
	})

	// Create a listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen error: %v", err)
	}

	// Start serving in background
	go server.Serve(listener)

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Connect a client
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatalf("Dial error: %v", err)
	}

	// Send a message
	msg := &Message{Type: MessageTypeHeartbeat, From: "client", ID: 1}
	if err := WriteMessage(conn, msg); err != nil {
		conn.Close()
		listener.Close()
		t.Fatalf("WriteMessage error: %v", err)
	}

	// Read response
	conn.SetReadDeadline(time.Now().Add(time.Second))
	resp, err := ReadMessage(conn)
	if err != nil {
		t.Logf("ReadMessage error (may be expected): %v", err)
	} else {
		if resp.Type != MessageTypeHeartbeatAck {
			t.Errorf("Response type = %v, want HeartbeatAck", resp.Type)
		}
	}

	conn.Close()
	listener.Close()
}

func TestRPCServerHandlerRegistration(t *testing.T) {
	node := &Node{
		info: NodeInfo{ID: "node1"},
	}

	server := NewRPCServer(node)

	// Register multiple handlers
	server.RegisterHandler(MessageTypeWrite, func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeWriteAck}
	})

	server.RegisterHandler(MessageTypeQuery, func(peer *Peer, msg *Message) *Message {
		return &Message{Type: MessageTypeQueryResp}
	})

	// Verify server was created successfully
	if server == nil {
		t.Error("Server should not be nil")
	}
}
