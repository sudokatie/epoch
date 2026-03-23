package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// ReplicationManager handles data replication and anti-entropy
type ReplicationManager struct {
	mu sync.RWMutex

	node        *Node
	coordinator *Coordinator
	config      ReplicationConfig

	// Merkle trees for anti-entropy
	trees map[string]*MerkleTree

	// Background sync
	stopCh chan struct{}
	doneCh chan struct{}
}

// ReplicationConfig holds replication configuration
type ReplicationConfig struct {
	// SyncInterval is how often to run anti-entropy
	SyncInterval time.Duration
	// MaxSyncBatchSize is max items per sync batch
	MaxSyncBatchSize int
	// RepairTimeout is timeout for repair operations
	RepairTimeout time.Duration
}

// DefaultReplicationConfig returns default configuration
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		SyncInterval:     1 * time.Minute,
		MaxSyncBatchSize: 1000,
		RepairTimeout:    30 * time.Second,
	}
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(node *Node, coordinator *Coordinator, config ReplicationConfig) *ReplicationManager {
	if config.SyncInterval == 0 {
		config.SyncInterval = time.Minute
	}
	if config.MaxSyncBatchSize == 0 {
		config.MaxSyncBatchSize = 1000
	}
	if config.RepairTimeout == 0 {
		config.RepairTimeout = 30 * time.Second
	}

	return &ReplicationManager{
		node:        node,
		coordinator: coordinator,
		config:      config,
		trees:       make(map[string]*MerkleTree),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Start starts background replication
func (rm *ReplicationManager) Start() {
	go rm.syncLoop()
}

// Stop stops background replication
func (rm *ReplicationManager) Stop() {
	close(rm.stopCh)
	<-rm.doneCh
}

// syncLoop runs periodic anti-entropy sync
func (rm *ReplicationManager) syncLoop() {
	defer close(rm.doneCh)

	ticker := time.NewTicker(rm.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.runAntiEntropy()
		case <-rm.stopCh:
			return
		}
	}
}

// runAntiEntropy runs anti-entropy sync with peers
func (rm *ReplicationManager) runAntiEntropy() {
	peers := rm.node.Peers()

	for _, peerInfo := range peers {
		if peerInfo.Type != NodeTypeData {
			continue
		}

		peer, ok := rm.node.GetPeer(peerInfo.ID)
		if !ok {
			continue
		}

		rm.syncWithPeer(peer)
	}
}

// syncWithPeer synchronizes data with a peer using Merkle trees
func (rm *ReplicationManager) syncWithPeer(peer *Peer) error {
	ctx, cancel := context.WithTimeout(context.Background(), rm.config.RepairTimeout)
	defer cancel()

	// Get local tree roots
	rm.mu.RLock()
	localRoots := make(map[string]string)
	for db, tree := range rm.trees {
		localRoots[db] = tree.Root()
	}
	rm.mu.RUnlock()

	// Request remote tree roots
	req := &SyncRequest{
		Type:  SyncTypeRoots,
		Roots: localRoots,
	}

	payload, _ := json.Marshal(req)
	msg := &Message{
		Type:    MessageTypeQuery, // Reuse query message type
		From:    rm.node.Info().ID,
		Payload: payload,
	}

	if err := peer.Send(msg); err != nil {
		return err
	}

	resp, err := peer.Receive()
	if err != nil {
		return err
	}

	var syncResp SyncResponse
	if err := json.Unmarshal(resp.Payload, &syncResp); err != nil {
		return err
	}

	// Compare roots and find differences
	for db, remoteRoot := range syncResp.Roots {
		localRoot, ok := localRoots[db]
		if !ok || localRoot != remoteRoot {
			// Trees differ, need to sync this database
			rm.syncDatabase(ctx, peer, db)
		}
	}

	return nil
}

// syncDatabase synchronizes a specific database with a peer
func (rm *ReplicationManager) syncDatabase(ctx context.Context, peer *Peer, database string) error {
	// Request leaf hashes from peer
	req := &SyncRequest{
		Type:     SyncTypeLeaves,
		Database: database,
	}

	payload, _ := json.Marshal(req)
	msg := &Message{
		Type:    MessageTypeQuery,
		From:    rm.node.Info().ID,
		Payload: payload,
	}

	if err := peer.Send(msg); err != nil {
		return err
	}

	resp, err := peer.Receive()
	if err != nil {
		return err
	}

	var syncResp SyncResponse
	if err := json.Unmarshal(resp.Payload, &syncResp); err != nil {
		return err
	}

	// Find missing keys
	rm.mu.RLock()
	localTree, ok := rm.trees[database]
	rm.mu.RUnlock()

	if !ok {
		return nil
	}

	localLeaves := localTree.Leaves()

	// Find keys we're missing
	missing := make([]string, 0)
	for key := range syncResp.Leaves {
		if _, ok := localLeaves[key]; !ok {
			missing = append(missing, key)
		}
	}

	// Request missing data
	if len(missing) > 0 {
		rm.requestMissingData(ctx, peer, database, missing)
	}

	return nil
}

// requestMissingData requests missing data from a peer
func (rm *ReplicationManager) requestMissingData(ctx context.Context, peer *Peer, database string, keys []string) error {
	// Batch requests
	for i := 0; i < len(keys); i += rm.config.MaxSyncBatchSize {
		end := i + rm.config.MaxSyncBatchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]

		req := &SyncRequest{
			Type:     SyncTypeData,
			Database: database,
			Keys:     batch,
		}

		payload, _ := json.Marshal(req)
		msg := &Message{
			Type:    MessageTypeQuery,
			From:    rm.node.Info().ID,
			Payload: payload,
		}

		if err := peer.Send(msg); err != nil {
			return err
		}

		resp, err := peer.Receive()
		if err != nil {
			return err
		}

		var syncResp SyncResponse
		if err := json.Unmarshal(resp.Payload, &syncResp); err != nil {
			return err
		}

		// Store received data locally
		for key, data := range syncResp.Data {
			rm.storeRepairedData(database, key, data)
		}
	}

	return nil
}

// storeRepairedData stores data received during repair
func (rm *ReplicationManager) storeRepairedData(database, key string, data []byte) {
	// This would integrate with the storage engine
	// For now, just update the Merkle tree
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tree, ok := rm.trees[database]
	if !ok {
		tree = NewMerkleTree()
		rm.trees[database] = tree
	}

	tree.Insert(key, data)
}

// UpdateTree updates the Merkle tree for a database
func (rm *ReplicationManager) UpdateTree(database, key string, data []byte) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tree, ok := rm.trees[database]
	if !ok {
		tree = NewMerkleTree()
		rm.trees[database] = tree
	}

	tree.Insert(key, data)
}

// GetTreeRoot returns the Merkle tree root for a database
func (rm *ReplicationManager) GetTreeRoot(database string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	tree, ok := rm.trees[database]
	if !ok {
		return ""
	}

	return tree.Root()
}

// SyncType represents the type of sync request
type SyncType int

const (
	SyncTypeRoots SyncType = iota
	SyncTypeLeaves
	SyncTypeData
)

// SyncRequest represents a sync request
type SyncRequest struct {
	Type     SyncType          `json:"type"`
	Database string            `json:"database,omitempty"`
	Roots    map[string]string `json:"roots,omitempty"`
	Keys     []string          `json:"keys,omitempty"`
}

// SyncResponse represents a sync response
type SyncResponse struct {
	Roots  map[string]string `json:"roots,omitempty"`
	Leaves map[string]string `json:"leaves,omitempty"`
	Data   map[string][]byte `json:"data,omitempty"`
}

// MerkleTree is a simple Merkle tree for anti-entropy
type MerkleTree struct {
	mu     sync.RWMutex
	leaves map[string]string // key -> hash
	root   string
	dirty  bool
}

// NewMerkleTree creates a new Merkle tree
func NewMerkleTree() *MerkleTree {
	return &MerkleTree{
		leaves: make(map[string]string),
	}
}

// Insert inserts or updates a key
func (t *MerkleTree) Insert(key string, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash := sha256.Sum256(data)
	t.leaves[key] = hex.EncodeToString(hash[:])
	t.dirty = true
}

// Delete removes a key
func (t *MerkleTree) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.leaves, key)
	t.dirty = true
}

// Root returns the root hash
func (t *MerkleTree) Root() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.dirty {
		t.recalculateRoot()
	}

	return t.root
}

// Leaves returns all leaf hashes
func (t *MerkleTree) Leaves() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]string, len(t.leaves))
	for k, v := range t.leaves {
		result[k] = v
	}
	return result
}

// recalculateRoot recalculates the root hash
func (t *MerkleTree) recalculateRoot() {
	if len(t.leaves) == 0 {
		t.root = ""
		t.dirty = false
		return
	}

	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(t.leaves))
	for k := range t.leaves {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build tree from leaves
	hashes := make([]string, len(keys))
	for i, k := range keys {
		hashes[i] = t.leaves[k]
	}

	// Iteratively hash pairs until we get root
	for len(hashes) > 1 {
		nextLevel := make([]string, 0, (len(hashes)+1)/2)
		for i := 0; i < len(hashes); i += 2 {
			if i+1 < len(hashes) {
				combined := hashes[i] + hashes[i+1]
				hash := sha256.Sum256([]byte(combined))
				nextLevel = append(nextLevel, hex.EncodeToString(hash[:]))
			} else {
				nextLevel = append(nextLevel, hashes[i])
			}
		}
		hashes = nextLevel
	}

	t.root = hashes[0]
	t.dirty = false
}

// Diff returns keys that differ between two trees
func (t *MerkleTree) Diff(other *MerkleTree) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	diff := make([]string, 0)

	// Find keys in other but not in t, or with different hash
	for k, v := range other.leaves {
		if localHash, ok := t.leaves[k]; !ok || localHash != v {
			diff = append(diff, k)
		}
	}

	return diff
}

// Size returns the number of leaves
func (t *MerkleTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.leaves)
}
