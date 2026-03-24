package cluster

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// HashRing implements consistent hashing for shard placement
type HashRing struct {
	mu sync.RWMutex

	// Virtual nodes per physical node (for better distribution)
	virtualNodes int

	// Sorted list of hash values
	ring []uint64

	// Map from hash value to node ID
	hashToNode map[uint64]string

	// Map from node ID to its hash values
	nodeToHashes map[string][]uint64

	// Replication factor
	replicationFactor int
}

// HashRingConfig holds configuration for the hash ring
type HashRingConfig struct {
	VirtualNodes      int
	ReplicationFactor int
}

// DefaultHashRingConfig returns sensible defaults
func DefaultHashRingConfig() HashRingConfig {
	return HashRingConfig{
		VirtualNodes:      150, // 150 virtual nodes per physical node
		ReplicationFactor: 2,
	}
}

// NewHashRing creates a new consistent hash ring
func NewHashRing(config HashRingConfig) *HashRing {
	if config.VirtualNodes == 0 {
		config.VirtualNodes = 150
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 2
	}

	return &HashRing{
		virtualNodes:      config.VirtualNodes,
		ring:              make([]uint64, 0),
		hashToNode:        make(map[uint64]string),
		nodeToHashes:      make(map[string][]uint64),
		replicationFactor: config.ReplicationFactor,
	}
}

// AddNode adds a node to the ring
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodeToHashes[nodeID]; exists {
		return // Already added
	}

	hashes := make([]uint64, hr.virtualNodes)
	for i := 0; i < hr.virtualNodes; i++ {
		key := fmt.Sprintf("%s-%d", nodeID, i)
		hash := xxhash.Sum64String(key)
		hashes[i] = hash
		hr.hashToNode[hash] = nodeID
		hr.ring = append(hr.ring, hash)
	}

	hr.nodeToHashes[nodeID] = hashes
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// RemoveNode removes a node from the ring
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hashes, exists := hr.nodeToHashes[nodeID]
	if !exists {
		return
	}

	// Remove all virtual nodes
	hashSet := make(map[uint64]bool)
	for _, h := range hashes {
		hashSet[h] = true
		delete(hr.hashToNode, h)
	}

	// Rebuild ring without removed hashes
	newRing := make([]uint64, 0, len(hr.ring)-len(hashes))
	for _, h := range hr.ring {
		if !hashSet[h] {
			newRing = append(newRing, h)
		}
	}
	hr.ring = newRing

	delete(hr.nodeToHashes, nodeID)
}

// GetNode returns the node responsible for a key
func (hr *HashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return ""
	}

	hash := xxhash.Sum64String(key)
	idx := hr.search(hash)
	return hr.hashToNode[hr.ring[idx]]
}

// GetNodes returns the N nodes responsible for a key (for replication)
func (hr *HashRing) GetNodes(key string, n int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return nil
	}

	if n > len(hr.nodeToHashes) {
		n = len(hr.nodeToHashes)
	}

	hash := xxhash.Sum64String(key)
	idx := hr.search(hash)

	seen := make(map[string]bool)
	nodes := make([]string, 0, n)

	for i := 0; i < len(hr.ring) && len(nodes) < n; i++ {
		pos := (idx + i) % len(hr.ring)
		nodeID := hr.hashToNode[hr.ring[pos]]
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}

	return nodes
}

// GetNodesForShard returns the nodes that should store a shard
func (hr *HashRing) GetNodesForShard(database, measurement string, shardID uint64) []string {
	key := fmt.Sprintf("%s:%s:%d", database, measurement, shardID)
	return hr.GetNodes(key, hr.replicationFactor)
}

// search finds the first ring position >= hash
func (hr *HashRing) search(hash uint64) int {
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= hash
	})
	if idx >= len(hr.ring) {
		idx = 0 // Wrap around
	}
	return idx
}

// NodeCount returns the number of nodes in the ring
func (hr *HashRing) NodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodeToHashes)
}

// Nodes returns all node IDs
func (hr *HashRing) Nodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]string, 0, len(hr.nodeToHashes))
	for nodeID := range hr.nodeToHashes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetReplicationFactor returns the replication factor
func (hr *HashRing) GetReplicationFactor() int {
	return hr.replicationFactor
}

// SetReplicationFactor updates the replication factor
func (hr *HashRing) SetReplicationFactor(rf int) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.replicationFactor = rf
}
