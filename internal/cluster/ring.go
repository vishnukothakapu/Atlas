package cluster

import (
	"hash/crc32"
	"sort"
	"sync"

	"github.com/vishnukothakapu/atlas/internal/storage"
)

type HashRing struct {
	mu        sync.RWMutex
	nodes     map[uint32]*storage.Node
	nodeIndex map[string]*storage.Node
	keys      []uint32
}

func NewHashRing() *HashRing {
	return &HashRing{
		nodes:     make(map[uint32]*storage.Node),
		nodeIndex: make(map[string]*storage.Node),
	}
}

func hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// AddNode adds a new node to the consistent hashing ring.
func (r *HashRing) AddNode(node *storage.Node) {
	h := hash(node.ID)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[h] = node
	r.nodeIndex[node.ID] = node
	r.keys = append(r.keys, h)

	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
}

// GetNode routes a key to its primary node on the ring.
func (r *HashRing) GetNode(key string) *storage.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return nil
	}
	h := hash(key)

	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= h
	})

	if idx == len(r.keys) {
		idx = 0
	}
	return r.nodes[r.keys[idx]]
}

// GetNodes routes a key to a specific number of replicas on the ring.
func (r *HashRing) GetNodes(key string, count int) []*storage.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return nil
	}

	h := hash(key)

	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= h
	})
	if idx == len(r.keys) {
		idx = 0
	}

	if count > len(r.keys) {
		count = len(r.keys)
	}

	nodes := make([]*storage.Node, 0, count)
	for i := 0; i < count; i++ {
		pos := (idx + i) % len(r.keys)
		nodes = append(nodes, r.nodes[r.keys[pos]])
	}
	return nodes
}

// AllNodes returns a list of all nodes currently registered on the ring in ring-order.
func (r *HashRing) AllNodes() []*storage.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return nil
	}
	nodes := make([]*storage.Node, 0, len(r.keys))
	for _, h := range r.keys {
		nodes = append(nodes, r.nodes[h])
	}
	return nodes
}

// GetNodeByID retrieves a node by its unique ID.
func (r *HashRing) GetNodeByID(id string) *storage.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.nodeIndex[id]
}

// RemoveNode marks a node dead and removes it from the hash ring.
func (r *HashRing) RemoveNode(node *storage.Node) {
	node.Kill()

	r.mu.Lock()
	defer r.mu.Unlock()

	h := hash(node.ID)
	if _, ok := r.nodes[h]; !ok {
		return
	}

	delete(r.nodes, h)
	delete(r.nodeIndex, node.ID)

	for i, key := range r.keys {
		if key == h {
			r.keys = append(r.keys[:i], r.keys[i+1:]...)
			break
		}
	}
}
