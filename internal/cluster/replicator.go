package cluster

import (
	"log"

	"github.com/vishnukothakapu/atlas/internal/storage"
)

type Replicator struct {
	Ring       *HashRing
	LogManager storage.LogStore
}

func NewReplicator(ring *HashRing, logManager storage.LogStore) *Replicator {
	return &Replicator{Ring: ring, LogManager: logManager}
}

// Set replicates a write operation to nodes on the hash ring.
func (r *Replicator) Set(key string, value []byte) {
	seq := r.LogManager.Append(storage.SetOperation, key, value)
	nodes := r.Ring.GetNodes(key, 3)
	for _, node := range nodes {
		node.Mu.RLock()
		alive := node.Alive
		node.Mu.RUnlock()

		if !alive {
			log.Printf("[replicator] Skipping dead node %s on Set(%q)", node.ID, key)
			continue
		}

		node.Set(key, value, seq)
	}
}

// Delete replicates a delete operation to nodes on the hash ring.
func (r *Replicator) Delete(key string) {
	seq := r.LogManager.Append(storage.DelOperation, key, nil)
	nodes := r.Ring.GetNodes(key, 3)
	for _, node := range nodes {
		node.Mu.RLock()
		alive := node.Alive
		node.Mu.RUnlock()

		if !alive {
			log.Printf("[replicator] Skipping dead node %s on Delete(%q)", node.ID, key)
			continue
		}

		node.Delete(key, seq)
	}
}
