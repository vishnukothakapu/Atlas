package cluster

import (
	"log"

	"github.com/vishnukothakapu/atlas/internal/storage"
)

type FailoverManager struct {
	Ring *HashRing
}

func NewFailoverManager(ring *HashRing) *FailoverManager {
	return &FailoverManager{
		Ring: ring,
	}
}

// GetPrimary returns the first alive node in the replica set for a given key.
func (f *FailoverManager) GetPrimary(key string) *storage.Node {
	nodes := f.Ring.GetNodes(key, 3)

	for _, node := range nodes {
		node.Mu.RLock()
		alive := node.Alive
		node.Mu.RUnlock()

		if alive {
			return node
		}
	}
	return nil
}

// Promote searches the replica set and logs a new leader node promotion for a key.
func (f *FailoverManager) Promote(key string) *storage.Node {
	nodes := f.Ring.GetNodes(key, 3)

	for _, node := range nodes {
		node.Mu.RLock()
		alive := node.Alive
		node.Mu.RUnlock()

		if alive {
			log.Printf("[failover] %s promoted as new owner for key %q", node.ID, key)
			return node
		}
	}

	log.Printf("[failover] No alive node available to promote for key %q", key)
	return nil
}
