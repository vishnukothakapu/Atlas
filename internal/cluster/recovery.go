package cluster

import (
	"github.com/vishnukothakapu/atlas/internal/storage"
)

type RecoveryManager struct {
	Ring       *HashRing
	LogManager storage.LogStore
}

func NewRecoveryManager(ring *HashRing, logManager storage.LogStore) *RecoveryManager {
	return &RecoveryManager{
		Ring:       ring,
		LogManager: logManager,
	}
}

// RecoverNode replays operations to target from its last known sequence.
func (r *RecoveryManager) RecoverNode(target *storage.Node) {
	operations := r.LogManager.GetAfter(target.LastSequence)
	if len(operations) > 0 {
		target.ApplyRecoveryBatch(operations)
	}
}

// Apply replays a single log operation onto a node.
func (r *RecoveryManager) Apply(node *storage.Node, op storage.Operation) {
	node.ApplyRecovery(op)
}
