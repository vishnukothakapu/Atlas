package storage

import (
	"log"
	"sync"
	"time"
)

type Node struct {
	ID               string
	Data             map[string][]byte
	Alive            bool
	LastHeartbeat    time.Time
	Mu               sync.RWMutex
	stopHeartbeat    chan struct{}
	heartbeatRunning bool
	LastSequence     uint64
}

func NewNode(id string) *Node {
	return &Node{
		ID:            id,
		Data:          make(map[string][]byte),
		Alive:         true,
		LastHeartbeat: time.Now(),
		stopHeartbeat: make(chan struct{}),
	}
}

func (n *Node) Set(key string, value []byte, seq uint64) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	if !n.Alive {
		return
	}
	n.Data[key] = value
	n.LastSequence = seq
}

func (n *Node) Delete(key string, seq uint64) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	if !n.Alive {
		return
	}
	delete(n.Data, key)
	n.LastSequence = seq
}

func (n *Node) Get(key string) ([]byte, bool) {
	n.Mu.RLock()
	defer n.Mu.RUnlock()

	if !n.Alive {
		return nil, false
	}
	val, ok := n.Data[key]
	return val, ok
}

func (n *Node) Heartbeat() {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	if !n.Alive {
		log.Printf("[node] Node %s recovered (heartbeat resumed)", n.ID)
		n.Alive = true
	}
	n.LastHeartbeat = time.Now()
}

func (n *Node) StartHeartbeat() {
	n.Mu.Lock()
	if n.heartbeatRunning {
		n.Mu.Unlock()
		return
	}
	n.heartbeatRunning = true
	stopChan := n.stopHeartbeat
	n.Mu.Unlock()

	ticker := time.NewTicker(2 * time.Second)

	go func() {
		defer func() {
			n.Mu.Lock()
			n.heartbeatRunning = false
			n.Mu.Unlock()
		}()

		for {
			select {
			case <-ticker.C:
				n.Heartbeat()
			case <-stopChan:
				ticker.Stop()
				return
			}
		}
	}()
}

func (n *Node) Kill() {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	n.Alive = false
	if n.stopHeartbeat != nil {
		close(n.stopHeartbeat)
		n.stopHeartbeat = nil
	}
}

func (n *Node) Revive() {
	n.Mu.Lock()

	if n.stopHeartbeat == nil {
		n.stopHeartbeat = make(chan struct{})
	}
	n.Alive = true
	n.LastHeartbeat = time.Now()
	n.Mu.Unlock()

	n.StartHeartbeat()
}

func (n *Node) ApplyRecovery(op Operation) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	switch op.Type {
	case SetOperation:
		n.Data[op.Key] = op.Value
	case DelOperation:
		delete(n.Data, op.Key)
	}
	n.LastSequence = op.Sequence
}

func (n *Node) ApplyRecoveryBatch(ops []Operation) {
	if len(ops) == 0 {
		return
	}
	n.Mu.Lock()
	defer n.Mu.Unlock()

	for _, op := range ops {
		switch op.Type {
		case SetOperation:
			n.Data[op.Key] = op.Value
		case DelOperation:
			delete(n.Data, op.Key)
		}
	}
	n.LastSequence = ops[len(ops)-1].Sequence
}
