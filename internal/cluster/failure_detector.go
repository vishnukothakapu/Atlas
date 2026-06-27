package cluster

import (
	"log"
	"sync"
	"time"
)

type FailureDetector struct {
	Ring     *HashRing
	Failover *FailoverManager
	Timeout  time.Duration

	mu   sync.Mutex
	stop chan struct{}
}

func NewFailureDetector(ring *HashRing, failover *FailoverManager) *FailureDetector {
	return &FailureDetector{
		Ring:     ring,
		Failover: failover,
		Timeout:  5 * time.Second,
	}
}

// Start launches the background failure detection loop.
func (f *FailureDetector) Start() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stop != nil {
		return
	}
	f.stop = make(chan struct{})

	ticker := time.NewTicker(time.Second)
	stopChan := f.stop

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				f.CheckNodes()
			case <-stopChan:
				return
			}
		}
	}()
}

// Stop shuts down the background failure detection loop.
func (f *FailureDetector) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stop != nil {
		close(f.stop)
		f.stop = nil
	}
}

func (f *FailureDetector) CheckNodes() {
	for _, node := range f.Ring.AllNodes() {
		node.Mu.Lock()

		timedOut := time.Since(node.LastHeartbeat) > f.Timeout

		if timedOut && node.Alive {
			node.Alive = false
			log.Printf("[detector] Node %s marked DEAD (no heartbeat for >%s)", node.ID, f.Timeout)

			keys := make([]string, 0, len(node.Data))
			for k := range node.Data {
				keys = append(keys, k)
			}
			node.Mu.Unlock()

			for _, key := range keys {
				promoted := f.Failover.Promote(key)
				if promoted != nil {
					log.Printf("[detector] Failover: %s → %s for key %q",
						node.ID, promoted.ID, key)
				}
			}
			continue
		}

		node.Mu.Unlock()
	}
}
