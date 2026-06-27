package cluster

import (
	"log"
)

type Router struct {
	Failover *FailoverManager
}

func NewRouter(failover *FailoverManager) *Router {
	return &Router{Failover: failover}
}

func (rt *Router) Get(key string) ([]byte, string, bool) {
	primary := rt.Failover.GetPrimary(key)
	if primary == nil {
		log.Printf("[router] No alive node available for Get(%q)", key)
		return nil, "", false
	}

	val, ok := primary.Get(key)
	if !ok {
		return nil, primary.ID, false
	}

	log.Printf("[router] Get(%q) served by %s", key, primary.ID)
	return val, primary.ID, true
}
