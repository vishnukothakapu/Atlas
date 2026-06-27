package api

import (
	"encoding/json"
	"net/http"

	"github.com/vishnukothakapu/atlas/internal/cluster"
)

type cacheSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SetHandler handles writing/replicating values to the cluster under a key.
func SetHandler(ring *cluster.HashRing, rep *cluster.Replicator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req cacheSetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, "invalid request body: expected {\"key\":\"...\",\"value\":\"...\"}", http.StatusBadRequest)
			return
		}

		rep.Set(req.Key, []byte(req.Value))

		node := ring.GetNode(req.Key)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"stored_on": node.ID,
		})
	}
}

// GetHandler retrieves a key's value from the cluster, supporting automatic failover routing.
func GetHandler(router *cluster.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		value, nodeID, ok := router.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"node":  nodeID,
			"value": string(value),
		})
	}
}

// DeleteHandler replicates key deletions across consistent hashing replicas.
func DeleteHandler(ring *cluster.HashRing, rep *cluster.Replicator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		rep.Delete(key)

		node := ring.GetNode(key)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"deleted_from": node.ID,
		})
	}
}
