package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vishnukothakapu/atlas/internal/cluster"
)

// KillHandler disables heartbeats and shuts down a specific node to simulate a network partition/crash.
func KillHandler(ring *cluster.HashRing) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/kill/")
		if id == "" {
			http.Error(w, "missing node id", http.StatusBadRequest)
			return
		}
		node := ring.GetNodeByID(id)
		if node == nil {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		node.Kill()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	}
}

// ReviveHandler triggers offline batch log recovery and brings a dead node back online.
func ReviveHandler(ring *cluster.HashRing, recovery *cluster.RecoveryManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/revive/")
		if id == "" {
			http.Error(w, "missing node id", http.StatusBadRequest)
			return
		}
		node := ring.GetNodeByID(id)
		if node == nil {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}

		recovery.RecoverNode(node)
		node.Revive()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "revived", "node": id})
	}
}
