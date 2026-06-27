package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/vishnukothakapu/atlas/internal/cluster"
)

type nodeDebugInfo struct {
	ID            string            `json:"id"`
	Alive         bool              `json:"alive"`
	Keys          int               `json:"keys"`
	HeartbeatAge  string            `json:"heartbeat_age"`
	LastHeartbeat string            `json:"last_heartbeat"`
	LastSequence  uint64            `json:"last_sequence"`
	Data          map[string]string `json:"data"`
}

type clusterNodeInfo struct {
	ID    string `json:"id"`
	Alive bool   `json:"alive"`
}

// DebugHandler retrieves detailed telemetry, key-value mappings, sequence IDs, and heartbeat timings from all nodes.
func DebugHandler(ring *cluster.HashRing) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]nodeDebugInfo)

		for _, node := range ring.AllNodes() {
			node.Mu.RLock()
			hb := node.LastHeartbeat
			alive := node.Alive
			lastSeq := node.LastSequence

			copyData := make(map[string]string, len(node.Data))
			for k, v := range node.Data {
				copyData[k] = string(v)
			}
			node.Mu.RUnlock()

			age := time.Since(hb)
			result[node.ID] = nodeDebugInfo{
				ID:            node.ID,
				Alive:         alive,
				Keys:          len(copyData),
				HeartbeatAge:  fmt.Sprintf("%.1fs", age.Seconds()),
				LastHeartbeat: hb.Format("2006-01-02 15:04:05"),
				LastSequence:  lastSeq,
				Data:          copyData,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// ClusterHandler returns a simplified health status summary list for all nodes.
func ClusterHandler(ring *cluster.HashRing) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodes := ring.AllNodes()
		result := make([]clusterNodeInfo, 0, len(nodes))

		for _, node := range nodes {
			node.Mu.RLock()
			info := clusterNodeInfo{ID: node.ID, Alive: node.Alive}
			node.Mu.RUnlock()
			result = append(result, info)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}
