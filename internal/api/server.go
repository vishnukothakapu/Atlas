package api

import (
	"net/http"

	"github.com/vishnukothakapu/atlas/internal/cluster"
)

func Setup(
	ring *cluster.HashRing,
	rep *cluster.Replicator,
	router *cluster.Router,
	recovery *cluster.RecoveryManager,
) *http.ServeMux {
	mux := http.NewServeMux()

	// Cache – write goes through Replicator, read through Router.
	mux.HandleFunc("POST /cache", SetHandler(ring, rep))
	mux.HandleFunc("GET /cache/{key}", GetHandler(router))
	mux.HandleFunc("DELETE /cache/{key}", DeleteHandler(ring, rep))

	// Node lifecycle
	mux.HandleFunc("/kill/", KillHandler(ring))
	mux.HandleFunc("/revive/", ReviveHandler(ring, recovery))

	// Observability
	mux.HandleFunc("GET /debug", DebugHandler(ring))
	mux.HandleFunc("GET /cluster", ClusterHandler(ring))

	return mux
}
