package main

import (
	"log"
	"net/http"

	"github.com/vishnukothakapu/atlas/internal/api"
	"github.com/vishnukothakapu/atlas/internal/cluster"
	"github.com/vishnukothakapu/atlas/internal/storage"
)

func main() {
	ring := cluster.NewHashRing()
	for _, id := range []string{"node-A", "node-B", "node-C"} {
		n := storage.NewNode(id)
		n.StartHeartbeat()
		ring.AddNode(n)
		log.Printf("[main] Added node %s", id)
	}

	logManager := storage.NewLogManager()
	failover := cluster.NewFailoverManager(ring)
	rep := cluster.NewReplicator(ring, logManager)
	router := cluster.NewRouter(failover)
	detector := cluster.NewFailureDetector(ring, failover)
	detector.Start()

	recovery := cluster.NewRecoveryManager(ring, logManager)

	mux := api.Setup(ring, rep, router, recovery)
	log.Println("[main] Atlas listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
