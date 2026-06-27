# Atlas Distributed Key-Value Store

Atlas is a lightweight, distributed, in-memory key-value store designed to demonstrate core concepts in distributed systems. It features consistent hashing, partition replication, heartbeat-based failure detection, automatic client read failovers, and offline batch log recovery.

---



## Core Features

1. **Consistent Hashing & Dynamic Ring Routing**: Distributes keys evenly across active nodes using CRC32 checksum rings.
2. **Replication Group Consistency**: Automatically replicates Set and Delete mutations to $N=3$ logical nodes in key-hash order, maintaining a monotonic sequence ID per operation.
3. **Heartbeat Failure Detection**: Uses background detectors to continuously verify node health. If a heartbeat expires (default timeout 5s), the node is marked DEAD and replica failover is triggered.
4. **Transparent Failover Routing**: Client GET requests are automatically rerouted to the next available healthy replica on the hash ring if the primary owner node dies.
5. **Incremental Offline Log Recovery**: Reconstructs state on a revived node by replaying *only* the missed operations (via sequence tracking `GetAfter(LastSequence)`) in a single batch while offline, avoiding costly full-database transfers before reviving heartbeats.

---

## Project Structure

```text
atlas/
│
├── cmd/
│   └── node/
│       └── main.go                 # Application server entry point
│
├── internal/
│   ├── api/                        # HTTP endpoints, routing, and debug controllers
│   │   ├── debug.go
│   │   ├── handlers.go
│   │   ├── node.go
│   │   └── server.go
│   │
│   ├── cluster/                    # Consistent hashing, routing, and membership
│   │   ├── failover.go
│   │   ├── failure_detector.go
│   │   ├── recovery.go
│   │   ├── replicator.go
│   │   └── ring.go
│   │
│   └── storage/                    # Low-level memory mapping & operation logging
│       ├── node.go
│       └── oplog.go
│
├── postman.json                    # Exported Postman collection for manual verification
├── README.md                       # This document
└── go.mod                          # Go module configuration
```

---

## Getting Started

### Prerequisites
- Go 1.22+ installed on your system.
- Postman (optional, for manual verification).

### Run the Server
Start the local cluster consisting of `node-A`, `node-B`, and `node-C` listening on port `8080`:
```bash
go run cmd/node/main.go
```

**Output:**
```text
[main] Added node node-A
[main] Added node node-B
[main] Added node node-C
[main] Atlas listening on :8080
```

---

## Example API Usage (End-to-End Walkthrough)

Here is a step-by-step walkthrough to test consistent hashing, replica routing, failover, and incremental log recovery using `curl`. You can also import the pre-configured Postman Collection file `postman.json` at the root of the project to test these routes.

### Step 1: Check Initial Cluster Status
Check that all three nodes are healthy and running:
```bash
curl -X GET http://localhost:8080/cluster
```
**Response:**
```json
[
  { "id": "node-A", "alive": true },
  { "id": "node-B", "alive": true },
  { "id": "node-C", "alive": true }
]
```

### Step 2: Store Initial Data
Write a key to the cluster. The hash ring will map this write to a primary node and replicate it to backup nodes:
```bash
curl -X POST http://localhost:8080/cache \
  -H "Content-Type: application/json" \
  -d '{"key": "user_1", "value": "premium"}'
```
**Response:** (Indicates which node has the primary mapping)
```json
{
  "stored_on": "node-B"
}
```

Write a second key to advance the operation sequence:
```bash
curl -X POST http://localhost:8080/cache \
  -H "Content-Type: application/json" \
  -d '{"key": "user_2", "value": "standard"}'
```

### Step 3: Simulate Node Outage
Kill the node that was returned as `stored_on` in Step 2 (e.g., `node-B`):
```bash
curl -X POST http://localhost:8080/kill/node-B
```
**Response:** `204 No Content`

### Step 4: Write Mutations and Check Failover
Perform a new write operation while `node-B` is dead:
```bash
curl -X POST http://localhost:8080/cache \
  -H "Content-Type: application/json" \
  -d '{"key": "user_3", "value": "trial"}'
```

Reading `user_1` (whose primary owner `node-B` is dead) will transparently failover and serve data from a healthy replica on the hash ring:
```bash
curl -X GET http://localhost:8080/cache/user_1
```
**Response:** (Served from replica node-C)
```json
{
  "node": "node-C",
  "value": "premium"
}
```

### Step 5: Check Debug State (Before Recovery)
Query the detailed telemetry endpoint. You will observe that `node-B` is offline (`alive: false`), its `last_sequence` is stuck at `2`, and it is missing the `user_3` key:
```bash
curl -X GET http://localhost:8080/debug
```
**Response:**
```json
{
  "node-A": {
    "id": "node-A",
    "alive": true,
    "keys": 3,
    "last_sequence": 3,
    "data": { "user_1": "premium", "user_2": "standard", "user_3": "trial" }
  },
  "node-B": {
    "id": "node-B",
    "alive": false,
    "keys": 2,
    "last_sequence": 2,
    "data": { "user_1": "premium", "user_2": "standard" }
  },
  "node-C": {
    "id": "node-C",
    "alive": true,
    "keys": 3,
    "last_sequence": 3,
    "data": { "user_1": "premium", "user_2": "standard", "user_3": "trial" }
  }
}
```

### Step 6: Revive & Trigger Incremental Log Recovery
Trigger node recovery for `node-B`. The system will fetch the missed operations (sequences greater than `2`) from the log manager, batch-replay them to the node while it is offline, and then revive its heartbeats:
```bash
curl -X POST http://localhost:8080/revive/node-B
```
**Response:**
```json
{
  "status": "revived",
  "node": "node-B"
}
```

### Step 7: Verify Recovery Reconcile (After Recovery)
Query the debug endpoint again. Verify that `node-B` has caught up to sequence `3`, and the missing key `user_3` has been successfully restored to its local storage map:
```bash
curl -X GET http://localhost:8080/debug
```
**Response:** (Reconciled state)
```json
{
  "node-B": {
    "id": "node-B",
    "alive": true,
    "keys": 3,
    "last_sequence": 3,
    "data": { "user_1": "premium", "user_2": "standard", "user_3": "trial" }
  }
}
```

---
