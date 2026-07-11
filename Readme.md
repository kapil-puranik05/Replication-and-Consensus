# Raft Consensus Implementation

A minimal, production-style implementation of the Raft consensus algorithm built from scratch in Java. This project demonstrates the core mechanics of distributed consensus—including leader election, log replication, and fault tolerance—paired with a custom middleware gateway for request routing.

---

## Architecture Overview

The system consists of a self-managing Raft cluster and a middleware gateway that decouples clients from cluster topology.

```
                    Client
                       │
             Middleware Gateway
                       │
          (Routes requests to Leader)
                       │
          ┌────────────┼────────────┐
        Node 1       Node 2       Node 3
     (Follower)     (Leader)    (Follower)
```

Each node maintains the standard Raft state machine variants:
*   **Persistent State:** `currentTerm`, `votedFor`, and the `replicated log` (flushed to disk).
*   **Volatile State:** `role` (Leader/Follower/Candidate), `leaderId`, and `commitIndex`.
*   **Leader-Specific State:** `nextIndex[]` and `matchIndex[]` tracking peer progress.

---

## Core Features

### 1. Raft Consensus Engine
*   **Leader Election:** Automated elections driven by randomized election timeouts to minimize split votes.
*   **Log Replication:** Reliable state synchronization across peers using AppendEntries RPCs.
*   **Quorum-Based Commits:** Entries are marked committed only after safe replication to a majority (> N/2) of the cluster.
*   **Failover & Recovery:** Automatic leader reelection upon failure, and disk-based state recovery for restarting nodes.

### 2. Middleware Gateway
*   **Leader Discovery:** Dynamically queries the cluster at startup to locate the active leader.
*   **Smart Caching:** Caches the current leader's address to optimize subsequent client requests.
*   **Transparent Routing:** Automatically forwards client write operations to the active leader.
*   **Failover Handling:** Intercepts routing failures, triggers immediate leader re-discovery, and retries dropped requests transparently.

---

## Technical Stack

*   **Language:** Java
*   **Framework:** Spring Boot & Spring WebClient (REST-based RPC communication)
*   **Containerization:** Docker & Docker Compose
*   **Build Tool:** Maven

---

## Configuration & Setup

### 1. Directory Structure
Create the required configuration topology inside the project's root folder (`/raft`):

```text
raft/
├── configs/
│   ├── node1/
│   │   └── node.json
│   ├── node2/
│   │   └── node.json
│   └── node3/
│       └── node.json
```

### 2. Configuration Format (`node.json`)
Every node requires a local JSON configuration outlining its network profile, peer cluster, and the entry gateway.

#### Docker Compose Profile
```json
{
  "node": "node1:8080",
  "peers": ["node2:8081", "node3:8082"],
  "gatewayAddress": "gateway:9000"
}
```

#### Local Native Profile
```json
{
  "node": "localhost:8080",
  "peers": ["localhost:8081", "localhost:8082"],
  "gatewayAddress": "localhost:9000"
}
```

### 3. Launching the Cluster

Run your preferred deployment mode from the project root:

**Using Docker Compose:**
```bash
docker compose up --build
```

**Using Local Bash Script (Linux/macOS):**
```bash
source start.sh
```

---

## Current Scope & Limitations

This project targets the foundational protocols of the Raft whitepaper. The following optimization and production-hardening features are omitted by design:

*   **Log Management:** No log compaction or snapshotting mechanisms.
*   **Cluster Dynamics:** Fixed cluster membership (no dynamic additions or removals).
*   **Advanced Networking:** Basic network partition recoveries are implemented, but complex network split-brain edges are unoptimized.
*   **Security:** Transport layer lacks TLS authentication or payload encryption.

---

## Purpose & Insights

This system was developed to bridge the gap between distributed systems theory and implementation realities. Building the engine from the ground up highlighted the subtle challenges of concurrency, the necessity of rigorous state persistence, and the elegance of using a gateway middleware layer to simplify client interactions with shifting cluster topologies.