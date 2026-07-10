# Raft Consensus Implementation

## Overview

This project is a minimal implementation of the Raft consensus algorithm built from scratch in Java. The objective was to understand how distributed consensus works by implementing the protocol rather than only studying its theory.

The project consists of a Raft cluster responsible for leader election and log replication, along with a middleware gateway that routes client requests to the current leader.

---

## Features

### Raft Consensus

- Leader election using randomized election timeouts
- Vote request and response protocol
- Log replication using AppendEntries RPC
- Majority quorum-based commit mechanism
- Persistent state recovery after node failures
- Automatic leader failover

### Middleware Gateway

- Receives all client requests
- Discovers the cluster leader during startup
- Caches the current leader for subsequent requests
- Automatically forwards write requests to the leader
- Re-discovers the leader if the cached leader becomes unavailable

### Persistence

Each node persists the following state to disk:

- Current Term
- Voted For
- Replicated Log

This allows nodes to recover their state after a restart.

---

## Architecture

```
                    Client
                       │
              Middleware Gateway
                       │
             Routes requests to leader
                       │
          ┌────────────┼────────────┐
        Node 1       Node 2       Node 3
```

Each node maintains:

### Persistent State

- currentTerm
- votedFor
- replicated log

### Volatile State

- role
- leaderId
- commitIndex
- nextIndex / matchIndex (leader only)

Communication between nodes is performed using REST APIs.

---

## How It Works

### Leader Election

- Followers start an election after a randomized timeout.
- Candidates request votes from all peers.
- A node becomes the leader after receiving votes from the majority of the cluster.
- Followers step down if they receive messages with a higher term.

### Log Replication

- The middleware forwards client write requests to the current leader.
- The leader appends the request to its log.
- Log entries are replicated to follower nodes.
- Entries are committed after acknowledgement from a majority of the cluster.
- Once committed, the entries are applied to the state machine on every node.

### Middleware

The middleware acts as the single entry point for client requests.

During startup, it discovers the current leader by contacting the available nodes. The leader information is cached to avoid unnecessary lookups for every request.

If the cached leader becomes unavailable, the middleware contacts the cluster again to determine the current leader and retries the request automatically.

### Fault Tolerance

- Persistent state survives node failures.
- Restarted nodes recover their previous state from disk.
- Leader failures trigger automatic re-election.

---

## Running

1. Configure the nodes using the respective `node.json` files.
2. Start the cluster using Docker Compose.

```bash
docker compose up --build
```

The compose file starts:

- Three Raft nodes
- Middleware Gateway

Client requests should be sent to the middleware instead of individual nodes.

---

## Current Limitations

This project focuses on implementing the core Raft protocol.

The following features are currently not implemented:

- Snapshotting
- Log compaction
- Dynamic cluster membership
- Network partition handling
- Authentication and security

---

## Technologies Used

- Java
- Spring Boot
- Spring WebClient
- Docker
- Docker Compose
- Maven

---

## Why I Built This

The purpose of this project was to gain a practical understanding of distributed systems and consensus algorithms by implementing Raft from scratch.

Building the protocol helped me understand leader election, quorum-based replication, fault tolerance, and how middleware can simplify client interaction with a distributed system.