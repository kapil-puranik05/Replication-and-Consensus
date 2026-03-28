# Raft Consensus Implementation

## Overview

This is a minimal implementation of the Raft consensus algorithm.

Main goal was to understand how distributed consensus actually works by building it from scratch. The system simulates multiple nodes coordinating to maintain a consistent replicated log.

---

## Features

* Leader election using randomized timeouts
* Vote request/response flow
* Log with term + index
* Persistent state (term, vote, log) using files
* Crash recovery
* HTTP based communication between nodes

---

## Architecture

Each node maintains:

**Persistent State**

* currentTerm
* votedFor
* log

**Volatile State**

* role 
* commitIndex
* leaderId

Nodes communicate via REST APIs.

---

## How It Works

### Leader Election

* Followers start election after timeout
* Candidate requests votes from peers
* Majority → becomes leader

### Log Replication

* Leader appends entries
* Replicates to followers
* Entries are committed after majority

### Fault Tolerance

* State is persisted to disk
* Nodes recover on restart

---

## Running

1. Configure nodes in `node.json` (ports + peers)
2. Start multiple instances
3. Check logs for election + replication

---

## Notes

* This is an MVP, not production-grade
* Focus is on core Raft logic
* No snapshotting / optimizations yet

---

## Why I Built This

Wanted to understand distributed systems properly instead of just reading theory. Implementing Raft helped in understanding elections, consistency, and failure handling much better.

---
