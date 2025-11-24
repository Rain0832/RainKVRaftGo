# RainKVRaftGo

A production-style, educational distributed key-value storage system built upon a fully functional **Raft consensus implementation**, following the MIT **6.5840 (formerly 6.824)** Spring 2025 lab architecture.
This repository contains a clean, well-structured, and interview-ready implementation of:

* **Raft consensus protocol**
* **Fault-tolerant key/value service (KV-Raft)**
* **Client-side linearizable operations**
* **At-least-once & duplicate-suppression semantics**
* **Snapshotting for log compaction**
* **Shard Controller & Sharded KV (ShardKV)**
* **Dynamic shard reconfiguration & migration**

It can serve as a reference implementation, a learning project, or a baseline for building more advanced distributed systems.

---

## ✨ Features

### ✔ Fully Functional Raft Implementation

* Leader election
* Log replication & commitment
* Log compaction with snapshots
* Persistent state across crashes
* Correct handling of stale term, RPC races, network partitions
* Graceful shutdown (`Kill()`) with condition-variable broadcast

### ✔ Fault-Tolerant Key/Value Store (KV-Raft)

* Linearizable **Get/Put/Append** operations
* Duplicate RPC elimination (per-client request tracking)
* Apply channel with graceful exit
* Efficient snapshot installation

### ✔ Shard Controller (ShardCtrler)

* Manages shard-to-group assignments
* Dynamic rebalancing (Join/Leave/Move)
* Supports concurrent reconfigurations (Config Numbers)

### ✔ Sharded KV (ShardKV)

* Per-shard state machine
* Shard freezing, pulling, and installation
* Correct cross-group coordination
* End-to-end correctness under failures

---

## 📂 Repository Structure

```
RainKVRaftGo/
├── raft/               # Raft consensus module
│   ├── raft.go
│   ├── persistence.go
│   ├── log.go
│   ├── apply.go
│   └── ...
│
├── kvraft/             # Key/Value server on top of Raft
│   ├── server.go
│   ├── client.go
│   ├── ops.go
│   ├── snapshot.go
│   └── ...
│
├── shardctrler/        # Shard controller (central reconfig coordinator)
│   ├── server.go
│   ├── client.go
│   ├── config.go
│   └── ...
│
├── shardkv/            # Sharded KV servers
│   ├── shard.go
│   ├── migration.go
│   ├── ops.go
│   ├── server.go
│   └── ...
│
└── labrpc/             # RPC simulation layer used by 6.5840 labs
```

---

## 🚀 How to Run

### Run all tests (Raft + KV + ShardKV)

```
go test ./... -v
```

### Run an individual test

```
go test -run TestSnapshotInstall2E ./raft
go test -run TestBasic4B ./kvraft
go test -run TestDynamicConfig ./shardkv
```

---

## 🧠 What You Will Learn

Studying this repository allows you to deeply understand:

### **Distributed Systems Concepts**

* Consensus algorithms & their correctness
* Fault tolerance & replication
* Leader-based state machine replication
* Deterministic state machines
* Sharding & reconfiguration
* Dealing with partitions, retries, races

### **Systems Engineering Skills**

* Designing modular distributed components
* Implementing persistent protocol state
* Handling concurrency (locks, cond vars, atomic types)
* Snapshotting, serialization
* Writing reproducible correctness tests

### **Practical Engineering Knowledge**

* How etcd, TiKV, CockroachDB, YugabyteDB use Raft
* Why linearizability matters in real systems
* Interview-ready distributed system patterns

---

## 📘 Background & Motivation

This project is built after completing **MIT 6.5840 Lab 1–5** in full detail.
The goal is to transform lab assignments into:

* A production-style **reference implementation**
* A **learning project** for new students
* A **portfolio-ready distributed system**
* A basis for deeper research or extensions (e.g., Multi-Raft, Raft pipelining)

---

## 🔧 Dependencies

* Go 1.21+
* Standard library only (no external dependencies)
* 6.5840 RPC simulator (`labrpc`)

---

## 📄 License

MIT License. Free to use for study, research, or building your own distributed systems.

---

## 📣 Acknowledgements

This project is based on:

* MIT 6.5840 (previously 6.824) labs and code structure
* Raft: *In Search of an Understandable Consensus Algorithm*
* Lessons learned through completing the full set of labs