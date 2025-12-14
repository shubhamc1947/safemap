# 🔍 Design Decisions & Tradeoffs

This project intentionally favors **simplicity, correctness, and performance predictability** over feature completeness. The following sections document key design choices and their associated tradeoffs.

---

## Sharding Strategy

### **Current Design**

* The key-value store uses a **fixed number of shards** (buckets) determined at startup.
* Each shard contains:

  * A native Go `map`
  * A per-shard `sync.RWMutex`
* Keys are assigned to shards using:

```
shard = fnv1a_hash(key) % numShards
```

### **Why Fixed Shard Count?**

* Avoids global coordination or stop-the-world rehashing
* Guarantees stable routing of keys to shards
* Simplifies correctness and concurrency reasoning
* Matches designs used in many high-performance in-memory systems

### **Tradeoffs**

* Shard count cannot adapt dynamically to workload changes
* Highly skewed key distributions may overload individual shards

### **Future Improvement (Planned)**

* **Shard-level internal resizing** (rehashing the underlying map within a shard)
* Adaptive shard splitting for extremely hot shards (advanced)

---

## Concurrency Model

### **Current Design**

* Each shard is protected by a `sync.RWMutex`
* Operations on different shards execute concurrently
* Operations on the same shard are serialized for writes

### **Guarantees**

* Atomicity per key
* Thread-safe reads and writes
* No global locks
* No cross-shard ordering guarantees

### **Tradeoffs**

* No linearizability across multiple keys
* Range iteration does not provide a consistent snapshot
* Concurrent writes may interleave between shards during iteration

---

## Hash Function Selection

### **Current Design**

* Uses **FNV-1a (64-bit)** hashing for string keys

### **Why FNV-1a?**

* Fast, non-cryptographic
* Good uniform distribution for typical string workloads
* Minimal CPU overhead

### **Tradeoffs**

* Not cryptographically secure
* Vulnerable to adversarial key selection
* Unsuitable for untrusted public inputs in hostile environments

---

## TTL Expiration Strategy

### **Current Design**

* **Lazy expiration** on read (`GET`)
* **Periodic background scanning** to remove expired keys

### **Why Not a Priority Queue / Timer Heap?**

* A global heap introduces contention
* Timer-based systems are complex under heavy concurrency
* Periodic scans provide predictable overhead

### **Tradeoffs**

* Expired keys may remain in memory briefly until scanned
* Cleanup latency depends on scan interval
* Slight CPU overhead proportional to key count

---

## Rate Limiting

### **Current Design**

* Per-IP fixed-window counter
* Implemented in-memory

### **Tradeoffs**

* Memory grows with number of unique client IPs
* Fixed-window approach allows bursts at window boundaries
* Not suitable for distributed environments

### **Future Improvement (Planned)**

* Sliding window or token bucket algorithm
* TTL-based eviction of inactive clients
* Distributed rate limiting using Redis or similar

---

## Authentication

### **Current Design**

* Static API key authentication via HTTP headers
* Implemented as middleware

### **Tradeoffs**

* Not suitable for fine-grained authorization
* No token expiration or rotation
* No identity context

### **Future Improvement (Planned)**

* JWT-based authentication
* OAuth2 / OpenID Connect integration
* Per-key or per-namespace authorization

---

## Metrics & Observability

### **Current Design**

* Atomic counters using `sync/atomic`
* Exposed via JSON endpoint

### **Tradeoffs**

* No histograms or percentiles
* No long-term aggregation
* JSON parsing overhead

### **Future Improvement (Planned)**

* Prometheus-compatible `/metrics`
* Latency histograms
* Per-shard contention metrics

---

## Memory Management

### **Current Design**

* Values stored as raw byte slices
* No compression or pooling

### **Tradeoffs**

* Higher memory usage for large values
* Increased GC pressure under heavy churn

### **Future Improvement (Planned)**

* Optional compression (Snappy, Zstd)
* Object pooling using `sync.Pool`
* Size-based eviction strategies

---

## Persistence & Durability

### **Current Design**

* Pure in-memory store
* No durability guarantees

### **Tradeoffs**

* Data loss on process restart
* Unsuitable for primary storage

### **Future Improvement (Planned)**

* Write-Ahead Logging (WAL)
* Snapshotting
* Pluggable persistence backends

---

## Distributed Operation

### **Current Design**

* Single-node deployment
* No cluster awareness

### **Tradeoffs**

* Horizontal scaling requires external sharding
* No fault tolerance

### **Future Improvement (Planned)**

* Gossip-based cluster membership
* Consistent hashing
* Replicated shards with quorum reads/writes

---

## Summary of Non-Goals

This project intentionally does **not** aim to:

* Replace Redis or Memcached
* Provide strict multi-key transactional guarantees
* Offer cryptographic security primitives
* Act as a durable primary database

Instead, it focuses on:

* Clear concurrency semantics
* Predictable performance
* Extensible system design
* Educational value for systems programming

---

## Final Note

This design documentation exists to make **tradeoffs explicit** and **engineering intent clear**.
Every limitation is either:

* A deliberate simplification, or
* A documented future extension

