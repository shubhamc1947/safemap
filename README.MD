# Go Concurrent Key-Value Store

![License](https://img.shields.io/badge/License-MIT-green)

A high-performance, thread-safe, in-memory Key-Value store written in Go, featuring:

* **Sharded concurrency** using per-bucket RWMutexes
* **Low-latency HTTP JSON API**
* **TTL (Time-To-Live) support**
* **IP-based Rate Limiting**
* **Optional API Key Authentication**
* **Real-time atomic metrics**
* **Built-in load benchmark tool**

---

## 🚀 Features

### **High-Performance Concurrency**

* Internally uses a **sharded map** (`ConcurrentMap`)
* Each shard has its own `sync.RWMutex`
* Keys distributed using **FNV-1a hash**
* Greatly reduces lock contention under heavy parallel load

### **TTL Expiration**

* Passive (lazy) TTL check on GET
* Background expiry worker removes expired keys periodically

### **Authentication (Optional)**

If `--auth-token` is provided:

Accepted headers:

```
X-API-Key: <token>
Authorization: <token>
Authorization: Bearer <token>
```

Routes excluded from auth:

* `/healthz`

### **Rate Limiting**

* Simple per-IP counter
* Configurable using flags
* Excludes `/healthz`

### **Metrics**

Exposed at `/metrics` (JSON):

* total_requests
* total_gets
* total_puts
* total_deletes
* unauthorized
* rate_limited
* not_found

### **Structured Logging**

Every request logs method, path, status code, and duration.

---

## Architecture

### **Sharded Map**

```
ConcurrentMap
├── buckets[0] → { RWMutex, map }
├── buckets[1] → { RWMutex, map }
├── ...
└── buckets[N]
```

Bucket selection:

```
Bucket = fnv1a_hash(key) % numBuckets
```

Advantages:

* Reads on different keys do **not** block each other
* Writes on different buckets do **not** interfere
* Scales extremely well on multi-core CPUs

---

## 📂 Project Structure

```
.
├── cmd/
│   ├── kv-server/           # Main Key-Value HTTP server
│   │   └── main.go
│   └── benchmark/           # Load testing / RPS tool
│       └── main.go
├── pkg/
│   └── concurrentmap/       # Sharded map implementation
│       ├── concurrent_map.go
│       ├── counter_map.go
│       ├── range.go
│       └── atomic_ops.go
└── go.mod
```

---

## 🛠️ Running the Server

### **Default run (port 8080, 64 buckets)**

```bash
go run ./cmd/kv-server
```

### **Run with Authentication**

```bash
go run ./cmd/kv-server --auth-token=mySecret123
```

### **Run with Rate Limiting**

```bash
go run ./cmd/kv-server --rate-limit=100 --rate-window=1m
```

### **All flags**

| Flag                  | Description             | Default        |
| --------------------- | ----------------------- | -------------- |
| `--port`              | HTTP port               | `8080`         |
| `--buckets`           | Number of shards        | `64`           |
| `--auth-token`        | API Key (optional)      | `""`           |
| `--rate-limit`        | Max requests per window | `0` (disabled) |
| `--rate-window`       | Rate window duration    | `1m`           |
| `--ttl-scan-interval` | Cleanup frequency       | `5s`           |

---

## 📡 API Usage

### **PUT /kv/{key}**

```bash
curl -X PUT http://localhost:8080/kv/user123 \
  -H "Content-Type: application/json" \
  -H "X-API-Key: mySecret123" \
  -d '{"value": "Hello", "ttl_seconds": 30}'
```

Response:

```json
{
  "value": "Hello",
  "expires_at": "2025-01-30T14:03:22Z"
}
```

---

### **GET /kv/{key}**

```bash
curl -H "X-API-Key: mySecret123" \
  http://localhost:8080/kv/user123
```

---

### **DELETE /kv/{key}**

```bash
curl -X DELETE -H "X-API-Key: mySecret123" \
  http://localhost:8080/kv/user123
```

---

### **Metrics**

```bash
curl -H "X-API-Key: mySecret123" http://localhost:8080/metrics
```

### **Health**

```bash
curl http://localhost:8080/healthz
```

---

## 📊 Benchmarking

This project ships with a built-in benchmark tool:

### **Start server (no rate limit)**

```bash
go run ./cmd/kv-server --rate-limit=0 --buckets=100
```

### **Run benchmark**

```bash
go run ./cmd/benchmark --n=200000 --c=100 --url="http://localhost:8080/kv/"
```

### Sample Output

```
--- 📊 Benchmark Results ---
Time Taken:       46.43s
Total Operations: 200000
Successful:       200000
Failed:           0
-------------------------------
🚀 Requests/Sec:  4306.64
-------------------------------
```

This includes:

* JSON parsing
* PUT lock (write)
* GET lock (read)
* TTL structure
* Logging
* Metrics updates

Very strong performance for a pure Go key-value store.

---

# Future Plans & Roadmap

The current version focuses on high-performance in-memory operations with strong concurrency guarantees. The next milestones aim to expand the KV store into a more durable, distributed, and feature-rich system.

### **1. Data Persistence Layer (Planned)**

A durable persistence engine is under active consideration. Possible approaches:

* **Append-Only Write-Ahead Log (WAL)**
  Each write is appended to a durable log file on disk.
  On startup, the store replays the WAL to rebuild memory state.

* **Snapshotting Mechanism**
  Periodic full dumps of the in-memory store to reduce WAL replay time.

* **Pluggable Persistence Backends**
  Supporting:

  * Local disk (default)
  * S3 / GCS object storage
  * Embedded SQLite or BadgerDB for high-throughput local durability

The goal is to keep persistence **optional**, keeping the KV server a drop-in in-memory cache when durability isn’t needed.

---

### **2. Distributed Mode (Cluster Support)**

Exploring the introduction of a multi-node mode:

* **Gossip-based cluster membership**
* **Consistent hashing for key distribution**
* **Replicated shards for fault tolerance**
* **Leaderless quorum reads/writes (inspired by Dynamo)**

This would allow the KV server to behave like a lightweight distributed cache.

---

### **3. Pub/Sub on Key Updates**

A lightweight mechanism where clients can subscribe to events:

* `SET`
* `DELETE`
* `EXPIRE`

Transport candidates:

* Server-Sent Events (SSE)
* WebSockets
* HTTP streams

Useful for real-time dashboards or reactive microservices.

---

### **4. Extended Metrics / Prometheus Exporter**

A Prometheus-native `/metrics` endpoint with:

* Histogram buckets (latency, RPS)
* Saturation metrics per shard
* Live shard lock contention stats
* Rate limiter drop graphs

---

### **5. Pluggable Middleware Pipeline**

Turning middlewares into first-class, user-provided components:

* Custom authentication modules
* Custom rate limiters
* Request rewriting
* Tracing providers (OpenTelemetry)

---

### **6. Optional Compression for Stored Values**

Future plan includes supporting:

* Snappy
* Zstd
* Gzip (only for large payloads)

Automatically enabled based on value size.

---

### **7. CLI Client + Go SDK**

A CLI tool for interacting with the server:

```
kvctl set key value
kvctl get key
kvctl watch key
kvctl metrics
```

And a Go SDK:

```go
client.Set("user123", "hello", kv.WithTTL(time.Minute))
```

---

### **8. Web UI Dashboard**

A small React-based dashboard for:

* Key browsing
* Metrics visualization
* Hot shards and lock contention
* TTL visualization
* Rate limit insights

---

### **9. Binary Release & Docker Image**

Planned distribution formats:

* Precompiled binaries for Linux, macOS, Windows
* Minimal Alpine Docker image
* Helm chart for Kubernetes deployment

---

## 📄 License

MIT

---

