# epoch

A time series database that doesn't require a PhD to operate.

## Why This Exists?

Because InfluxDB's query language is stuck in 2015, TimescaleDB wants you to learn PostgreSQL first, and Prometheus thinks disk space is infinite.

epoch is what happens when you take the good parts of each and leave the baggage behind: Gorilla compression for actual storage efficiency, InfluxQL-style queries for humans, and clustering that doesn't require a Kubernetes certification.

## Features

- **Gorilla compression** - 10-55x compression ratios depending on your data (sequential timestamps love us)
- **Line protocol ingestion** - drop-in compatible with InfluxDB writes
- **InfluxQL queries** - SELECT, WHERE, GROUP BY, aggregates, the works
- **HTTP API** - POST your data, GET your answers
- **Retention policies** - automatic data lifecycle management
- **Continuous queries** - downsampling without cron jobs
- **Clustering** - horizontal scaling with consistent hashing and quorum writes
- **Anti-entropy** - Merkle tree-based repair for eventual consistency

## Quick Start

```bash
# Build
go build -o epoch ./cmd/epoch

# Start server
./epoch server --bind :8086 --data ./data

# Or jump into the shell
./epoch shell --host localhost:8086
```

## Writing Data

Line protocol, just like you're used to:

```bash
curl -X POST "http://localhost:8086/write?db=mydb" \
  --data-binary 'cpu,host=server01 value=0.64 1434055562000000000
cpu,host=server02 value=0.55 1434055562000000000'
```

Create the database first if it doesn't exist (or let auto-create handle it):

```bash
# Databases are auto-created on write, but you can be explicit
curl -X POST "http://localhost:8086/query" \
  --data-urlencode "q=CREATE DATABASE mydb"
```

## Querying Data

```bash
# Simple query
curl -G "http://localhost:8086/query" \
  --data-urlencode "db=mydb" \
  --data-urlencode "q=SELECT * FROM cpu WHERE time > now() - 1h"

# Aggregations
curl -G "http://localhost:8086/query" \
  --data-urlencode "db=mydb" \
  --data-urlencode "q=SELECT mean(value) FROM cpu GROUP BY time(5m), host"
```

## Interactive Shell

```
$ ./epoch shell
epoch shell v0.1.0
Connected to localhost:8086
Type 'help' for available commands, 'exit' to quit.

> USE mydb
Using database: mydb

[mydb]> SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)
name: cpu
time                 | mean
---------------------+-------
2024-01-15T12:00:00Z | 0.58
2024-01-15T12:10:00Z | 0.62
2024-01-15T12:20:00Z | 0.55

3 rows returned

[mydb]> FORMAT json
Format set to: json
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       HTTP API                              │
│  /write  /query  /ping  /debug/vars                        │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Query Engine                            │
│  Parser → Planner → Executor                                │
│  Aggregates: mean, sum, count, min, max, percentile, etc.  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Storage Engine                           │
│  Shards → Column Files → Gorilla Compression               │
│  Write-Ahead Log → Tag Index → Retention Policies          │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                       Cluster                               │
│  Coordinator → Replication → Anti-Entropy (Merkle Trees)   │
│  Consistency: ONE, QUORUM, ALL                              │
└─────────────────────────────────────────────────────────────┘
```

## Compression

epoch uses Facebook's Gorilla compression for both timestamps and values:

- **Timestamps**: Delta-of-delta encoding with variable bit packing
- **Floats**: XOR encoding with leading/trailing zero compression

Real-world results:
- Sequential timestamps (1s intervals): 55x compression
- Constant values: 58x compression
- Random floats: 3-5x compression

## Configuration

```yaml
# epoch.yaml
server:
  bind: ":8086"
  data_dir: "./data"

storage:
  shard_duration: "24h"
  retention: "7d"
  max_buffer_size: 10000
  flush_interval: "10s"

cluster:
  node_id: "node-1"
  bind: ":7946"
  rpc: ":7947"
  peers:
    - "node-2:7947"
    - "node-3:7947"
  replication_factor: 3
  write_consistency: "quorum"
```

## Query Language

### SELECT

```sql
SELECT field1, field2 FROM measurement
SELECT * FROM measurement WHERE time > now() - 1h
SELECT mean(value), max(value) FROM measurement GROUP BY time(5m)
SELECT * FROM measurement WHERE host = 'server01' ORDER BY time DESC LIMIT 10
```

### Supported Aggregates

- `COUNT`, `SUM`, `MEAN` (alias: `AVG`)
- `MIN`, `MAX`, `FIRST`, `LAST`
- `MEDIAN`, `PERCENTILE(field, n)`
- `STDDEV`

### Time Functions

```sql
WHERE time > now() - 1h
WHERE time >= '2024-01-15T00:00:00Z' AND time < '2024-01-16T00:00:00Z'
GROUP BY time(5m)
GROUP BY time(1h), host
```

## Performance

Targets (your mileage may vary):

| Metric | Target |
|--------|--------|
| Write throughput | 500k points/sec |
| Query latency p95 | <100ms |
| Compression ratio | 10:1 |

Run benchmarks yourself:

```bash
go test -bench=. ./internal/storage/
go test -bench=. ./internal/query/
go test -bench=. ./internal/compress/
```

## Philosophy

1. **Simple over clever** - if you need to read the source to use it, we failed
2. **Disk is cheap, ops time isn't** - compression and retention should just work
3. **SQL-ish is good enough** - perfect compatibility isn't worth complexity
4. **Cluster when needed** - single node should be viable for most uses

## License

MIT

## Author

Katie

---

*Built because sometimes you just want to store metrics without reading a 200-page manual first.*
