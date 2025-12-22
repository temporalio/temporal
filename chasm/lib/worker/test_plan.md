# CHASM Worker Liveness Scale Test Plan

This document outlines the test plan for evaluating the scalability of CHASM-based worker liveness tracking under various load conditions, including crash-loop scenarios.

## Minimum Granularity

**A worker is namespace-scoped.** Each worker belongs to exactly one namespace.

- **Minimum test unit**: 1 worker, 1 namespace
- **Scaling dimension**: Number of workers within a single namespace
- **Multi-namespace scaling**: Results can be linearly extrapolated by number of namespaces

Since workers across namespaces are independent, testing N workers in 1 namespace gives equivalent per-namespace load as testing N workers across M namespaces (each namespace gets N/M workers).

## Test Environment

- Number of namspaces: 1
- Number of workers per namespace: 10K
- Number of activities per worker:
  - N=5: Normal throughput
  - N=100: High throughput (Note: SDK default is 1000 for MaxConcurrentActivityExecutionSize)

- Worker heartbeat interval: 20 seconds
- Heartbeat lease duration: 1 minute

For single-namespace testing, a minimal cluster suffices:

| Component | Specification | Notes |
|-----------|--------------|-------|
| Frontend | 1 node | Single namespace, not a bottleneck |
| History | 1 node | CHASM entities + timer tasks processed here |
| Matching | 1 node | Task queue routing |
| Database | Cassandra or PostgreSQL | Main bottleneck for writes |
| History shards | 4 | Can increase for higher load |

**For production capacity planning**, multiply by number of namespaces and add nodes proportionally.

## Load Analysis

### Steady State Baseline

With 10K workers heartbeating every 20s (single namespace):
```
Heartbeats/sec = 10,000 workers / 20s interval = 500 heartbeats/sec
```

### Crash Loop Impact

When workers restart every 10s (aggressive crash loop):

**CHASM Worker Entity Operations:**

| Operation | Per Worker | Rate (10K workers, 10s restart) |
|-----------|------------|-------------------------------|
| Create new CHASM entity | 1 DB write | 1,000/sec |
| Initial heartbeat | 1 DB write | 1,000/sec |
| Lease timer scheduled | 1 timer task | 1,000/sec |
| Old lease expires | 1 timer task execution | 1,000/sec (60s delayed) |
| State → INACTIVE | 1 DB write | 1,000/sec |
| State → CLEANED_UP | 1 DB write | 1,000/sec |
| Delete CHASM entity | 1 DB delete | 1,000/sec |
| **CHASM subtotal** | **6 DB ops** | **~6,000/sec** |

**Activity Rescheduling (when worker becomes INACTIVE):**

| Operation | Per Worker | Rate (10K workers, 10s restart) |
|-----------|------------|-------------------------------|
| RespondActivityTaskFailed RPC | N per worker | 1,000 × N/sec |
| Workflow mutable state update | 1 per activity | 1,000 × N/sec |
| **Activity subtotal (N=5)** | **5 DB ops** | **~5,000/sec** |

**Total DB operations: ~11,000/sec** (6,000 CHASM + 5,000 activity with N=5)

### Entity Accumulation

During crash loop, old workers don't immediately disappear (lease duration = 60s):
```
Peak entities = steady_state × (lease_duration / restart_interval)
             = 10,000 × (60s / 10s)
             = 60,000 entities (6× steady state)
```

### Multi-Namespace Scaling

To estimate load for M namespaces:
```
Total load = Single namespace load × M
```

Example: 100 namespaces × 10K workers each = 1M workers total
- Heartbeats: 100,000/sec
- DB writes: 100,000/sec

## Test Scenarios

All scenarios use **1 namespace** for simplicity. Scale results by namespace count for capacity planning.

### Scenario 1: Steady State Baseline

**Goal**: Establish baseline metrics for normal operation (no crashes).

| Parameter | Value |
|-----------|-------|
| Workers | 10,000 |
| Heartbeat interval | 20s |
| Lease duration | 60s |
| Duration | 10 minutes |
| Worker restarts | None |

**Expected load**:
- Heartbeats: 500/sec
- DB writes: ~500/sec
- Timer operations: ~0 (leases not expiring)

### Scenario 2: Moderate Crash Loop (N=5 activities)

**Goal**: Test gradual worker turnover with moderate activity count.

| Parameter | Value |
|-----------|-------|
| Workers | 10,000 |
| Restart rate | 1,000 workers/min (10% churn) |
| Heartbeat interval | 20s |
| Lease duration | 60s |
| Activities per worker | 5 |
| Duration | 15 minutes |

**Expected load**:
- New worker creates: ~17/sec
- Lease expirations: ~17/sec (after 60s ramp)
- Activity reschedules: ~85/sec
- Peak entities: ~10,500 (1.05× steady state)
- Peak DB writes: ~200/sec

### Scenario 3: Aggressive Crash Loop (N=5 activities)

**Goal**: Stress test under rapid worker churn with moderate activity count.

| Parameter | Value |
|-----------|-------|
| Workers | 10,000 |
| Restart interval | 10s |
| Heartbeat interval | 20s |
| Lease duration | 60s |
| Activities per worker | 5 |
| Duration | 10 minutes |

**Expected load**:
- New worker creates: 1,000/sec
- Lease expirations: 1,000/sec (after 60s ramp)
- Activity reschedules: 5,000/sec
- Peak entities: ~60,000 (6× steady state)
- Peak DB writes: ~11,000/sec

### Scenario 4: Recovery from Crash Loop

**Goal**: Measure system recovery when crash loop stops.

| Phase | Duration | Behavior |
|-------|----------|----------|
| Ramp-up | 5 min | Aggressive crash loop (Scenario 3) |
| Stabilize | 5 min | All workers stop crashing, heartbeat normally |
| Observe | 5 min | Watch entity count decrease to steady state |

**Key metrics**:
- Time to return to 10K entities
- Cleanup task backlog
- DB write rate during cleanup

### Scenario 5: Moderate Crash Loop (N=100 activities)

**Goal**: Test gradual worker turnover with high activity count.

| Parameter | Value |
|-----------|-------|
| Workers | 10,000 |
| Restart rate | 1,000 workers/min (10% churn) |
| Heartbeat interval | 20s |
| Lease duration | 60s |
| Activities per worker | 100 |
| Duration | 15 minutes |

**Expected load**:
- New worker creates: ~17/sec
- Lease expirations: ~17/sec (after 60s ramp)
- Activity reschedules: ~1,700/sec (20× Scenario 2)
- Peak entities: ~10,500 (1.05× steady state)
- Peak DB writes: ~2,000/sec

### Scenario 6: Aggressive Crash Loop (N=100 activities)

**Goal**: Stress test activity rescheduling at scale.

| Parameter | Value |
|-----------|-------|
| Workers | 10,000 |
| Restart interval | 10s |
| Heartbeat interval | 20s |
| Lease duration | 60s |
| Activities per worker | 100 |
| Duration | 10 minutes |

**Expected load**:
- New worker creates: 1,000/sec
- Lease expirations: 1,000/sec (after 60s ramp)
- Activity reschedules: **100,000/sec** (20× Scenario 3)
- Peak entities: ~60,000 (6× steady state)
- Peak DB writes: **~106,000/sec**

---

## Scenario Comparison Summary

| Scenario | Workers | Activities (N) | Restart Interval | Peak Activities | Activity Reschedules/sec | DB Writes/sec |
|----------|---------|----------------|------------------|-----------------|--------------------------|---------------|
| 1 Steady State | 10,000 | N/A | None | N/A | 0 | ~500 |
| 2 Moderate (N=5) | 10,000 | 5 | ~1/min | ~52K | ~85 | ~200 |
| 3 Aggressive (N=5) | 10,000 | 5 | 10s | ~300K | 5,000 | ~11,000 |
| 5 Moderate (N=100) | 10,000 | 100 | ~1/min | ~1M | ~1,700 | ~2,000 |
| 6 Aggressive (N=100) | 10,000 | 100 | 10s | ~6M | 100,000 | ~106,000 |

**Key takeaway**: Activity count (N) is the primary multiplier for crash loop impact.
- Scenarios 2 & 3 (N=5) vs Scenarios 5 & 6 (N=100) show 20× difference in activity reschedules.

## Metrics to Collect

### CHASM Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `chasm.worker.entity_count` | CHASM | > 2× steady state |
| `chasm.worker.create_rate` | CHASM | > 5,000/sec |
| `chasm.worker.lease_expiry_rate` | CHASM | > 5,000/sec |
| `chasm.worker.cleanup_rate` | CHASM | Falling behind create rate |
| `chasm.worker.activity_reschedule_latency_p99` | CHASM | > 5s |

### Database Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| DB write latency p99 | Database | > 100ms |
| DB write throughput | Database | Approaching capacity |
| DB connection pool exhaustion | Database | > 80% |

### Timer Queue Metrics (History Service)

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Timer queue depth | History | > 100K pending |
| Timer queue processing latency p99 | History | > 10s |
| Timer task completion rate | History | < creation rate |

### History Service Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `RespondActivityTaskFailed` rate | History | N/A (informational) |
| `RespondActivityTaskFailed` latency p99 | History | > 1s |
| Workflow lock wait time p99 | History | > 500ms |
| Mutable state cache hit rate | History | < 50% |

## Test Implementation

See `tests/chasm_worker_benchmark_test.go` for the actual test code.

```go
// Simplified example - single worker lifecycle test
func TestBaseline_SingleWorker() {
    workerID := uuid.New().String()
    
    // Send heartbeat - creates CHASM entity
    RecordWorkerHeartbeat(namespace, workerID)
    
    // Send second heartbeat - updates CHASM entity
    RecordWorkerHeartbeat(namespace, workerID)
    
    // Verify worker exists
}
```

## Success Criteria

| Scenario | Criteria |
|----------|----------|
| Scenario 1 (Baseline) | DB p99 < 50ms, all heartbeats processed |
| Scenario 2 (10% churn) | Entity count stable at ~10.5K, cleanup keeps up |
| Scenario 3 (Aggressive) | No OOM, DB p99 < 200ms, cleanup eventually catches up |
| Scenario 4 (Recovery) | Return to 10K entities within 2× lease duration |
| Scenario 5 (Activity) | Activity reschedule success rate > 99% |

## Failure Modes to Test

1. **Database slowdown**: Inject 500ms latency, verify backpressure
2. **Timer queue saturation**: Verify graceful degradation
3. **History service overload**: Verify activity reschedule retries
4. **Network partition**: Worker can't heartbeat, verify lease expiry works

## Comparison: CHASM vs Current Registry

| Aspect | Current Registry (Matching) | CHASM Worker |
|--------|----------------------------|--------------|
| Storage | In-memory | Database (persistent) |
| State per worker | ~1KB (heartbeat proto) | ~5KB (full CHASM entity) |
| Heartbeat cost | O(1) map update | DB write + timer reschedule |
| Cleanup | Background eviction | State machine + timer tasks |
| Activity binding | None | Stored in worker entity |
| Crash loop 10K workers | ~60MB memory peak | ~300MB DB + 22K ops/sec |

## Optimizations

1. **Batch activity timeout API**: Single RPC per history shard to timeout multiple activities
2. **Rate limit worker registration**: Prevent runaway entity creation during crash loops
3. **Debounce heartbeats**: Coalesce rapid heartbeats from same worker
4. **Async cleanup**: Don't block on activity rescheduling
