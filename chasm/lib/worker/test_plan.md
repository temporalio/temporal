# CHASM Worker Liveness Scale Test Plan

This document outlines the test plan for evaluating the scalability of CHASM-based worker liveness tracking under various load conditions, including crash-loop scenarios.

## Minimum Granularity

**A worker is namespace-scoped.** Each worker belongs to exactly one namespace.

- **Minimum test unit**: 1 worker, 1 namespace
- **Scaling dimension**: Number of workers within a single namespace
- **Multi-namespace scaling**: Results can be linearly extrapolated by number of namespaces

Since workers across namespaces are independent, testing N workers in 1 namespace gives equivalent per-namespace load as testing N workers across M namespaces (each namespace gets N/M workers).

## Test Configuration

Current benchmark configuration (see `tests/chasm_worker_benchmark_test.go`):

| Parameter | Value |
|-----------|-------|
| Number of namespaces | 1 |
| Number of workers | 1,000 |
| Activities per worker | 100 |
| Worker heartbeat interval | 20 seconds |
| Churn interval | 10 seconds |
| Test duration | 120 seconds |
| Heartbeat lease duration | 60 seconds (default) |

For single-namespace testing, a minimal cluster suffices:

| Component | Specification | Notes |
|-----------|--------------|-------|
| Frontend | 1 node | Single namespace, not a bottleneck |
| History | 1 node | CHASM entities + timer tasks processed here |
| Matching | 1 node | Task queue routing |
| Database | Cassandra or PostgreSQL | Main bottleneck for writes |
| History shards | 4 | Can increase for higher load |

**For production capacity planning**, multiply by number of namespaces and add nodes proportionally.

## Test Scenarios

All scenarios use **1 namespace** with the configuration above.

### Scenario 1: Steady State (0% Churn)

**Goal**: Establish baseline metrics for normal operation (no crashes).

| Parameter | Value |
|-----------|-------|
| Workers | 1,000 |
| Churn % | 0% |
| Activities | 100 per worker |

**Expected**: All 1,000 workers created once, heartbeating every 20s.

### Scenario 2: Moderate Churn (20%)

**Goal**: Test gradual worker turnover simulating typical rolling deployments.

| Parameter | Value |
|-----------|-------|
| Workers | 1,000 |
| Churn % | 20% every 10s |
| Activities | 100 per worker |

**Expected**: 200 workers restart every 10s, generating ~27 workers/sec.

### Scenario 3: Aggressive Churn (50%)

**Goal**: Stress test under rapid worker churn.

| Parameter | Value |
|-----------|-------|
| Workers | 1,000 |
| Churn % | 50% every 10s |
| Activities | 100 per worker |

**Expected**: 500 workers restart every 10s, generating ~54 workers/sec.

---

## Expected Load (Theoretical)

### Quick Summary: Feature Overhead

| Scenario | DB Writes/sec | vs Steady State | Peak Entities | Typical Duration |
|----------|---------------|-----------------|---------------|------------------|
| **Steady State** | 50 | baseline | 1,000 | 99% of time |
| **20% Churn** | ~2,100 | +4,100% | 2,200 | 5-10 min (deployment) |
| **50% Churn** | ~5,200 | +10,300% | 4,000 | Rare (mass restart) |

*Based on 1,000 workers × 100 activities, 20s heartbeat interval, 60s lease duration*

**Key insights**:
1. **Churn is temporary**: Deployments typically last 5-10 minutes, then back to steady state
2. **Steady state dominates**: 99%+ of time, overhead is just 50 writes/sec (heartbeats)
3. **Activity rescheduling dominates churn cost**: 100 activity ops vs 4 CHASM ops per restart

### CHASM Worker State Machine

```
ACTIVE → INACTIVE (terminal) → delete entity
```

- **ACTIVE**: Worker is heartbeating, lease timer running
- **INACTIVE**: Lease expired, activities rescheduled, entity deleted
- No intermediate CLEANED_UP state (SDK handles network partition by creating new WorkerInstanceKey)

### Steady State Baseline

With 1K workers heartbeating every 20s:
```
Heartbeats/sec = 1,000 workers / 20s interval = 50 heartbeats/sec
```

### Worker Restarts (20% Churn)

When 20% of workers restart every 10s:

| Metric | Value |
|--------|-------|
| Workers restarting | 200 every 10s = 20/sec |
| CHASM ops | 20/sec × 4 ops = **80/sec** |
| Activity reschedules | 20/sec × 100 activities = **2,000/sec** |
| **Total DB ops** | **~2,080/sec** |

**Entity Accumulation:** Old workers don't immediately disappear (lease = 60s):
```
Peak entities = 1,000 + (20/sec × 60s) = 2,200 entities (2.2× steady state)
```

### Worker Restarts (50% Churn)

When 50% of workers restart every 10s:

| Metric | Value |
|--------|-------|
| Workers restarting | 500 every 10s = 50/sec |
| CHASM ops | 50/sec × 4 ops = **200/sec** |
| Activity reschedules | 50/sec × 100 activities = **5,000/sec** |
| **Total DB ops** | **~5,200/sec** |

**Entity Accumulation:** Old workers don't immediately disappear (lease = 60s):
```
Peak entities = 1,000 + (50/sec × 60s) = 4,000 entities (4× steady state)
```

### Summary Comparison

| Scenario | Workers/sec | CHASM ops/sec | Activity ops/sec | Total DB ops/sec | Peak Entities |
|----------|-------------|---------------|------------------|------------------|---------------|
| Steady State | 0 | 0 | 0 | 50 (heartbeats) | 1,000 (1×) |
| 20% Churn | 20 | 80 | 2,000 | ~2,080 | 2,200 (2.2×) |
| 50% Churn | 50 | 200 | 5,000 | ~5,200 | 4,000 (4×) |

---

## Benchmark Results (Measured)

See `benchmark_results.md` for detailed results.

### Summary (1,000 Workers × 100 Activities)

| Test | Churn % | Workers/sec | Throughput (hb/sec) | Latency p50 | Latency p99 | Errors |
|------|---------|-------------|---------------------|-------------|-------------|--------|
| Steady State | 0% | 8.33 | 49.99 | 564.74ms | 1.513s | 0 |
| Churn 20% | 20% | 26.67 | 61.31 | 276.80ms | 888.58ms | 0 |
| Churn 50% | 50% | 54.16 | 71.11 | 369.33ms | 1.084s | 0 |

### Key Observations

1. **Activity IDs add significant overhead**: p50 latency ~300-560ms with 100 activities vs ~6ms without.
2. **Zero errors**: System handles 1,000 workers × 100 activities with 50% churn.
3. **Churn paradoxically improves p50 latency**: Steady state has highest p50 (564ms vs 276-369ms).

---

## Metrics to Collect

### CHASM Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `chasm.worker.entity_count` | CHASM | > 2× steady state |
| `chasm.worker.create_rate` | CHASM | > 5,000/sec |
| `chasm.worker.lease_expiry_rate` | CHASM | > 5,000/sec |
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

---

## Success Criteria

| Scenario | Criteria |
|----------|----------|
| Steady State | p99 < 2s, 0 errors |
| 20% Churn | p99 < 1.5s, 0 errors |
| 50% Churn | p99 < 2s, 0 errors, no OOM |

---

## Failure Modes to Test

1. **Database slowdown**: Inject 500ms latency, verify backpressure
2. **Timer queue saturation**: Verify graceful degradation
3. **History service overload**: Verify activity reschedule retries
4. **Network partition**: Worker can't heartbeat, verify lease expiry works

---

## Comparison: CHASM vs Current Registry

| Aspect | Current Registry (Matching) | CHASM Worker |
|--------|----------------------------|--------------|
| Storage | In-memory | Database (persistent) |
| State per worker | ~1KB (heartbeat proto) | ~5KB (full CHASM entity) |
| Heartbeat cost | O(1) map update | DB write + timer reschedule |
| Cleanup | Background eviction | Lease expiry timer (single timer per worker) |
| Activity binding | None | Stored in worker entity |
| 1K workers with 50% churn | ~6MB memory peak | ~15MB DB + 5.2K ops/sec |

---

## Optimizations

1. **Batch activity timeout API**: Single RPC per history shard to timeout multiple activities
2. **Rate limit worker registration**: Prevent runaway entity creation during high churn
3. **Debounce heartbeats**: Coalesce rapid heartbeats from same worker
4. **Async cleanup**: Don't block on activity rescheduling
