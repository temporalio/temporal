# CHASM Worker Benchmark Results

**Date:** 2025-12-23
**Environment:**
- Persistence: Cassandra (Docker)
- Go version: go1.25.0 darwin/arm64
- Machine: MacBook Pro (Apple Silicon)

---

## Configuration

| Parameter | Value |
|-----------|-------|
| Workers | 1000 |
| Activities per worker | 100 |
| Heartbeat interval | 20s |
| Churn interval | 10s |
| Test duration | 120s |

---

## Results: With Activity IDs (100 activities/worker)

### Steady State (0% Churn)

```
Workers created: 8.33/sec
Throughput: 49.99 heartbeats/sec
Latency: avg=594.51ms p50=564.74ms p95=1.285s p99=1.513s
Errors: 0
```

### 20% Churn

```
Workers created: 26.67/sec
Throughput: 61.31 heartbeats/sec
Latency: avg=310.89ms p50=276.80ms p95=704.21ms p99=888.58ms
Errors: 0
```

### 50% Churn

```
Workers created: 54.16/sec
Throughput: 71.11 heartbeats/sec
Latency: avg=405.51ms p50=369.33ms p95=869.81ms p99=1.084s
Errors: 0
```

---

## Summary

| Test | Churn % | Workers/sec | Throughput (hb/sec) | Latency p50 | Latency p95 | Latency p99 | Errors |
|------|---------|-------------|---------------------|-------------|-------------|-------------|--------|
| Steady State | 0% | 8.33 | 49.99 | 564.74ms | 1.285s | 1.513s | 0 |
| Churn 20% | 20% | 26.67 | 61.31 | 276.80ms | 704.21ms | 888.58ms | 0 |
| Churn 50% | 50% | 54.16 | 71.11 | 369.33ms | 869.81ms | 1.084s | 0 |

---

## Observations

1. **Steady state has highest latency**: p50=564ms vs 276-369ms for churn tests. This is counterintuitive - expected steady state to be faster.

2. **Churn increases throughput**: 20% churn = 61 hb/sec, 50% churn = 71 hb/sec, vs 50 hb/sec steady state. This is because each worker restart generates additional registration heartbeats.

3. **Tail latency (p99) around 1-1.5s**: All tests show p99 latencies of 888ms-1.5s, indicating some operations take significantly longer.

4. **Zero errors**: System handles 1000 workers × 100 activities with up to 50% churn without errors.

5. **Activity IDs add significant overhead**: Compared to previous tests without activities:
   - p50 latency: ~6ms → ~300-560ms (50-90x increase)
   - p99 latency: ~900ms → ~1-1.5s (~1.5x increase)

---

## Comparison: Without Activities vs With Activities

| Metric | Without Activities | With 100 Activities | Change |
|--------|-------------------|---------------------|--------|
| Steady State p50 | 6.13ms | 564.74ms | ~92x |
| Steady State p99 | 903ms | 1.513s | ~1.7x |
| Churn 20% p50 | - | 276.80ms | - |
| Churn 50% p50 | 72.23ms | 369.33ms | ~5x |

---

## Previous Results (Without Activities)

### 1000 Workers, No Activities

| Test | Workers/sec | Throughput (hb/sec) | Latency p50 | Latency p99 | Errors |
|------|-------------|---------------------|-------------|-------------|--------|
| Steady State | - | 50.00 | 6.13ms | 903.47ms | 0 |
| Worker Churn (10s restart) | 50.00 | 91.67 | 95.98ms | 860.27ms | 0 |
