# History gRPC P99 Latency — Recurring Incident Analysis (s-gc014)

**Cluster:** s-gc014
**Period:** Tuesday Feb 24 – Thursday Feb 26, 2026
**Status:** Closed. Follow-ups open.

---

## Pattern Summary

This is a **recurring incident pattern**, not a one-off. A history pod starts up with degraded
connectivity to Astra (Cassandra), causing it to invoke gocql flow control. This throttles
persistence operations and causes context deadline exceeded errors that spread across **all
pods in the cell**, not just the problem pod. The impact manifests as a cell-wide P99 latency
spike to ~31s (the gRPC context deadline).

The first three incidents (Feb 24–25) were all on the same GKE node (`bj2w`), which was
eventually cordoned by GKE. Incidents continued on other nodes after that.

---

## Mechanism

1. A new or restarting history pod comes up with **high latency to Astra** (Cassandra)
2. The pod's gocql flow control interprets the high latency as Astra being slow and
   **rate-limits its own persistence operations**
3. Shard recovery times on the pod are extremely long (5–20+ seconds)
4. The `host_health` mechanism **correctly detects the pod as unhealthy** (`declined_serving`)
   and routes new inbound requests away from it
5. However, **cross-shard calls bypass health state**. When another pod needs to act on a
   workflow whose shard is owned by the problem pod, it must forward that call there regardless
   of the pod's health state — shard ownership is deterministic
6. These cross-shard calls hang until the 30s gRPC context deadline
7. Cell-wide P99 latency spikes to ~31s even though average latency stays low (~18ms),
   because only requests touching the problem pod's shards are affected
8. The Astra latency spikes seen after pod kill (~13:49) are the **cell catching up**, not
   the initial trigger

**Key distinction:** WAL boss → bookies and SMS latencies were normal. The high latency
was specific to the problem pod's connection to Astra.

**What worked vs. what didn't:**
- ✅ `host_health` detected the pod as `declined_serving` — inbound request routing worked
- ❌ `declined_serving` has no effect on cross-shard call routing, which is shard-owner-deterministic
- ❌ Cell-wide average latency (18ms) and error ratio (~0%) checks did not fire — both too coarse

---

## Observed Data (Feb 26 – 13:35 incident)

**Time range:** `2026-02-26 13:15:00` to `2026-02-26 14:15:00`

| Metric | Baseline | During incident |
|--------|----------|-----------------|
| gRPC average latency | ~2ms | ~18ms |
| gRPC P99 latency | ~0s | ~31s |
| gRPC error ratio | ~0% | ~0% (requests hang, not fail) |
| Persistence average latency | ~2ms | ~18ms |
| Persistence P99 latency | ~16ms | ~43ms |
| Persistence error ratio | ~0.06% | ~0.21% |

**Timeline:**
- `13:35` — `history-6b9db96fb9-xlhv4` starts up; shard recovery times immediately >15s
- `13:35–13:48` — High rates of context deadline exceeded errors to Astra on **all pods**
- `13:49` — Pod `xlhv4` manually killed by Nithya
- `13:49+` — Astra latency spike (cell catching up, not the root cause)
- Recovery to baseline shortly after pod kill

**Dashboards:**
- [History](https://grafana.tmprl-internal.cloud/goto/Iraik0ODR?orgId=1)
- [Cassandra](https://grafana.tmprl-internal.cloud/goto/5bemzAdDg?orgId=1)
- [Raw chronicle tables](https://grafana.tmprl-internal.cloud/goto/kKMnzAdDg?orgId=1)
- [gocql flow control](https://grafana.tmprl-internal.cloud/goto/FwDGi0dDg?orgId=1)

---

## Full Incident Timeline (All Week)

| Date/Time (UTC) | Alert | Node | Pod | Resolution |
|-----------------|-------|------|-----|------------|
| Tue Feb 24 – 6:02 PM | HistoryP99LatencySLOViolation | `bj2w` | `x4trm` | Resolved; Astra case opened |
| Wed Feb 25 – 7:17 AM | HistoryP99LatencySLOViolation | `bj2w` | `8l2q` | Resolved |
| Wed Feb 25 – 10:28 PM | HistoryP99LatencySLOViolation | — | — | Self-resolved |
| Wed Feb 25 – 10:30 PM | CDSHistoryPodNotReady | `bj2w` | `nh6cz` | Node cordoned by GKE |
| Thu Feb 26 – 1:37 PM | HistoryP99LatencySLOViolation | — | — | Scaled out history + frontend |
| Thu Feb 26 – 1:40 PM | CDSHistoryPodNotReady | `6mfm` | `xlhv4` | Pod manually killed |
| Thu Feb 26 – 4:14 PM | HistoryP99LatencySLOViolation | — | — | Query errors + flow control |
| Thu Feb 26 – 5:30 PM | HistoryP99LatencySLOViolation | `8pfc` | `cgztn` | Higher Astra latencies |

**Notable:** Node `bj2w` caused the first three incidents before GKE cordoned it. Incidents
continued on different nodes after that, suggesting the node was a contributing factor but
not the only cause.

---

## Open Questions

1. **Why does a new pod have high Astra latency on startup?**
   - Is this a network routing issue specific to certain GKE nodes?
   - Is this a Cassandra client connection warmup issue (cold connection pool)?
   - Is this related to shard recovery load overwhelming the pod's Astra connection on startup?

2. **Why does one pod's flow control affect all pods?**
   - Confirmed: context deadline exceeded errors appear on all pods, not just `xlhv4`
   - Is this because other pods make cross-shard calls to the affected pod's shards?
   - Or is there a shared Astra resource being exhausted?

3. **Are the high Astra latencies specific to the problem pod's node?**
   - WAL boss → bookies OK, SMS OK — so it appears Astra-specific
   - Is the GKE node's network path to Astra degraded?
   - Astra case was opened after the Feb 24 incident — what did they find?

4. **Why is the same pattern recurring after node `bj2w` was cordoned?**
   - Subsequent incidents on `6mfm` and `8pfc` suggest this is not purely a bad node issue

---

## Follow-Up Items

### 1. Fast-fail cross-shard calls to unhealthy pods
The highest-impact improvement. The `host_health` mechanism correctly identified `xlhv4` as
`declined_serving` and routed inbound requests away from it. The gap is that **cross-shard
calls are not health-aware** — they route to the shard owner regardless of its health state
and hang for 30s until context deadline. When a pod is `NOT_SERVING` or `DECLINED_SERVING`,
cross-shard calls targeting its shards should fail fast so callers can handle the error rather
than blocking.

### 2. Ship the P99 health check (branch: `laniehei-workflow-lock-wait-metric`)
**Status: code complete.** The new `CheckTypeRPCP99Latency` check added to `DeepHealthCheck`
fires at 1000ms threshold. During this incident `xlhv4`'s RPC P99 was ~31s — well over
threshold. The check also emits `history_rpc_p99_latency_ms` as a gauge for dashboards.

Note: detection was already partially working (host_health showed `declined_serving`). The
P99 check improves detection speed and reliability and closes the gap where average latency
and error ratio checks were both flat throughout the incident.

### 3. Investigate gocql flow control propagation
Understand exactly how one pod's flow control causes cell-wide context deadline exceeded
errors. Is there a shared resource or is this purely cross-shard call propagation?

### 4. Improve pod startup behavior
- Should shard recovery cap its Astra request rate independently of flow control?
- Should a pod that is still recovering shards be excluded from receiving cross-shard traffic?
- Consider a startup health gate: pod does not accept cross-shard traffic until shard
  recovery latencies are below a threshold.

### 5. Fix the History gRPC Error Ratio panel
Y-axis scaled to 10000% with "Value" legend — query appears broken (likely divide-by-zero
or missing recording rule). This panel should show errors during incidents and was dark
throughout.

### 6. Follow up with Astra
Case was opened after the Feb 24 incident. Confirm findings and whether the high-latency
connection pattern from new pods is a known Astra/Cassandra client behavior.

---

## Metrics & Instrumentation Added

### New Grafana panels (in `tmp/grafana.json`)
- **Persistence P${quantile} Latency** — `histogram_quantile($quantile, sum(rate(persistence_latency_bucket{...}[$rate])) by (le))`
- **History gRPC P${quantile} Latency** — `histogram_quantile($quantile, sum(rate(service_latency_bucket{...}[$rate])) by (le))`

### New DeepHealthCheck metric (branch: `laniehei-workflow-lock-wait-metric`)

| Component | Change |
|-----------|--------|
| `common/metrics/metric_defs.go` | Added `HistoryRPCP99LatencyGauge` (`history_rpc_p99_latency_ms`) |
| `common/aggregate/moving_window_average.go` | Added `Percentile(p float64) float64` on `MovingWindowAvgImpl` |
| `common/rpc/interceptor/health_check.go` | Added `P99Latency()` to `HealthSignalAggregator` interface + impl |
| `common/health/check_types.go` | Added `CheckTypeRPCP99Latency = "rpc_p99_latency"` |
| `common/dynamicconfig/constants.go` | Added `HealthRPCP99LatencyFailure` (default: 1000ms) |
| `service/history/configs/config.go` | Added field + wired it |
| `service/history/handler.go` | Added check 6; emits `HistoryRPCP99LatencyGauge` alongside `HistoryHostHealthGauge` |
