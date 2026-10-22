# Local concurrency metrics dashboard

This dashboard covers all six new concurrency metric families, component-tagged
persistence and visibility metrics, and the Batch / Wait RPCs. It selects the
newest observing history host for slot gauges instead of summing host gauges.

## View the local run

Grafana: http://localhost:13000/d/concurrency-slot-metrics

Prometheus: http://localhost:18080

The local setup scrapes the instrumented server at
`host.docker.internal:18000/metrics` every **1 second**. The dashboard refreshes
every **5 seconds**. Ports are bound to localhost. It uses its own Compose project
and volumes; Prometheus retains samples for two days.

From the repository root:

```bash
OBS=chasm/lib/flowcontrol/concurrency/observability

docker compose -p concurrency-metrics-local -f "$OBS/compose.yaml" up -d
```

The instrumented Temporal server must already be running. The local smoke test
used frontend port 17233, history port 17234, and metrics port 18000. This Compose
file starts only Prometheus and Grafana. The image digests pin the locally tested
Prometheus 3.13.1 and Grafana 13.1.0 images.

To stop these two monitoring containers while retaining their recorded data:

```bash
docker compose -p concurrency-metrics-local -f "$OBS/compose.yaml" down
```

## Import the JSON elsewhere

Import `concurrency-slots-dashboard.json` in Grafana and select the Prometheus
data source, scrape job, namespace ID, and task queues. The persistence component
selector defaults to `concurrency_limiter`.

Local Tally exports sanitized label values, such as `slot_metrics_live`, while
other exporters may preserve the original hyphens. The variables discover the
actual labels. The two default local queues are `slot_metrics_live` and
`slot_metrics_probe`; choose the queues available in the destination data source.
Histogram timers in this dashboard are in **seconds**.

## What was verified locally

On October 22, 2026, using the instrumented server and in-memory SQLite:

- A real standalone activity workload completed **300/300** activities with C=5,
  D=850ms, 20 producer starts/s, 8 pollers, and 64 worker slots. Maximum SDK
  executions observed: **5**.
- The live queue recorded **300 APPLIED reserves, 300 APPLIED commits, and 300
  APPLIED releases**, plus 300 samples in each of the two latency histograms.
- A separate C=3 probe verified 18 phases: all four slot states, all ten supported
  operation/outcome pairs, a blocked Wait waking after capacity became available,
  and one reservation expiration.
- After expiration, the slot gauge retained its last successful Batch observation.
  A subsequent Batch refreshed it to zero held slots and three available slots.
- Limiter execution reads/writes carried `archetype="concurrency_limiter"`;
  standalone activity and shared maintenance operations were separately tagged.
  Activity visibility writes carried `archetype="activity"`.
- Every dashboard expression was executed successfully against local Prometheus,
  and Grafana loaded the dashboard. This is an instrumentation smoke test;
  SQLite timings do not represent Cassandra costs.

The current run's raw metrics, phase log, query checks, results JSON, and fixed
Grafana time-window link are saved in:

```text
/private/tmp/concurrency-all-metrics-20261022/
```

## Interpretation

Slot gauges and their timestamps are event-driven observations, not periodic
state reads. Consult observation age before interpreting idle values. Counter
panels show process-lifetime counts; rate panels show the selected time window.
The cumulative panels expose rare first events that can occur before a series'
first scrape. Short probe phases deliberately hold reservations and are not a
steady-state latency benchmark.

Persistence panels are cell-wide for the selected component. They cannot be
filtered by the namespace ID or task queue variables because those labels are
absent. These counters count persistence API attempts, including failed attempts,
rather than individual Cassandra statements. Shared queue scans/range cleanup
and shard checkpoints remain `_unknown_`.

Typed persistence errors include expected NotFound lookups during execution
creation; the probe also verifies a missing-limiter NotFound response. Unexpected
persistence failures and resource exhaustion are expected to remain zero in this
healthy run. No database outage or persistence rate-limit failure is injected.
