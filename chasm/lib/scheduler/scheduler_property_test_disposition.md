# Scheduler property-test disposition

- Ported: independent transition model; Rapid and bounded-explorer model tests; descriptor-backed RPC behaviors; delivery/reload harness; model conformance; retry/redelivery; overlap; generator/backfill boundaries; migration retry and terminal recovery; callback recovery; and initial-patch, conflict-token, capacity, and concurrent-backfill audits.
- Superseded: the pre-Phase-4 monolithic `scheduler_model_test.go` retains the durable trace corpus and low-level delivery contract cases.
- Deferred: the remaining narrow audit regressions that are already subsumed by the model, conformance, failure, overlap, and boundary campaigns.
