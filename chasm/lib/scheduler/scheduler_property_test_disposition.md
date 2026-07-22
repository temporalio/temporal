# Scheduler property-test disposition

- Ported: independent transition model; Rapid and bounded-explorer model tests; descriptor-backed RPC behaviors; delivery/reload harness; model conformance; retry/redelivery; overlap; generator/backfill boundaries; migration retry/reload.
- Superseded: the pre-Phase-4 monolithic `scheduler_model_test.go` retains the durable trace corpus and low-level delivery contract cases.
- Deferred: callback recovery, migration terminal-failure recovery, and the audit matrix rely on Scheduler production behavior present only on `recovery/chasm-property-20260721`. They should be enabled with the corresponding product fixes in a separate change.
