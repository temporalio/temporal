# Worker liveness tracking

Workers that heartbeat together share a lease in CHASM.

- One execution per SDK `worker_grouping_key` owns the shared lease and worker children keyed by `worker_instance_key`.
- Heartbeat batches renew the shared lease. Worker metadata stays in the in-memory registry. Lease expiry closes the run; a closed run cannot be renewed.
- Worker children are detached so their tasks can continue after the group closes, within retention.
- CHASM handles persistence and timer scheduling. The library owns worker state and lease transitions.
- Frontend forwards heartbeats to the worker state machine when `worker.enableDurableWorkerHeartbeats` is enabled for the namespace. SDKs without a grouping key continue using the metadata-only registry.
