# Worker liveness tracking

Workers that heartbeat together share a lease in CHASM.

- One execution per SDK `worker_grouping_key` contains worker components keyed by `worker_instance_key`.
- Heartbeat batches update worker metadata and renew the shared lease. Lease expiry closes the run; a closed run cannot be renewed.
- CHASM handles persistence and timer scheduling. The library owns worker state and lease transitions.
- Matching forwards heartbeats to History when `worker.enableStateTracking` is enabled for the namespace. SDKs without a grouping key continue using the metadata-only registry.
