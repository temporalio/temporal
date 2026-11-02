# Worker liveness tracking

Workers that heartbeat together share a lease in CHASM.

- One execution per SDK `worker_grouping_key` stores the shared lease.
- Heartbeat batches renew the shared lease. Worker metadata stays in the in-memory registry. Lease expiry closes the run; a closed run cannot be renewed.
- CHASM handles persistence and timer scheduling. The library owns worker state and lease transitions.
