# Atomic synchronization architecture spike

## Decision

**Selected on 2026-09-04: Option A, with protocol v1 limited to one existing workflow run.** The
alternatives below remain as supporting analysis, not open choices. A failed attempt must leave
normal and Admin history reads at the previous acknowledged cursor, and no new owner may extend a
partly committed delta.

This spike first addresses the protocol-v1 scope: one existing workflow run per
`SyncLocalExecution`. Continue-as-new and arbitrary parent/child groups are called out separately
because they change the persistence boundary.

## Relevant persistence behavior

An ordinary History update already has a useful logical commit point:

1. `CloseTransactionAsMutation` produces one final mutable-state mutation, tasks, and one or more
   `WorkflowEvents` batches.
2. SQL and Cassandra execution stores append those history nodes first, one batch at a time.
3. They then conditionally update mutable state and its tasks. SQL does this in one shard-locked DB
   transaction; Cassandra uses its mutable-state conditional/logged-batch path.
4. Mutable state records the current branch, `NextEventId`, last history-node transaction ID, and
   version history. Normal history reads and `GetWorkflowExecutionRawHistoryV2` derive their upper
   bound and branch from that committed state.

If a history append or final mutable-state CAS fails, the old mutable state remains authoritative.
Already-appended nodes are not on its selected transaction-ID chain or are above its
`NextEventId`, so supported reads do not expose them. A retry uses newer generated transaction IDs;
the history reader follows that winning chain. Existing best-effort history trimming later removes
abandoned nodes.

This means the required atomicity is *logical visibility*, not one storage-engine transaction that
contains every history byte. Temporal already relies on this append-then-CAS pattern. The local
sync path must make exactly one final mutable-state update for the whole bounded delta; the current
implementation violates the contract because it invokes `ReplicateEventsV2` once per batch, and
each call advances mutable state separately.

## Option A: one bounded active workflow transaction

Run the complete sync in one History request while holding the workflow lock:

1. Load mutable state and validate owner/token/epoch, source cursor, sync ID/fingerprint, versions,
   complete batch continuity, and configured limits before writing.
2. Apply every batch to a working mutable state in memory. Reuse the workflow mutable-state
   rebuilder's event switch, but remove passive-policy assumptions.
3. Clear tasks generated while traversing intermediate states. Refresh tasks from the final state:
   suppress executable tasks while ownership is retained, or create the tasks needed to resume
   upstream when ownership is released. Preserve the lease-expiration task as appropriate.
4. Put the final cursor, sync ID/hash, and retained/released ownership fields in the same mutable
   state mutation.
5. Call the ordinary active `UpdateWorkflowExecution` path once with all event batches and that
   one mutation.
6. Return success only after the mutable-state CAS succeeds. Evict/reload mutable state after any
   ambiguous persistence error.

Failure behavior:

| Failure point | Authoritative result | Recovery |
| --- | --- | --- |
| Validation or in-memory application | Old history/cursor | Return an error; no writes occurred |
| During history-node appends | Old history/cursor; possible unreachable nodes | Retry the same sync or let ownership expire after the failed request has unwound |
| After appends, before/at failed CAS | Old history/cursor; appended nodes remain unreachable | Retry with a new transaction-ID chain; trim abandoned nodes later |
| CAS committed but response was lost | New history/cursor and sync fingerprint | Retry detects the same `sync_id` and hash and returns the committed result |
| Retry has same ID but different content | Existing state unchanged | Reject as an idempotency conflict |

No durable `SYNCING` state is required for this single-request design. While the handler is active,
the workflow lock serializes its lease timer and other mutations. If the process dies before the
CAS, the old mutable state is already a complete rollback point; if it dies after the CAS, the new
state contains the complete commit and fingerprint. Reloading mutable state resolves the only
ambiguous outcome. Physical orphan cleanup need not block takeover because those nodes cannot be
selected by a supported read or mutation.

Choosing this option therefore requires narrowing the current protocol text that unconditionally
requires persisted `pending_sync`, `SYNCING`, and `ROLLING_BACK`. Those states are needed only for a
multi-request staging design. The accepted observable contract remains unchanged: ownership cannot
transfer while an operation has an unresolved logical outcome, and a new owner sees only a whole
old or whole new delta.

Benefits:

- Smallest contract-compliant change and closest to ordinary History writes.
- Works with all existing persistence backends without a new table or distributed transaction.
- No recovery state machine is needed for protocol-v1 bounded single-run syncs.
- Sync fingerprint, cursor, ownership, tasks, and final mutable state share one CAS.

Risks to prove in tests:

- Every supported persistence backend must preserve the observed append-then-CAS visibility
  behavior.
- Event application must never mutate cached authoritative state before commit; failures must evict
  the workflow context.
- Task generation must reflect only the final state and retain/release intent.
- Generated transaction-ID chains and exact-retry handling must tolerate timeout-after-write.
- The maximum bounded request must fit frontend/History memory and RPC constraints.

This is the recommended option for protocol version 1.

## Option B: stage on a hidden history branch, then swap

Persist `pending_sync`, fork or create an unreachable branch at the old cursor, append chunks to
it across one or more requests, build the target mutable state, then atomically swap the committed
branch/version-history pointer and mutable state. Lease expiry and force revoke drive a durable
commit-or-rollback state machine before ownership transfer.

Benefits:

- Supports deltas larger than one RPC and resumable chunk uploads.
- Makes staged data structurally separate from the current branch.
- Explicit recovery progress is inspectable.

Costs and risks:

- Requires branch lifecycle/GC, continuation tokens, durable recovery timers, and idempotent
  commit/rollback logic.
- The final pointer swap still needs correct mutable-state and task reconstruction.
- A branch fork is not by itself an atomic commit for arbitrary child executions on other shards.
- More states create more crash points than the bounded one-request design needs.

Choose this if a required protocol-v1 delta cannot be bounded to one History request or if server
maintainers do not accept unreachable same-branch nodes as sufficient rollback isolation.

## Option C: stage in a new persistence object, then ingest

Upload opaque sync chunks into a dedicated table/blob keyed by namespace, execution, epoch, and
sync ID. Once complete, History validates and ingests them with a final workflow CAS; recovery
deletes or retries the staging object.

This provides clean transport resumability but requires schemas and implementations for every
persistence backend, garbage collection, quotas, encryption/retention decisions, and the same
final event-application work. It is the highest-effort option and has no clear protocol-v1 benefit
over a hidden branch.

## Why the existing replication loop is not an option

Pre-renewing the lease, applying batches through `ReplicateEventsV2`, and finalizing the cursor
does not provide atomicity. After the first successful batch, mutable state and supported history
reads can expose that prefix even if a later batch fails. Preventing takeover does not undo that
partial visibility. Wrapping the same calls in a higher-level handler cannot combine their already
committed mutations.

## Initial dynamic-config defaults

For Option A, start conservatively and tune with tests:

| Limit | Initial default | Rationale |
| --- | ---: | --- |
| Serialized history bytes per sync | 8 MiB | Well below the 128 MiB internode receive cap while allowing more than one ordinary history node |
| Events per sync | 10,240 | Matches the existing history-count warning threshold |
| History batches per sync | 256 | Matches the existing history read page-size scale and bounds per-request loops |
| Unsynchronized bytes per execution | 8 MiB | Ensures the whole retained delta remains eligible for one atomic request |
| Unsynchronized events per execution | 10,240 | Same eligibility rule as the request event cap |
| Bridge early-sync watermark | 75% of byte or event cap | Leaves room for the current local transaction to reach a safe boundary |

These are separate namespace-filtered dynamic-config settings. Existing limits still apply,
including the default 2 MiB per-event blob limit, 4 MiB persistence history-node transaction
limit, and 50 MiB/51,200-event execution-history hard limits. The bridge must not begin another
local transaction if its worst-case admitted output cannot remain within the hard sync limit; a
single oversized transaction fails with a stable resource-exhausted result rather than being
partly synchronized.

If testing shows that 8 MiB is too large for bounded in-memory application or too small for an
ordinary safe-boundary delta, adjust it dynamically or choose a staging option. Numeric tuning is
not a protocol decision.

## Multi-run and multi-execution scope

The normal workflow persistence API can update a current run and its continue-as-new run together;
both have the same workflow ID and therefore the same History shard. The active-path spike should
defer enabling this until that two-run transaction is explicitly tested.

Locally created child workflows can have different workflow IDs and therefore different History
shards. Temporal has no ordinary atomic transaction spanning those executions. Phase 5 must choose
one of:

- constrain each sync commit to one execution and define an ordering that is acceptable without
  group-wide atomic visibility;
- add a durable cross-shard publication coordinator/barrier; or
- narrow the product contract so fencing is group-wide but history visibility is atomic only per
  execution.

Option A is a complete answer for protocol-v1 single-execution synchronization, not proof of a
future cross-shard atomic group.

## Selected rationale

**Option A** was selected: one bounded active workflow transaction for protocol version 1. It satisfies
the required observable rollback/commit semantics by using Temporal's existing logical commit
point and avoids a staging recovery state machine.

Protocol version 1 is also confirmed to remain limited to **one existing workflow run per sync**.
Continue as new and child groups stay disabled until their transaction boundaries are separately
specified and tested in a later protocol version.

The alternatives considered were:

- **A — One bounded active workflow transaction (recommended).**
- **B — Hidden-branch staging with durable commit/rollback.**
- **C — New persistence staging object with durable commit/rollback.**

## Code evidence

- Workflow mutation and event-batch shape:
  `temporal-server/common/persistence/data_interfaces.go`.
- Active transaction close and one update call:
  `temporal-server/service/history/workflow/context.go`,
  `temporal-server/service/history/workflow/transaction_impl.go`, and
  `temporal-server/service/history/workflow/mutable_state_impl.go`.
- SQL append-before-mutable-state-CAS:
  `temporal-server/common/persistence/sql/execution.go`.
- Cassandra append-before-mutable-state-update:
  `temporal-server/common/persistence/cassandra/execution_store.go`.
- Transaction-ID chain selection and orphan trimming:
  `temporal-server/common/persistence/history_node_util.go`,
  `temporal-server/common/persistence/history_manager.go`, and
  `temporal-server/common/persistence/execution_manager.go`.
- History-read bounds derived from mutable state:
  `temporal-server/service/history/api/getworkflowexecutionhistory/api.go` and
  `temporal-server/service/history/api/getworkflowexecutionrawhistoryv2/api.go`.
- Event application and final-state task refresh:
  `temporal-server/service/history/workflow/mutable_state_rebuilder.go` and
  `temporal-server/service/history/workflow/task_refresher.go`.
- Existing limits: `temporal-server/common/dynamicconfig/constants.go`,
  `temporal-server/common/primitives/constants.go`, and `temporal-server/common/rpc/grpc.go`.
