# CHASM Property Testing: Phase 4 Durable Contract

## Objective

Phase 4 makes CHASM's durable-execution contract explicit and testable without
turning `chasmtest.Engine` into a second History service. A CHASM component
must be correct under every delivery schedule permitted by the contract:
duplicate, stale, delayed, independently reordered, and recovered after a
durable commit whose caller did not observe the result.

History continues to own distributed execution: persistence transactions,
shard ownership, queue implementation, acknowledgement, DLQ handling,
replication transport, failover, and service routing. CHASM owns the component
semantics that those facilities must preserve.

## Contract Boundary

The framework must define and enforce the following guarantees.

| Concern | CHASM contract | History implementation |
| --- | --- | --- |
| Transition identity | A committed transition has a stable execution identity and monotonic transition identity. | Persists and replicates the identity with the execution. |
| Task identity | A task identifies its execution, transition/generation, component path, and task type. | Queues, polls, acknowledges, retries, and DLQs the task. |
| Delivery | A handler may receive a task late or more than once; stale work is detectable and safe to drop. Ordering is guaranteed only where the framework declares a causal dependency. | Selects partitions, worker concurrency, and the actual delivery order. |
| Durability boundary | State publication and emitted-task identity are atomic from a component's point of view. A lost response or acknowledgement does not permit an invalid second transition. | Implements the database transaction, cache/lease protocol, and error recovery. |
| Reload and compatibility | Snapshot/reload preserves the durable state and validates component/task compatibility. | Loads persistence records and coordinates migrations and replication. |
| Replication | A replicated transition can be applied deterministically, idempotently, and with a defined version/conflict rule. | Transfers records, chooses active/standby ownership, resolves cluster routing, and retries transport. |

The framework must not imply global FIFO delivery, exactly-once handler
execution, replica transport ordering, or shard-level atomicity. Those are
History guarantees and must be tested in History integration suites.

## Framework Changes

### Delivery-driver contract

`chasmtest` exposes replayable delivery as an opt-in API. A `DeliveryRef`
identifies a physical task delivery, a `DeliveryReceipt` records its outcome,
and `DeliveryTrace` is JSON-serializable. A first delivery is permitted only
when the task is due and is the physical queue head of its category. The host
may choose any eligible category head; it does not imply global FIFO ordering.

After a successful first delivery the host retains the physical task identity,
including across `ReloadExecution`, solely so a test can call `Redeliver`.
Redelivery can therefore exercise stale and duplicate handler behavior without
pretending that History acknowledgement or queue retention is implemented by
the host. `RunnableTasks`, `ExecuteTask`, and `DrainTasks` remain the
deterministic conventional-test APIs. A pure task is one opaque physical batch
tick: delivering it runs the due logical pure work selected by `Node`, not an
individually addressable logical task.

The RPC scenario script records invocation sequence and matches exactly one
outstanding typed predicate. It reports unmatched and ambiguous calls, and
releases its mutex before invoking a responder. FIFO is represented only by a
test's own matcher predicates.

### 1. Durable task protocol

Define a production-owned task contract shared by CHASM task creation,
validation, and the test host. The contract must expose the execution key,
component path, task type, transition/generation identity, and the minimum
causal predecessor required for a task to run. Avoid test-only copies of this
logic.

Task validation has three explicit outcomes:

- executable: the task belongs to the current durable transition;
- stale/no-op: the task was superseded, duplicated, or targets a removed
  component and is safe to acknowledge;
- invalid: the task violates the protocol and is surfaced as a framework or
  persistence invariant failure.

Handlers must be idempotent for duplicate delivery. A transition must never
depend on a queue's incidental ordering across independent task keys.

### 2. Constrained adversarial test host

Replace the current single deterministic drain order with a delivery scheduler
that enumerates or generates only schedules allowed by the durable task
protocol. It must support:

- selecting any causally-ready task, including tasks from distinct categories;
- retaining and redelivering an acknowledged or unacknowledged task;
- delaying a task while allowing independent work to advance;
- snapshot/reload before and after task handling;
- injecting a failure before commit, after durable commit but before the caller
  observes success, and after handling but before acknowledgement;
- recording each choice in a replayable trace.

The host remains a test facility. It may model a committed snapshot and task
set, but must not simulate shard ownership, persistence engines, queue
partitions, or replication transport.

Keep one deterministic default strategy for conventional unit tests. Property
tests opt into adversarial scheduling explicitly and set bounds on deliveries,
reloads, and pending tasks. Exhausting a bound is an inconclusive test failure,
not evidence of correctness.

### 3. Model-oracle process

Use one trace vocabulary for model and system-under-test execution:

```text
command | time advance | dependency outcome | deliver task | reload | fault point
```

Models describe externally observable behavior and emitted side-effect intent;
they must not mirror CHASM node layout, protobuf payloads, queue internals, or
the production helper algorithm being tested. Normalize SUT observations to
the same vocabulary, then compare after each command and each causal
quiescence point.

Every failure produces a durable replay artifact containing the generator seed,
the minimized trace, selected RPC outcomes, task identities, and normalized
model/SUT observations. Keep a regression corpus of fixed traces. Triage each
counterexample as one of:

1. product bug;
2. model/oracle bug;
3. test-host divergence from the CHASM contract; or
4. unsupported History-level behavior.

The owner of the component contract resolves this classification. A property
test is not accepted merely because its model and SUT agree.

### 4. RPC test boundary

Keep descriptor-backed generation as a transport-structure primitive only. It
can produce marshalable messages, but cannot determine valid business inputs,
authorization, idempotency semantics, or meaningful dependency states.

Replace a globally FIFO RPC script with a scenario matcher:

- match requests by a declared correlation key or predicate;
- permit explicitly concurrent in-flight calls;
- never hold the script mutex while executing a responder;
- report unmatched and ambiguously matched requests with the full trace;
- retain FIFO only where a test deliberately asserts one causal call sequence.

Each component owns typed semantic profiles for its dependencies, such as
`started`, `already-started`, `retryable-before-commit`,
`committed-response-lost`, and `terminal`. Generic RPC tooling supplies
recording, matching, fault timing, and replay; it does not centralize service
semantics. Streaming RPCs remain out of scope until a consumer supplies a
separate contract.

### 5. Production contract suites

For every framework guarantee, add a side-by-side suite that runs the same
small trace against the constrained host and the practical production History
fixture. Where a production fixture cannot cover a guarantee, call the same
production-owned pure validation helper from both paths and document the
boundary.

History integration suites own proof of persistence transaction failures,
queue acknowledgement/DLQ behavior, shard failover, active/standby routing,
and replication transport. CHASM suites own proof that components tolerate the
resulting permitted schedules.

## Rollout

1. Write the durable task protocol and classify every existing `chasmtest`
   behavior as shared, constrained test-host behavior, or unsupported.
2. Extract shared task validation and identity logic to production-owned code;
   delete any duplicate implementation from the host.
3. Add the trace recorder and deterministic replay before adding new generated
   schedules or RPC behavior families.
4. Convert one scheduler idempotency/reload/retry property to the new host and
   prove it catches a seeded mutation.
5. Add the matching RPC script and migrate only the scheduler methods required
   by that property.
6. Add side-by-side contract tests, then promote the narrow, demonstrated
   abstractions for use by a second CHASM library.

Run a small fixed-seed campaign and the replay corpus on pull requests. Run
bounded adversarial campaigns, race detection, and production-fixture contract
suites in nightly CI. Persist any new minimized counterexample before fixing
the product or oracle.

## Acceptance Criteria

- A CHASM task has a documented identity, causal-ordering, stale-delivery, and
  duplicate-delivery contract used by production and tests.
- The host can reproduce a failing trace without relying solely on a Rapid
  seed or an incidental drain order.
- At least one scheduler property covers duplicate delivery, reload, and a
  post-commit lost-response fault using the same trace vocabulary as its model.
- The RPC test script supports correlated concurrent calls and does not invoke
  user responders while holding its lock.
- No CHASM test claims to validate shard ownership, global queue ordering, or
  replication transport; the corresponding History integration suite is named
  in the test plan.
- A second CHASM library can adopt the durable task protocol without importing
  scheduler behavior or scheduler model types.

## Trade-offs and Non-goals

Adversarial scheduling increases runtime and produces more complex failures,
so campaigns are bounded and traces are minimized. A narrower framework has
less apparent reuse than a full in-memory History simulator, but prevents
semantic drift and keeps production ownership clear. Correlation-based RPC
matching costs more setup than FIFO scripts, but is necessary to expose
concurrency and retry races.

Phase 4 does not build a generic distributed-systems simulator, a replacement
for History integration tests, streaming-RPC fuzzing, or a universal semantic
generator for protobuf APIs.
