# Umpire example: a compositional Nexus model with auto-close

_Assessment date: 2026-08-15. Draft implementation reviewed at PR
[#11577](https://github.com/temporalio/temporal/pull/11577), head commit
[`1e77942647d323882cc4b050574ebb047c4e17da`](https://github.com/temporalio/temporal/commit/1e77942647d323882cc4b050574ebb047c4e17da)._

## Executive answer

Umpire is a good home for a model of Temporal's durable Nexus control plane, with operation
auto-close added as an extension of that model. It should not be one monolithic state machine or one
giant verification run. The useful unit is one `ModelFamily`: reusable operation, delivery, callback,
cancellation, timeout, routing, and caller-lifetime modules, projected into several bounded targets.
The two parts of the experiment still have different levels of readiness:

| Question | Feasible today? | What the result would mean |
| --- | --- | --- |
| Model the Temporal Nexus feature | **Yes, as a family of bounded protocol slices.** Current Umpire already models part of the operation lifecycle plus separate callback and integration targets; it needs a clearer Nexus-wide boundary and reusable connector modules. | Each projected abstract protocol satisfies its encoded properties for the target's finite bounds and assumptions. No individual projection establishes every Nexus guarantee. |
| Add auto-close | **Yes.** It should compose operation, cancellation, timeout, and caller-lifetime modules and add only policy capture plus close-trigger semantics. | The auto-close extension preserves the base Nexus invariants and satisfies its additional policy properties. |
| Express true eventual cancel delivery with fairness | **Not through the current Umpire IR/toolchain.** | The IR names progress properties, but every current backend plan rejects them as unsupported. A bounded/quiescent surrogate can be checked first. |
| Drive a Temporal test from the same conceptual model | **Partly.** Umpire already projects one `Protocol` into verification artifacts and live actions/rules. | The same declaration can define vocabulary, actions, and observations, but the live run is not currently checked as a refinement of `verify.Model`. |
| Automatically check the real implementation against the formal model | **No, not yet.** | Umpire needs a concrete-to-abstract observation adapter, cutpoint semantics, executable refinement mappings, and a trace/state conformance checker. |

The right near-term goal is therefore not “formally prove the implementation” or “put all of Nexus
in one model checker invocation.” It is:

1. define a compositional Nexus model family and find design bugs in small projections with bounded
   model checking;
2. add auto-close as a module and verify both its local rules and its interactions with base Nexus;
3. use the same canonical action/property vocabulary to generate model artifacts and live regression
   scenarios; then
4. add a refinement checker that rejects live traces which the abstract model cannot reproduce.

That would be a substantial increase in assurance while preserving honest boundaries. Umpire's own
assurance section already states those boundaries: partial observation is ambiguous, shared planners
and oracles can share a mistake, bounded progress is not eventuality, and generated formal models do
not verify Temporal's refinement of them ([`UMPIRE.md`](./UMPIRE.md#L202-L219)).

## Recommended Nexus modeling boundary

“The entire Nexus feature” needs a deliberate boundary. Nexus RPC itself is a synchronous HTTP
protocol on which arbitrary-duration operations are built. Start may return an inline result or an
operation token; async completion is delivered by callback, and cancel acceptance does not require
the operation eventually to become canceled
([Nexus RPC specification](https://github.com/nexus-rpc/api/blob/main/SPEC.md#overview),
[cancel semantics](https://github.com/nexus-rpc/api/blob/main/SPEC.md#cancel-operation)). Temporal adds
durable scheduling, retries, timeouts, endpoint routing, task dispatch, callback delivery, workflow
integration, and standalone operation APIs. The official architecture document locates those pieces
across the endpoint registry, outbound queue, Nexus operation/cancellation state machines,
callbacks, frontend, matching, and history
([`docs/architecture/nexus.md`](./docs/architecture/nexus.md#L98-L130),
[operation and callback machinery](./docs/architecture/nexus.md#L234-L443)).

The formal boundary should be **Temporal's durable Nexus control-plane semantics**:

- scheduling and the atomic hand-off from caller Workflow history;
- start request delivery, retry classification, sync and async responses, and correlation by logical
  request/operation identity;
- async completion callback attachment, retry, correlation, and terminal outcome;
- explicit and system-initiated cancellation, including its independent delivery lifecycle;
- schedule-to-close, schedule-to-start, and start-to-close timeout classes;
- workflow-backed and standalone hosting, caller close/reset/continue-as-new, and standalone
  describe/poll/cancel/terminate/delete behavior;
- endpoint resolution and routing isolation at the level visible to an operation; and
- handler Workflow/Activity links when they affect identity, completion, or lifetime.

The standalone API surface is not hypothetical: the CHASM service exposes start, describe, poll,
request-cancel, terminate, and delete operations
([service definition](./chasm/lib/nexusoperation/proto/v1/service.proto#L11-L40)). The model need not
encode protobuf layouts, but it should include their observable state semantics if “all Temporal
Nexus” is the claim.

Keep the following below or beside the protocol model: arbitrary payload serialization, exact HTTP
header bytes, protobuf validation, SDK language APIs, user handler business logic, authorization
policy details, quotas, metrics/UI, real retry delays, throughput, and database/transaction size.
Endpoint-registry serialization and replication also deserve their own subsystem target; the Nexus
core only needs a versioned `EndpointBinding` abstraction and resolution actions. Current Temporal
documentation explicitly says registry replication is not implemented for multi-cluster use, so
single-cluster endpoint consistency is an assumption unless a separate replication model is added
([architecture](./docs/architecture/nexus.md#L100-L128)).

This boundary follows the public lifecycle: synchronous operations complete during the start request;
asynchronous operations record `Started` and finish through a completion callback; automatic retries
provide at-least-once rather than exactly-once execution; and three distinct timeout classes constrain
different phases
([official Temporal Nexus lifecycle](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/nexus/nexus-operations.mdx#nexus-operation-lifecycle)).

## The feature that needs to be modeled

The proposal adds a per-operation policy with `ABANDON` as the default and `REQUEST_CANCEL` as the
opt-in behavior. A started handler should receive `CancelOperation` when its operation is forcibly
closed by its own schedule-to-close timeout, its caller workflow closes, or a standalone operation is
terminated/times out. Reset must not trigger the policy. Cancellation is detached so it can continue
after the operation/workflow closes, and system-initiated cancellation differs from explicit user
cancellation around timeout clamping and lifetime. The source design also calls out retries,
continue-as-new, a maximum of 2,000 pending operations, and the scheduled/start-ack race
([`.vscode/autoclose.md`](./.vscode/autoclose.md#L55-L149)).

This is a strong model-checking candidate because it combines:

- concurrent state machines (`Workflow`, `Operation`, `Cancellation`, and a remote handler);
- an event-sourced atomicity requirement (record the cancel request before closing the workflow);
- races between close, timeout, start acknowledgement, and cancellation delivery;
- crash/replay/reset behavior;
- retry and fairness assumptions; and
- lifetime inversion: a detached child must outlive a closed parent.

These are protocol properties rather than protobuf-shape or performance properties, which is exactly
Umpire's intended layer. Exact API encoding, authorization, payload handling, transaction size, and
throughput still need specialized tests.

## What the draft PR establishes

PR #11577 is an open draft with seven prototype commits. It demonstrates that the feature is
implementable on the CHASM path and already supplies valuable model inputs, but it is not the public
per-operation design yet.

### Implemented mechanics

- Auto-close creates a cancellation only when the operation is `STARTED` and has no existing
  cancellation. Workflow-backed operations event-source a cancel request; standalone operations
  create it directly
  ([`Operation.RequestCancelOnAutoClose`](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/nexusoperation/operation.go#L210-L233)).
- The `Cancellation` CHASM component is registered with `WithDetached`, allowing its tasks to run
  after the parent closes
  ([registration](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/nexusoperation/library.go#L48-L70)).
- `CancellationState.auto_close` distinguishes the system path
  ([proto](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/nexusoperation/proto/v1/operation.proto#L105-L129)).
  Auto-close cancellation skips the remaining-operation-time clamp, while explicit user
  cancellation retains it
  ([task handler](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/nexusoperation/cancellation_tasks.go#L124-L170)).
- Workflow close records `NexusOperationCancelRequested` before the close event and marks the
  resulting cancellation as auto-close
  ([workflow method](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/workflow/nexus_methods.go#L237-L324)).
- The operation's own schedule-to-close task requests cancellation before applying its timeout
  transition
  ([timeout task](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/nexusoperation/operation_tasks.go#L374-L418)).
- Functional tests cover explicit cancel, the main workflow close causes, workflow run/execution
  timeout, continue-as-new, operation schedule-to-close timeout, standalone terminate, and standalone
  timeout. These are useful acceptance tests, but they are hand-authored examples rather than traces
  generated from or checked by the formal model
  ([test file](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/tests/nexus_cancel_policy_test.go)).

### Prototype gaps that the model should make explicit

1. **No captured public policy yet.** The prototype reads one global dynamic-config value at close
   time, which retroactively changes every in-flight operation. Its own comment says production must
   capture the API value on each operation at schedule time
   ([`nexusclose`](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/service/history/api/nexusclose/nexus_close.go#L9-L47)).
2. **CHASM-only behavior.** If the mutable state has no CHASM workflow component, the workflow helper
   silently no-ops. Rollout/migration state therefore belongs either in the model's assumptions or in
   separate compatibility tests.
3. **Reset can change semantics.** `auto_close` is set after the cancel-request event is applied and is
   absent from the event attributes. Reset during the pending-delivery window reconstructs it as
   `false`, restores timeout clamping, and can prevent delivery. The draft documents this known
   limitation directly
   ([workflow method](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/chasm/lib/workflow/nexus_methods.go#L256-L267)).
4. **The start-in-flight race is real.** A skipped test shows the handler returning an async start
   token after the workflow has closed. The `STARTED` transition is then rejected, so detached
   `Cancellation` alone cannot send the cancel request
   ([skipped race test](https://github.com/temporalio/temporal/blob/1e77942647d323882cc4b050574ebb047c4e17da/tests/nexus_workflow_test.go#L3885-L3991)).
5. **Close fan-out is synchronous.** One history event and cancellation component are created per
   running operation in the workflow-close transaction, up to the default limit of 2,000. This is a
   bounded correctness problem but still a potentially large transaction; a small formal bound says
   nothing about production latency or transaction limits.

The reset and start-in-flight issues are particularly important: they are examples of bugs a model
can expose only if replay state and “HTTP start request in flight” are represented, rather than
collapsing everything into `SCHEDULED` or `STARTED`.

## A. Is Umpire suitable for formal modeling?

### Existing architecture is a good fit

Umpire already has the correct high-level seam: a compiled `Protocol` projects into a live runtime
declaration and into a finite `verify.Model` containing entity lifecycles, relations, actions,
properties, targets, bounds, and refinements
([protocol declaration](./tests/umpire2/internal/protocol/protocol.go),
[verification projection](./tests/umpire2/internal/protocol/verification.go)). The model generator
then emits TLA+, P, Ivy, and FizzBee from that common IR
([`generateTarget`](./cmd/umpire-genmodels/main.go#L122-L215)), and CI installs pinned tools, checks
generated artifacts, runs seeded counterexample tests, and runs smoke/nightly verification
([workflow](./.github/workflows/umpire-model-verification.yml#L38-L122)).

The canonical interpreter can enumerate enabled transitions, apply nondeterministic branches,
boundedly explore state space, and replay an abstract trace
([interpreter](./common/testing/umpire/verify/interpreter.go#L162-L260)). Native formal-tool
counterexamples are normalized by replaying them through that interpreter, preventing a backend trace
from being accepted unless it denotes a unique canonical transition and really violates the named
property
([counterexample normalization](./common/testing/umpire/verify/counterexample.go#L21-L63)). These are
excellent foundations for the proposed implementation bridge.

### Current Nexus coverage is useful but fragmented

The runtime `NexusOperation` entity already mirrors the main operation lifecycle: scheduled,
backing-off, started, sync/async success, failure, cancellation, timeout, standalone termination, and
synchronous rejection. It observes attempts, the three timeout details, cancel-request failures,
handler Workflow references, and public execution/history snapshots
([runtime model](./tests/umpire2/internal/model/nexus_operation.go#L20-L157),
[fact application](./tests/umpire2/internal/model/nexus_operation.go#L181-L308)). Live actions can
exercise standalone/embedded schedule, retryable start failure, sync/async response, callback
completion, timeout from scheduled/backoff, and standalone termination
([actions](./tests/umpire2/internal/action/nexus_actions.go#L15-L162)). This is a substantial base, not
an empty model.

Formal coverage is split across targets:

| Current target | What it checks | Important omissions |
| --- | --- | --- |
| `feature-nexus` | Operation lifecycle actions, workflow ownership/closure, and timeout evidence, bounded to one workflow and two operations. | No cancellation entity, start-delivery/handler task, callback, policy, reset, endpoint, or start-in-flight state. |
| `integration-callback-nexus` | Callback-to-operation/handler references, delivery retry/response consistency, and handler lifetime. | It is not composed with `feature-nexus` into one start-to-completion protocol. |
| `integration-nexus-activity` | Reciprocal Nexus Operation/Activity links and terminal refinement. | Link semantics only. |
| `protocol-atomic` | Generic lifecycle algebra from the canonical `Protocol`. | Callback integration actions are deliberately checked in separate targets; it is not an end-to-end Nexus target. |

The target list and manual family assembly are visible in
[`verification_family.go`](./tests/umpire2/internal/protocol/verification_family.go#L60-L177). The
callback integration already models reference, delivery, retry, acknowledgement, and lifetime as
separate modules
([`verification_callback.go`](./tests/umpire2/internal/protocol/verification_callback.go#L129-L221)).
This is the pattern to extend.

It is not yet a faithful callback-close model. `callback.delivery.enqueue` is guarded while the
handler run is nonterminal, while handler-close actions require already-created deliveries to be
acknowledged
([actions](./tests/umpire2/internal/protocol/verification_callback.go#L65-L126)). The real Temporal
contract triggers callback delivery from handler Workflow close and retries until success, permanent
failure, or retention expiry
([architecture](./docs/architecture/nexus.md#L337-L369)). The composed Nexus family must correct that
ordering rather than treating the current callback target as a finished reusable guarantee.

Cancellation is a larger missing slice. Runtime Umpire counts cancel-request failures but has no
canonical cancellation entity, origin/principal, request/delivery lifecycle, or formal action for
requesting cancellation. A terminal `NexusOperationCanceled` is an operation outcome, not evidence
that `CancelOperation` was requested or delivered. This distinction is required by the Nexus RPC
contract, which says an accepted, repeatable cancel request may still be followed by any operation
outcome
([specification](https://github.com/nexus-rpc/api/blob/main/SPEC.md#cancel-operation)).

The existing `feature-nexus` target nevertheless has a semantic mismatch for auto-close. Its
workflow-close action is guarded so that every owned operation is already terminal
([`verification_nexus.go`](./tests/umpire2/internal/protocol/verification_nexus.go#L64-L82)), and its
closure property says that a terminal workflow implies a terminal owned operation
([property](./tests/umpire2/internal/protocol/verification_nexus.go#L188-L201)). Auto-close permits
workflow close while a detached cancellation remains live. That property needs to be decomposed into
“the operation no longer belongs to the live caller” and “all required detached work remains valid,”
not merely weakened ad hoc.

### Recommended compositional Nexus model family

Use one family with narrow modules and explicit connector modules:

| Module/slice | Core entities | Principal actions | Representative properties |
| --- | --- | --- | --- |
| `nexus-operation-core` | `NexusOperation`, `HandlerExecution`, logical `StartRequest`/token identity | schedule, start accepted, sync result, async acknowledgement, terminal completion/failure/cancel, duplicate/late response | terminal outcome is monotonic; sync skips `Started`; async outcome is correlated to the acknowledged logical operation; retry never creates a second logical operation |
| `nexus-start-delivery` | `StartDelivery`, `Attempt`, endpoint/destination relation | enqueue, dispatch, retryable/non-retryable response, backoff, retry, circuit-breaker block/probe | at-least-once attempts preserve request identity; no cross-destination delivery; non-retryable results do not retry |
| `nexus-completion` | existing `Callback`, `CallbackDelivery`, `CallbackResponse`, handler `WorkflowRun` | attach, handler close, enqueue, retry, deliver, acknowledge, conflicting duplicate | completion refers to the right operation/handler; one accepted logical outcome; handler retry/continue-as-new retains the attachment |
| `nexus-cancellation` | `CancelIntent`, `CancelDelivery`, principal/origin | explicit request, enqueue, retry/backoff, handler accept, permanent failure/expiry | one logical intent; repeat `CancelOperation` is idempotent; accepting cancel does **not** force the operation outcome to be canceled |
| `nexus-timeouts` | finite `Deadline`/`TimeoutEvidence` classified as schedule-to-close, schedule-to-start, or start-to-close | arm, phase transition, expire, race with response | timeout type matches phase; no post-terminal timeout; total deadline dominates start/retry lifecycle |
| `nexus-caller-lifetime` | caller `Workflow`/`WorkflowRun`, ownership relation | cancel, complete, fail, terminate, run/execution timeout, continue-as-new, reset/replay | history/ownership ordering; caller cancellation creates explicit cancel intent; reset is replay, not a close-policy trigger; old-run operations are not transferred by continue-as-new |
| `nexus-standalone-api` | standalone root operation and waiter/visibility observation | start, describe, poll, request cancel, terminate, delete | describe/poll never report an impossible or regressing state; delete/terminate semantics match the root lifecycle |
| `nexus-endpoint-routing` | `EndpointBinding`, registry epoch, destination, handler task/poller | create/update/delete/resolve, dispatch, poll/respond | unique/current binding; no cross-namespace/task-queue delivery; stale lookup is explicit rather than silently accepted |
| `nexus-auto-close` | per-operation `AutoClosePolicy`; reuses cancellation and lifetime entities | capture policy, eligible operation/caller close, create system cancel | `ABANDON` creates none; `REQUEST_CANCEL` creates exactly one eligible system intent; detached delivery survives parent close; reset excluded |
| `nexus-links` | Operation↔Workflow/Activity link relations | attach/propagate link | reciprocal link identity and terminal refinement (largely present today) |

The delivery and routing slices should refine Umpire's existing generic foundation rather than
reimplement matching. It already models delivery tasks/attempts, route isolation, matching/history
owner fencing, and backlog acknowledgement in separate targets
([routing target](./tests/umpire2/internal/protocol/verification_delivery_routing.go#L32-L230),
[backlog target](./tests/umpire2/internal/protocol/verification_delivery_backlog.go)). What is missing
is a Nexus connector saying which start/cancel/callback state creates which generic delivery
obligation and how its response refines the feature state.

The Nexus RPC specification explicitly permits completion by callback before the start response has
been received by the caller, carrying token/start-time/link data for correlation
([callback requirements](https://github.com/nexus-rpc/api/blob/main/SPEC.md#callback-urls)). Therefore
`nexus-operation-core` and `nexus-completion` need an early-completion/late-start-response branch, not
only the auto-close start-in-flight race.

Connector modules should define the few cross-slice atomic steps—for example, “accepted async start
creates the operation-token relation,” “handler Workflow close enqueues callback delivery,” and
“caller close under auto-close creates a system cancel intent.” Leaf modules remain reusable and do
not reach directly into every other module's internal state.

### Auto-close state added by the extension

The smallest projection that still contains the known auto-close bugs uses:

| Entity | Essential abstract state |
| --- | --- |
| `Workflow` | `running`, terminal close causes, plus an explicit `reset`/replay action that is **not** a policy-triggering close |
| `NexusOperation` | `scheduled`, `start-in-flight`, `started`, terminal outcome; hosting = workflow-backed or standalone |
| `AutoClosePolicy` | captured `abandon` or `request-cancel`, related one-to-one to the operation |
| `CancelIntent`/`CancelDelivery` | requested, scheduled, backing off, delivered, permanent failure/expired; principal = user or system; detached lifetime |
| `HandlerExecution` | definitely not running, may be running, running, cancel observed |

Relations include `owns(workflow, operation)`, `policy-of(policy, operation)`,
`cancellation-of(intent, operation)`, and `delivers(delivery, intent)`. The projection can use one
workflow, one or two operations, and one cancellation per operation. It should be a separate
`feature-nexus-auto-close` **target for tractability, but not a separate model**: the target composes
the shared core, cancellation, timeout, caller-lifetime, and auto-close modules.

The IR currently supports finite entity states and relations, not scalar fields or counters
([IR](./common/testing/umpire/verify/model.go#L42-L205)). The first iteration can represent policy,
principal, timeout class, error class, and a coarse retry budget as small related entities. If the
same pattern repeats, add finite typed attributes; flattening every value into operation lifecycle
states creates a policy × timeout × retry × outcome product and obscures invariants.

The auto-close connector actions include schedule/capture policy; explicit user cancel; each eligible
workflow/standalone/operation close; atomic creation of system cancel intent before close-history
ordering; delivery retry/outcome; retention expiry; and replay/rebuild. The environment must choose
whether a start reached the handler just before close, whether its acknowledgement is delayed, and
whether cancel delivery fails transiently. Otherwise the model misses both the draft's known race and
the protocol's early-completion race.

### Properties and assumptions

Nexus-wide properties should be grouped by the contract they protect:

- **Identity/correlation:** every attempt, token, callback, cancel, link, describe response, and poll
  response refers to the intended logical operation; retries preserve identity.
- **Outcome consistency:** an operation has at most one accepted terminal outcome; duplicate matching
  completions are idempotent and conflicting completions are rejected; a sync result does not pass
  through `Started`.
- **At-least-once semantics:** retryable start/cancel/callback failures can produce repeated delivery
  attempts but not additional logical operations. Handler idempotency is an environment obligation,
  not an exactly-once claim. Temporal documents this contract explicitly
  ([execution semantics](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/nexus/nexus-operations.mdx#execution-semantics)).
- **Timeout correctness:** schedule-to-start expires only before start/sync completion,
  start-to-close only after async start, and schedule-to-close spans the entire operation. A terminal
  outcome and timeout cannot both win.
- **Callback correctness:** an early completion and late start response reconcile to the same
  operation; handler Workflow retry/continue-as-new preserves the callback attachment; accepted
  callback outcome reaches the caller exactly once logically.
- **Cancellation correctness:** cancel acceptance is idempotent and does not predict the eventual
  operation outcome; delivery respects origin-specific lifetime/deadline rules.
- **Routing/isolation:** an invocation goes only to the endpoint binding and handler task queue selected
  for its destination epoch; one failing destination/circuit breaker cannot corrupt another.
- **Caller/standalone observability:** history, describe, and poll are consistent with durable state;
  reset/replay reconstructs it; delete/terminate do not create impossible intermediate states.

The auto-close extension adds these safety properties:

1. `ABANDON` never creates a system cancellation.
2. `REQUEST_CANCEL` is captured at schedule/start and does not change when configuration changes.
3. Closing a workflow with a known-started owned operation atomically records exactly one system
   cancel intent before the workflow close event.
4. Reset does not create a fresh auto-close intent, and replay reconstructs the same principal/
   `auto_close` semantics.
5. A detached cancellation remains schedulable after its operation and workflow close.
6. User cancellation retains deadline clamping; system auto-close cancellation does not.
7. Operation schedule-to-close timeout may make the operation terminal, but necessary cancellation
   state remains until delivery/final resolution.
8. A definitely-not-started handler receives no cancel; an in-flight start that did reach the handler
   is eventually reconciled with a cancel intent.
9. Retries do not create a second logical cancellation, even though multiple delivery attempts are
   allowed.
10. Continue-as-new applies policy to the old run's operations and does not transfer their handles to
    the new run.

Progress must be conditional: if the endpoint is eventually reachable, task execution is fair, and
retention has not expired, a pending system cancellation is eventually delivered; otherwise it must
reach an explicit modeled terminal outcome. Those environment assumptions are part of the claim, not
implementation details to hide.

Today, Umpire can express the safety properties and a bounded “at quiescence, nothing remains silently
pending” approximation. It cannot express/check the actual temporal claim end-to-end. Although
`PropertyKind` includes `ProgressProperty` and properties carry fairness names
([IR](./common/testing/umpire/verify/model.go#L34-L40)), the toolchain explicitly marks every progress
property unsupported for SANY, TLC, Apalache, Apalache proof mode, P/PEx, Ivy, and FizzBee
([planner](./common/testing/umpire/verify/toolchain/internal/runner/toolchain.go#L113-L167)). The
canonical interpreter also evaluates only safety and quiescent properties; “quiescent” currently
means no model action is enabled
([interpreter](./common/testing/umpire/verify/interpreter.go#L263-L289)).

### Composition and state-space strategy

“One Nexus model” should mean one versioned family and vocabulary, not one flattened verification
target. Use three levels:

1. **Leaf targets** check one module's invariants with one or two identities: operation core,
   cancellation delivery, callback delivery, timeouts, endpoint binding, and standalone observation.
2. **Pairwise/feature compositions** close interface obligations at the important seams:
   operation+start-delivery, operation+callback, operation+cancellation, operation+caller-lifetime,
   endpoint+routing, and core+cancellation+lifetime+auto-close.
3. **One minimal end-to-end target** uses one caller, one endpoint, one handler, one operation, and at
   most one live start/callback/cancel delivery of each kind. It checks correlation and lifetime
   invariants across the entire sync path and the entire async path, but does not multiply every close
   cause, timeout class, retry budget, and routing failure in the same run.

Races such as early completion, late start acknowledgement, timeout-versus-result, and close-versus-
cancel deserve focused targets with two competing actions. Close causes that have identical abstract
effects can be one finite `CloseClass`; their distinct public/history plumbing remains live regression
coverage. Two operations are necessary only for identity/isolation/duplicate properties. This avoids
an operation × attempt × callback × cancel × workflow-run × endpoint Cartesian product while still
checking every cross-module contract.

Every target must publish its bounds, environment actions, fairness assumptions, abstractions, and
omissions. A green leaf target cannot substitute for the pairwise composition that closes its
assumption, and a green end-to-end target with one identity cannot substitute for a two-identity
isolation target.

### What Umpire composition supports—and still lacks

Umpire's family IR already has owned `Module`s, interfaces/obligations, refinement maps,
`Composition`s, and bounded `VerificationTarget`s
([family types](./common/testing/umpire/verify/family.go#L15-L105)). Projection unions selected module
declarations, follows imported obligations, closes entity/relation/action dependencies, and rejects
an undeclared omission that can affect retained state
([projection](./common/testing/umpire/verify/project.go#L41-L180)). That is sufficient to build the
target hierarchy above today.

However, a `Module` is currently a named list of declarations in one global model, and a
`Composition` is a union of names. There is no parameterized module instantiation, namespace/
renaming, per-instance bound, or generated synchronizing action. Nexus needs the same retrying
delivery shape for start, cancel, and callback, but copying it three times by hand invites drift.
Similarly, the current family is assembled by manually appending every fragment's entities,
relations, actions, properties, refinements, modules, compositions, and targets
([assembly](./tests/umpire2/internal/protocol/verification_family.go#L78-L177)); the Nexus fragments do
not yet expose Nexus-specific provider/consumer obligations.

The incremental fix should be a Go-level deep module before a new modeling language: a
`RetryingDeliveryModule(spec)` builder that emits namespaced declarations, interface obligations,
refinements, and standard safety properties for start/cancel/callback. Add small connector builders
for atomic cross-module actions and declare Nexus-specific interfaces such as `StartDelivery`,
`CompletionDelivery`, `CancellationDelivery`, `CallerLifetime`, and `EndpointResolution`. If several
features need scalar enum/counter values after this experiment, then add finite typed attributes and
parameterized module instances to the IR. The current state/relation encoding can start the work, but
it should not force all three delivery protocols into one undifferentiated entity type.

Other current gaps remain material:

- no clocks or arithmetic: deadlines and exponential backoff must be finite phases and nondeterministic
  expiry actions;
- no general data equality: request IDs, tokens, endpoints, and results must be represented by bounded
  identity entities/relations or a future finite-value facility;
- no temporal progress execution in any backend, as described above;
- no automatic assume-guarantee proof across targets—the composition target must actually be checked;
  and
- no live consumer for formal refinement maps, so module reuse does not itself establish
  implementation conformance.

### Backend choice

| Backend | Recommendation for this feature |
| --- | --- |
| **TLA+ with TLC/Apalache** | **Use first.** Umpire already generates it and pins the toolchain. TLA+ is explicitly designed for concurrent/distributed state machines, and TLC checks finite models; Apalache adds symbolic bounded checking. It naturally expresses nondeterminism, temporal properties, and fairness once Umpire's exporter/planner supports them. A successful bounded Apalache run is still only “no counterexample through the configured bound,” not an unbounded proof. See the official [TLA+ overview](https://lamport.azurewebsites.net/tla/tla.html) and [Apalache documentation](https://apalache-mc.org/docs/apalache/index.html). |
| **P/PEx** | Strong second experiment because communicating machines and monitors match the feature. P now advertises **PObserve**, which feeds service logs through the same P monitors for runtime conformance ([official P semantics](https://p-org.github.io/P/advanced/psemantics/)). That is directly relevant to part B. However, Umpire currently generates P for model checking only; it does not emit a PObserve event adapter/monitor pipeline. Evaluate this as a narrow spike, not as an already-present bridge. |
| **Ivy** | Useful independent safety backend and potentially attractive for inductive invariants/refinement. The current Umpire exporter intentionally supports inductive safety only and rejects quiescent properties. It does not solve cancel-delivery liveness. See [Ivy's official overview](https://microsoft.github.io/ivy/). |
| **FizzBee** | Keep as translation diversity and an approachable visualization/debug backend. Current Umpire smoke depth is capped at five for FizzBee, so it should not be the sole basis for a retry/race claim ([bounds](./common/testing/umpire/verify/toolchain/internal/runner/toolchain.go#L328-L343)). |
| **Dafny** | Not an existing Umpire backend. Dafny verifies programs written in Dafny against contracts, which does not verify the existing Temporal/CHASM Go implementation. It may later be useful for a small pure refinement-checker core if that core is authored in Dafny. See the official [Dafny reference](https://dafny.org/dafny/DafnyRef/DafnyRef). |
| **Lean or Agda** | Both can encode the transition system and prove theorems, but neither is an Umpire extension today. Connecting either to this PR requires formalizing the model, the relevant Go/CHASM semantics, observations, and a refinement theorem. That is much larger than the feature and would introduce a second proof stack. Reserve one for a specific theorem that bounded checking cannot discharge, not as the first integration. See the official [Lean reference](https://lean-lang.org/doc/reference/latest/) and [Agda introduction](https://agda.readthedocs.io/en/latest/getting-started/what-is-agda.html). |

TLA+ is therefore the pragmatic answer to “TLA+, Dafny, Lean, or Agda?” for part A. P/PObserve is the
most relevant adjacent experiment for part B. Multiple exporters can detect translation/backend
disagreements, but they do not make an incorrect common IR correct; model mutation and independent
property review remain necessary.

## B. Can the same model check the implementation?

### What can be shared today

Umpire's live side ingests observed facts into its runtime, records fact/transition/relation coverage,
evaluates runtime rules, and records execution action windows
([evidence ingestion](./tests/umpire2/evidence_ingestor.go#L32-L91),
[execution trace](./tests/umpire2/execution_trace.go#L54-L204)). Causal footprints can require or
forbid facts around a live action. This is enough to build model-informed acceptance scenarios today:
the same declaration can supply names, lifecycle edges, start/callback/cancel/close actions, and local
assertions to both a verification target and the live driver. Existing live facts already observe
operation history/CHASM transitions, public standalone snapshots, callback references, Workflow
runs, and Nexus/Activity links. What is missing is a unified abstract step stream, not all raw data.

That sharing is useful, but it is not yet “the implementation refines the model.” The live execution
trace records action names, facts, relations, and lifecycle edges; it does not carry canonical finite
model IDs and parameter bindings, nor does it normalize a runtime snapshot into `verify.ModelState`.
`verify.Refinement` has fields for lifecycle/regression actions, required/forbidden observations, and
stutter
([IR](./common/testing/umpire/verify/model.go#L186-L205)), but today those fields are inventory and
validation metadata. They are not consumed by a live conformance executor. Counterexample
normalization handles formal-backend traces only.

### Missing bridge

The bridge should be one deep `ConformanceRunner` module with a deliberately small interface:

```go
type ConformanceRunner interface {
    Run(context.Context, Experiment) (ConformanceResult, error)
}
```

`Experiment` is an immutable, protocol-compiled input containing the model identity, chosen abstract
trace or target, evidence requirements, and bounds. Identity binding, snapshot encoding, refinement
mapping, cutpoint selection, and successor checking belong behind this interface rather than becoming
author-facing seams. The Temporal session belongs behind an adapter because a local functional-test
cluster, CI cluster, and deployed cluster are genuinely different implementations of the same
remote-owned boundary. This is consistent with the one-source design in
[UMPIRE_STARLARK.md](./UMPIRE_STARLARK.md) and the runner direction in
[UMPIRE_UX.md](./UMPIRE_UX.md).

It needs the following pieces:

1. **Stable identity and binding adapter.** Map endpoint, workflow/run, operation, start-attempt,
   callback, cancellation, and delivery identities to the bounded target's canonical entity IDs and
   capture action parameter bindings.
2. **Concrete-state abstraction.** Convert an Umpire runtime view at a cutpoint to `verify.ModelState`,
   including endpoint binding, start-in-flight/started knowledge, callback/cancellation delivery,
   timeout class, captured auto-close policy/principal, and the cross-module relations selected by
   that target.
3. **Cutpoint and macrostep semantics.** Define when the distributed implementation is stable enough
   to compare. One abstract atomic action may cover a history transaction plus asynchronously
   observed tasks; internal implementation events must be explicitly classified as stutter.
4. **Executable refinement mappings.** For each abstract action, identify the live driver action,
   required/forbidden observations, permitted concrete stutters, and allowed nondeterministic branch.
5. **Successor checking.** At each cutpoint, require the normalized concrete state to be one of the
   successors returned by `Interpreter.Step`; do not compare only final states.
6. **Observation qualification.** Missing endpoint-resolution, matching-task, CHASM snapshot,
   history, outbound-call, callback, or handler evidence required by the selected module must produce
   `unsupported`/`inconclusive`, never a pass.
7. **First-class boundary observations.** Independently observe `StartOperation`, handler task
   acceptance/response, completion callback, and `CancelOperation` receipt. Inferring delivery solely
   from the same history/component state that drives the oracle would miss outbound-loss,
   misrouting, and wrong-handler bugs.
8. **Trace promotion.** Translate a canonical counterexample into a Umpire regression plan (or report
   precisely which abstract action lacks a live realizer), then retain the completed semantic trace
   rather than pretending a random seed reproduces a distributed schedule.
9. **Progress monitoring.** Add time/fairness-aware monitor semantics or integrate an external runtime
   monitor such as PObserve. A finite test timeout is evidence of bounded delivery, not proof of
   eventual delivery.

The intended flow would be:

```text
                              -> projected target -> TLA+/P/Ivy/Fizz -> evidence/counterexample
Canonical Nexus ModelFamily --|
                              -> abstract trace -> live Umpire plan -> Temporal cluster
                                                    |
                                                    v
verify.Interpreter <- normalized model steps <- runtime trace + snapshots + handler evidence
```

This is reuse of one model with an explicit refinement relation, rather than using the model as both
driver and unquestioned oracle.

### Realistic vertical slices

Start with the ordinary async-success path because every extension depends on its identities:

1. The interpreter executes schedule, start-delivery enqueue/dispatch, async acknowledgement,
   callback attachment, handler close, completion delivery, and terminal caller outcome.
2. Mapped live actions run one caller Workflow and one handler Workflow through a real endpoint.
3. Umpire independently observes endpoint resolution, Nexus task receipt/response, caller history,
   callback delivery, and the final public state.
4. At every cutpoint the normalized concrete state must equal an allowed interpreter successor; the
   operation token/callback/handler relations must stay correlated.

Then connect the auto-close extension using one started `REQUEST_CANCEL` operation and workflow
termination. The abstract close step creates a system cancel intent and closes the workflow; the live
run must observe cancel-request history before close, detached cancellation state, and independent
handler receipt. Add `ABANDON`, explicit user cancel, operation timeout, continue-as-new, transient
retry, reset/replay, and the scheduled/start-in-flight race only after those two vertical paths work.
This ordering separates base-model/conformance bugs from auto-close bugs.

## Missing model, product, and test capabilities

Before the family can be called a model of the whole durable Nexus protocol, Umpire needs:

- first-class cancellation request/delivery, plus a callback lifecycle connected correctly to
  handler close and operation completion rather than only to callback identity;
- explicit early-completion/late-start, timeout/result, and endpoint-resolution races;
- Nexus-specific interfaces and connector modules, plus a validated fragment-merging builder to
  replace manual family assembly;
- formal coverage for the standalone describe/poll/request-cancel/delete surface;
- an invocation/cancel/callback refinement into the existing generic delivery/routing foundation;
- target/property/action coverage accounting so that a family-level “covered” claim names the
  projection that checks each guarantee; and
- the live observations and conformance bridge described above.

The Umpire bridge cannot compensate for missing product semantics. Before treating this as a
conformance gate for auto-close, the draft implementation still needs:

- public API fields and per-operation persisted policy capture for workflow-backed and standalone
  operations;
- a durable, event-sourced system principal/auto-close marker so reset preserves behavior;
- a defined and implemented resolution to the start-in-flight race;
- deliberate migration/compatibility behavior outside CHASM;
- a clear terminal retry/retention policy and observable final outcome;
- system-worker/offload or evidence that inline fan-out remains safe at realistic cardinality; and
- handler-side or outbound-boundary observations usable by Umpire without retaining payloads or
  secrets.

Umpire also lacks live realizers/fault controls for several relevant workflow close and timeout
paths. The default protocol currently inventories workflow failure, cancellation, termination RPC,
and server timeout as gaps, and inventories started schedule-to-close timeout as a Nexus gap
([default protocol gaps](./tests/umpire2/internal/protocol/default.go)). Those gaps should remain
explicit: a formal action without a realizer is model-only coverage, not implementation coverage.

## Recommended incremental path

### Phase 0: inventory the durable contract and settle open auto-close semantics

Create a coverage ledger from public/CHASM actions and guarantees to model modules, formal targets,
live actions, observations, and specialized tests. Resolve the draft's per-operation policy storage,
reset persistence, and start-in-flight behavior while keeping its focused tests. Open choices can be
represented as competing model variants during design, but the checked target must name the selected
semantics.

### Phase 1: make the current Nexus fragments reusable

Introduce validated family/fragment merging and a namespaced `RetryingDeliveryModule` builder. Split
the current `feature-nexus` declarations into operation-core, timeout, and caller-lifetime modules;
declare Nexus-specific interfaces; and correct the callback close-trigger ordering. Preserve existing
target names as compatibility projections until generated artifacts and CI move deliberately.

At this phase, add leaf targets for operation core, timeout, cancellation, callback, and standalone
observation. Use TLA/TLC/Apalache plus Ivy safety as the primary gates and retain P/FizzBee for
translation diversity. Add mutations for duplicate outcome, wrong identity, missing retry, wrong
timeout phase, and callback-before-start.

### Phase 2: compose the base Nexus protocol

Add pairwise targets for operation+start delivery, operation+callback, operation+cancellation,
endpoint+routing, and caller lifetime. Then add the minimal sync and async end-to-end targets. Use two
identities only for isolation, duplicate, or shared-handler properties; use one elsewhere. Record
unbounded retry/liveness as an explicit omission and check only bounded/quiescent proxies today.

Extend live Umpire in parallel with cancellation/delivery entities, endpoint and handler boundary
facts, all three timeout classes, standalone control actions, and early-completion reconciliation.
Preserve protobuf, authorization, registry-internal, and scale tests outside the protocol model.

### Phase 3: add auto-close as an extension target

Compose the existing core, cancellation, timeout, and caller-lifetime modules with a small
`nexus-auto-close` module. Use one workflow, one operation/cancel delivery, one policy, and one close
class; add a separate two-operation fan-out/identity target. Cover `ABANDON`, explicit user cancel,
eligible force-close classes, operation timeout, standalone close, continue-as-new, reset, transient
retry, retention expiry, and the in-flight start race.

Add model mutations that remove detachment, event ordering, policy capture, reset persistence, or
late-start reconciliation and demonstrate a failure. This makes auto-close an extension of verified
base contracts rather than a parallel operation model.

### Phase 4: implement the pure refinement checker

Build and unit-test the identity adapter, target-specific runtime-to-model snapshot encoders, cutpoint
reducer, executable refinement map, and successor checker without a cluster. Test nondeterministic
branches, declared stutters, ambiguous observations, out-of-bound identities, missing evidence, and
incorrect intermediate states. A pass is qualified by family/target/model hash, observation profile,
bounds, assumptions, and mapped actions.

### Phase 5: connect vertical traces and counterexamples

Run ordinary async completion first, then the auto-close termination slice. Add counterexample-to-
regression promotion and the remaining pairwise seams. Consider a small PObserve spike here: determine
whether Umpire's canonical events can feed generated P monitors without creating a second
hand-maintained specification.

### Phase 6: strengthen temporal and implementation assurance

Extend the IR/exporters and canonical semantics for temporal progress/fairness, or deliberately make
P monitors the liveness layer. Add crash/replay/failover and model/implementation mutation campaigns.
Only after this bridge is trustworthy should stronger inductive proofs or a Dafny/Lean/Agda component
be considered for a concrete proof obligation.

## Tradeoffs and failure modes

- **State explosion:** operation × start attempts × callback × cancel delivery × timeout × endpoint ×
  caller lifetime already grows quickly before policy and close cause are added. Use leaf/pairwise/
  minimal-E2E targets, symmetry, one identity by default, and explicit connector invariants. Do not
  silently drop early completion, in-flight start, or reset merely to make checking fast.
- **False compositional confidence:** current module selection is not proof reuse. A leaf guarantee is
  valid in a composition only when its environment obligation is closed and the composed target is
  checked. Maintain a property-to-target coverage ledger.
- **Reusable-builder drift:** generated start/cancel/callback delivery modules can share a generator
  bug. Mutate each instance, inspect emitted target closures, and retain at least one independent
  end-to-end property per delivery kind.
- **False confidence from bounds:** smoke/nightly profiles bound depth and schedules; an empty bounded
  counterexample set is not global correctness. Publish bounds, fairness, omissions, and the actual
  backend status with every result.
- **Incomplete observation:** several real executions can map to one abstract state. Missing handler
  delivery, policy capture, or replay evidence must make the run inconclusive. Never interpret
  absence of an observed cancel as proof of `ABANDON` without a complete observation window.
- **Shared-model oracle bugs:** a planner and checker derived from one declaration can agree on the
  same mistake. Keep independent handler evidence, property review, model/implementation mutation
  tests, and multiple backend translations.
- **Distributed flakiness:** a live test cannot reproduce an exact scheduler trace. Reproduce semantic
  actions/cutpoints, retain actual observations, and report when the desired race was not achieved.
- **Crash/replay:** the PR's non-event-sourced flag already shows why in-memory final-state tests are
  insufficient. Restart/reset/failover must occur between cancel intent and delivery.
- **10× load:** a two-operation model can validate “one cancel intent per operation” but cannot
  validate a 2,000-event close transaction. Add transaction-size, latency, queue-backlog, and system
  worker/offload tests separately.
- **Security/privacy:** runtime monitoring should emit stable event names, opaque IDs, and principal
  class, not request payloads, headers, tokens, or secrets. Authorization and the integrity of the
  system/user principal require specialized tests below the abstract protocol.
- **Translation drift/tool failure:** use the canonical interpreter to normalize traces, pin and hash
  tools, fail on unsupported/inconclusive results, and retain artifacts. Multiple backends reduce but
  do not eliminate common-IR mistakes.

## Conclusion

Umpire is suitable for a bounded model of Temporal's durable Nexus protocol and already contains much
of the operation lifecycle, callback, relation, target-projection, formal-backend, and live-evidence
scaffolding. What exists is fragmented rather than complete: cancellation is not a first-class model,
the current callback target orders delivery before its real close trigger, start/callback/routing are
not joined end to end, and no live execution is checked as a refinement of `verify.Model`.

The right architecture is one compositional Nexus `ModelFamily`, several small verification targets,
and a thin minimal end-to-end projection. Auto-close is a module that imports operation,
cancellation, timeout, and caller-lifetime contracts and adds policy-qualified close actions. A
dedicated auto-close target remains desirable for state-space control; it must not fork the base
operation/cancellation model.

TLA+ remains the least-cost first formal backend. The main missing implementation-assurance work is a
clear, testable conformance module: observe concrete state at explicit cutpoints, map it to the chosen
target's canonical state/actions, and make every live macrostep replayable by the interpreter. Pair it
with independent endpoint/handler/callback/cancel evidence and honest bounded claims. PObserve is
worth a focused experiment because it targets the same design-to-runtime gap, but it is not currently
wired into Umpire. Dafny, Lean, and Agda should wait for a proof obligation that the TLA+/P/Ivy path
and refinement monitor cannot address economically.
