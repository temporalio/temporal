# CHASM Property Testing

## Execution baseline

Phase 1 implementation is on `chasm-property-test-harness`, created directly from `upstream/main`. The exploratory `prop-sch-chasm` branch remains research material only.

At execution time:

1. Fetch the latest `upstream/main` from `temporalio/temporal`.
2. Create a new, purpose-named branch directly from that commit. A separate worktree is acceptable when local `main` is already checked out elsewhere.
3. Verify the new branch's merge base and initial tree match `upstream/main`.
4. Use `prop-sch-chasm` only as research material. Port deliberately selected behavior and tests; do not merge or bulk cherry-pick the exploratory stack.
5. Implement and review Phase 1 before adding the Phase 2 scheduler consumer.

### Agent execution strategy

Use one Sol-class agent as the continuous owner and integrator for work that changes architectural boundaries, shared interfaces, semantic models, or test oracles. A clean agent may take over between reviewed commits, but only one agent owns the active design decision and final integration at a time.

Use Terra-class agents for bounded work after the owning Sol agent has written the relevant contract and acceptance criteria. Suitable delegated pieces include:

- read-only inventories of production/test behavior and call sites;
- mechanical removal of an already-rejected helper and updates to its direct callers;
- isolated contract tests for an already-decided supported or unsupported behavior;
- deterministic scheduler regression cases with a specified invariant and expected failure;
- populating audit-matrix evidence from existing finding records;
- generated-code plumbing or repetitive method-profile tests after the Phase 3 interface is fixed;
- focused test, race, lint, and diff verification.

Do not delegate these decisions to independent parallel agents:

- the supported `chasmtest.Engine` contract;
- whether production behavior should be shared, limited, or unsupported;
- scheduler model vocabulary or oracle semantics;
- bug-audit contract decisions;
- Phase 3 descriptor/profile architecture;
- final integration, simplification review, and commit scope.

Agents share one worktree, so parallel editing is allowed only for explicitly non-overlapping files after the owner records the interface they target. Read-only investigations may run in parallel. The Sol owner reviews every delegated diff, resolves cross-cutting changes, runs the complete verification set, and creates the phase commit. Delegated agents do not commit independently and never stage or commit this plan document.

Recommended phase allocation:

```text
Phase 1A
  Sol: engine contract, divergence dispositions, API reduction, integration
  Terra: read-only inventories, mechanical caller updates, decided contract tests

Phase 2
  Sol: scheduler model/oracles, audit classification, cross-suite integration
  Terra: isolated property families and deterministic regressions with fixed invariants

Phase 3
  Sol: descriptor boundary, semantic profile design, shrinking model
  Terra: generated plumbing, individual method profiles, repetitive compatibility tests
```

Prefer one Sol agent without delegation when the next change crosses `test_engine.go`, `task_driver.go`, RPC support, and scheduler tests simultaneously. Delegation is an optimization for separable work, not a reason to split coupled reasoning across agents.

### Implementation status

Phase 1 has an implemented prototype, but it is not yet accepted as the final reusable surface. Review found that `chasmtest.Engine` both predates this work and duplicates production-engine behavior in ways that can drift. It currently differs from the production engine in request-ID defaults, `TerminateExisting`, deletion, polling cancellation, reference consistency, task-generation validation, and persistence/shard behavior. The plan therefore pauses additional scheduler coverage for an alignment and reduction pass.

Phase 2 is complete. The independent `scheduler/model` covers pause and update operations, automatic and manual starts, all overlap policies, workflow completion, classified start failures and retries, action limits, bounded time advancement, recent-action retention, buffer limits, and bounded historical backfill effects. It is exercised with table tests, bounded exploration, and Rapid traces.

The real scheduler property environment uses a fresh registry and engine per case, generated frontend and history mocks, typed RPC scripts, normalized snapshots, and composable delivery, time, redelivery, reload, pause, unpause, trigger, completion, backfill, and delete operations. Current suites cover model-to-CHASM conformance, all overlap policies, retry and stale-redelivery idempotency, reload preservation, generator deadlines, inclusive backfill bounds, deletion, migration failure and retry, and callback recovery. `make chasm-scheduler-property-test` runs the bounded campaign intended for standard CI.

Phase 1A narrows and documents the supported in-memory contract, removes premature helpers, and adds contract checks for behavior that is intentionally shared with production. Phase 2 adds an explicit Schedule V2 bug-audit capability matrix and detection criteria. Phase 3 adds descriptor-driven request/response and RPC-behavior generation so individual use cases select relevant behavior families instead of manually scripting every outcome.

## Phase 1A: Alignment and Reduction

### Objective

Make the harness a thin in-memory host for real CHASM tree and task semantics, not a second implementation of the history CHASM engine. Reuse production code where it can be separated cleanly; explicitly limit or reject behavior that depends on persistence, shards, workflow caches, queue processors, or service routing.

No further Phase 2 feature coverage lands until this pass is reviewed.

### Production-alignment boundary

The harness must continue to execute the real shared CHASM mechanisms:

- `chasm.Node.CloseTransaction`;
- `chasm.Node.EachPureTask`;
- `chasm.Node.ValidateSideEffectTask`;
- `chasm.Node.ExecuteSideEffectTask`;
- snapshot serialization and `chasm.NewTreeFromDB` reconstruction;
- registered production component and task handlers.

The harness may implement only the in-memory facilities needed to host those mechanisms:

- execution storage and lookup for isolated tests;
- deterministic event time;
- immutable task inspection;
- controlled delivery, retained delivery, and reload;
- atomic publication of a successful working tree;
- narrow backend capabilities supplied explicitly by a consumer.

It must not claim production equivalence for:

- shard ownership, workflow cache leases, failover, replication, or persistence errors;
- production queue ordering, acknowledgement, DLQ, or concurrent processor behavior;
- request-ID generation and error conversion;
- delete-task generation and asynchronous persistence deletion;
- reference-consistency reload behavior or task-generation shard clocks;
- zombie workflow, active/standby, namespace routing, or visibility behavior.

For every `chasm.Engine` method, the pass records one of three dispositions: shared behavior, intentionally limited test behavior, or unsupported. An unsupported behavior returns a descriptive error rather than a plausible but different result. In particular, the pass resolves the current differences in `TerminateExisting`, deletion, polling cancellation, missing request IDs, and start references with explicit run IDs.

Where production behavior is pure and reusable, extract it to a production-owned helper and call it from both engines. Do not move shard or persistence orchestration into `chasm` merely to make the test engine look similar.

### Reduction targets

- Remove `rapidtest.ActionMap` and weighted duplicate action names unless a second consumer demonstrates a need that Rapid cannot express directly.
- Keep Rapid-specific execution orchestration scheduler-local until a second CHASM library needs the same API; retain only generic operations with two demonstrated consumers or an essential generic contract.
- Remove public operation-history access and arbitrary retained-delivery limits unless they materially improve minimized failures.
- Replace the unrestricted `WithNodeBackendDecorator` escape hatch with the narrowest explicit backend capability needed by consumers, or document why a generic backend factory is necessary.
- Simplify `rpctest.Script` to the concurrency and recording semantics exercised by real consumers. Do not retain in-flight ordering machinery solely for hypothetical concurrency.
- Remove trivial scheduler helpers where direct standard-library or local expressions are clearer. Preserve scheduler API helpers that form the test vocabulary, such as describe, update, trigger, backfill, completion, and migration.
- Keep the scheduler model limited to observable semantics. It must not mirror CHASM component layout, task payloads, or internal helper algorithms.

### Contract verification

- Add side-by-side contract cases for the supported subset of the in-memory and production engine fixtures where a production fixture is practical.
- Otherwise test through the same production-owned pure helper or `chasm.Node` method and document the omitted history-layer behavior.
- Add negative tests proving unsupported behavior fails explicitly.
- Retain rollback, stale delivery, redelivery, task replacement, and reload tests only where they exercise real shared `chasm.Node` semantics.
- Run focused tests with `-tags test_dep`, scheduler tests, race where appropriate, and `make lint-code`.

### Exit criteria

- The `chasmtest.Engine` comment describes a supported subset and no longer promises broad production equivalence.
- Every known engine difference has an explicit disposition and test.
- Shared packages expose no scheduler concepts and no speculative API justified only by a future SAA consumer.
- Removed helpers leave the scheduler property tests at least as readable and diagnosable.
- Phase 2 tests pass without depending on behavior declared production-only.

## Phase 1: Reusable Test Infrastructure

### Objective

Build a component-agnostic test harness that can drive real CHASM component trees and their registered tasks deterministically. The same infrastructure must support conventional table tests and Rapid state-machine tests without containing scheduler-, activity-, or other library-specific behavior.

Phase 1 establishes the reusable execution substrate. Component models, business invariants, and property campaigns are deferred to later phases.

### Goals

- Execute pure and side-effect CHASM tasks through the handlers registered with a `chasm.Registry`.
- Expose deterministic control over task delivery, redelivery, time, reloads, and failures.
- Reuse production `chasm.Node` transaction and task semantics for rollback, retries, stale tasks, and transition ordering.
- Allow each Rapid-generated case to create an isolated engine with its own logger, mocks, and cleanup.
- Let consumers compose Rapid actions without creating a second property-testing framework.
- Provide deterministic response scripting that plugs into generated gRPC client mocks.
- Keep the harness independent of every CHASM library.

### Non-goals

- Defining scheduler, standalone activity, Nexus operation, or workflow models.
- Implementing standalone-activity behavior or matching-service semantics.
- Defining business-level generators or invariants.
- Providing component-specific library factories or mocks.
- Running a high-volume property campaign in required CI.
- Simulating persistence, shard ownership, or concurrent production task processors in full.
- Reimplementing history-engine lifecycle, routing, deletion, polling, or error-conversion behavior.
- Generating RPC request/response domains; that belongs to Phase 3.

### Package structure

```text
chasm/chasmtest/
  test_engine.go
  task_driver.go
  task_driver_test.go

chasm/chasmtest/rpctest/
  script.go
  script_test.go
```

`chasmtest` remains usable without Rapid. Rapid orchestration stays in the consuming library until another CHASM library demonstrates an unchanged reusable surface. The `rpctest` subpackage has no Rapid or CHASM-library dependency; in Phase 1 it records typed protobuf RPCs and returns deliberately scripted outcomes through generated mocks.

### Engine construction

`NewEngine` should accept the minimal testing interface implemented by both `*testing.T` and `*rapid.T`:

```go
func NewEngine(
	t testlogger.TestingT,
	registry *chasm.Registry,
	opts ...EngineOption,
) *Engine
```

Each generated Rapid case creates a fresh engine:

```go
rapid.Check(t, func(rt *rapid.T) {
	engine := chasmtest.NewEngine(rt, registry)
	ctx := rt.Context()
	// Run one isolated generated case.
})
```

This scopes logging, mock cleanup, component state, physical tasks, and failure injection to one generated case. Rapid can then replay and shrink the entire case without state leaking from an earlier check.

The prototype currently has these library-neutral options:

```go
func WithTimeSource(clock.TimeSource) EngineOption

func WithNodeBackendDecorator(
	func(*chasm.MockNodeBackend),
) EngineOption
```

Phase 1A retains `WithTimeSource` and replaces or narrows the backend decorator where practical. The accepted interface should expose an explicit capability such as namespace lookup rather than arbitrary mock-backend mutation.

### Task-driving interface

The engine exposes physical CHASM task delivery directly:

```go
type TaskExecutionResult struct {
	LogicalTasksExecuted int
	Dropped              bool
}

func (e *Engine) Tasks(
	ref chasm.ComponentRef,
) (map[tasks.Category][]tasks.Task, error)

func (e *Engine) RunnableTasks(
	ref chasm.ComponentRef,
) ([]tasks.Task, error)

func (e *Engine) ExecuteTask(
	ctx context.Context,
	ref chasm.ComponentRef,
	task tasks.Task,
) (TaskExecutionResult, error)

func (e *Engine) DrainTasks(
	ctx context.Context,
	ref chasm.ComponentRef,
	limit int,
) (int, error)

func (e *Engine) ReloadExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
) error
```

The interface has the following test-host semantics; none defines production queue ordering or acknowledgement:

- `Tasks` returns copied maps and slices so callers cannot mutate engine queues accidentally.
- `RunnableTasks` includes due pure and side-effect CHASM tasks, excluding visibility maintenance tasks.
- Runnable tasks use a documented deterministic test order for reproducibility. Properties must not treat ordering between production queue categories as a product guarantee.
- `ExecuteTask` acknowledges successful and stale task deliveries.
- A handler error leaves the physical task queued for retry.
- A caller may retain a task and deliver it again to model duplicate delivery.
- `DrainTasks` stops at the supplied limit and reports the next runnable task when the limit is exhausted.
- `ReloadExecution` rebuilds the component tree from cloned persistence state while retaining the execution identity and physical task queues.

### Transaction behavior

Mutating engine operations should execute against a cloned working tree and publish it only after `CloseTransaction` succeeds. This applies to component updates and pure-task execution.

The engine tracks pending and committed transition counts separately:

```go
type transitionState struct {
	mu        sync.Mutex
	committed int64
	pending   int64
}
```

A transaction begins with the next pending count. Success commits that count; failure clears it without advancing the committed count. This prevents failed generated operations from changing later task validation behavior.

Pure-task replacement and deletion calls made during a successful transaction must be reflected in the in-memory physical timer queue. Failed transactions must not delete or replace queued tasks.

### Rapid integration

Rapid already supplies generation, state-machine execution, rejection of invalid actions, replay, and shrinking. Phase 1A removes the weighted action wrapper and evaluates whether the current `rapidtest.Execution` should remain shared. The default is to keep delivery, retained redelivery, time advancement, reload, and diagnostic history in scheduler test code until an unchanged second consumer exists.

Component request generators, expected-state models, RPC behavior selection, and business invariants always remain in the consuming library. A future SAA consumer may justify extracting identical orchestration after both implementations exist; it does not justify an API in advance.

### Generated-client RPC scripting

CHASM libraries already depend on generated service-client interfaces. Phase 1 uses the generated gomock clients rather than introducing parallel handwritten clients. A small typed script supplies deliberate outcomes and cloned call history behind a generated mock's `DoAndReturn` hook. It is runtime test support, not code generation; its closest repository analogue is a generated gomock client with `DoAndReturn`, not CHASM API generation or dynamic-config generation.

```go
type Responder[Request, Response proto.Message] func(Request) (Response, error)

type Call[Request, Response proto.Message] struct {
	Name     string
	Request  Request
	Response Response
	Err      error
}

type Script[Request, Response proto.Message] struct {
	// queued responders, default responder, and cloned call history
}

func (s *Script[Request, Response]) Push(
	name string,
	responder Responder[Request, Response],
)

func (s *Script[Request, Response]) Handle(
	ctx context.Context,
	request Request,
	_ ...grpc.CallOption,
) (Response, error)

func (s *Script[Request, Response]) Calls() []Call[Request, Response]
func (s *Script[Request, Response]) Pending() int
```

Convenience responders cover fixed success, fixed failure, and request-derived responses:

```go
script.Push("success", rpctest.Return(response))
script.Push("retryable", rpctest.Fail[Response](retryableErr))
script.Push("derived", func(req Request) (Response, error) {
	return responseFrom(req), nil
})
```

The Phase 1 script observes the following rules:

- Clone protobuf requests before recording them.
- Clone successful protobuf responses before returning and recording them.
- Consume queued responders in order, then use an explicit deterministic default.
- Fail descriptively when no responder/default exists.
- Expose unconsumed responder count so an action can prove its expected RPC occurred.
- Support only the concurrency semantics required by current production handlers and tests; do not retain more elaborate in-flight bookkeeping without a demonstrated caller.
- Preserve the response label in call history so model diagnostics identify the injected outcome class.

The generated mock remains responsible for interface conformance and unexpected-method failures:

```go
history := historyservicemock.NewMockHistoryServiceClient(ctrl)
history.EXPECT().
	DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
	DoAndReturn(services.Describe.Handle).
	AnyTimes()
```

In Phase 2, Rapid may draw a deliberate outcome before executing the CHASM task and queue the corresponding responder:

```text
Rapid action draws outcome
  -> typed script queues responder
  -> Engine delivers CHASM task
  -> library calls generated mock
  -> DoAndReturn invokes Script.Handle
  -> script clones request, consumes outcome, and records call
  -> Engine commits/acknowledges or rolls back/retains
  -> model checks state, task, and RPC history
```

Drawing before delivery keeps Rapid draws on the test goroutine, makes the fault explicit in the minimized action sequence, and avoids nondeterministic draws from RPC callbacks. Generic delivery actions use deterministic success defaults; fault-specific consumer actions queue one outcome and assert it was consumed.

Descriptor-driven outcome families, generated protobuf request/response values, semantic method profiles, timeout/cancellation observation, and ambiguous-commit behaviors are deferred to Phase 3.

### Tests

`chasmtest` should use a small synthetic CHASM library to cover the generic contract independently of any production component.

Required cases:

- runnable-task filtering and deterministic ordering;
- pure-task batching and successful acknowledgement;
- side-effect execution and successful acknowledgement;
- handler failure followed by retry;
- stale task delivery;
- duplicate task delivery;
- component-update rollback;
- pure-task rollback;
- committed transition counts after success and failure;
- singleton timer replacement and deletion;
- reload equivalence and task preservation;
- isolation between multiple executions;
- invalid execution references and task keys;
- non-runnable and unsupported task rejection;
- drain completion and drain-limit diagnostics.

If a shared Rapid helper survives Phase 1A, its tests cover only the retained generic contract: selection among runnable and delivered tasks, time advancement, reload, diagnostics, and isolation. There is no shared action-weighting API.

`rpctest` tests should verify:

- request and response cloning;
- ordered outcome consumption and deterministic defaults;
- fixed, failed, and request-derived responders;
- unexpected and unconsumed outcome diagnostics;
- the concurrency behavior retained after the reduction pass;
- compatibility with at least one generated unary client mock owned by a `*rapid.T` case.

Tests must use `-tags test_dep`. They should prefer `require`, avoid sleeps, and use exact or semantic comparisons appropriate to the value under test.

### CI

Phase 1 does not add a required property campaign. Its unit tests run through the existing unit-test coverage job:

```text
make unit-test-coverage
```

Rapid helper tests use bounded default checks and steps. High-volume checks and component property suites are deferred until a later phase establishes consumers and a measured runtime budget.

### Failure handling

- Invalid references, mismatched execution keys, unsupported task types, future tasks, and invalid drain limits return descriptive errors.
- Task-handler errors preserve the queued physical task and committed component state.
- Stale tasks return `Dropped` and are acknowledged without mutating component state.
- Reload failures leave the original tree installed.
- Drain-limit failures report the remaining runnable count and the next task's type, category, and visibility time.
- Rapid action failures include sufficient history to understand the failure in addition to Rapid's seed and minimized input.

### Trade-offs

- Cloning a component tree before mutations costs more than mutating in place, but it provides atomic publication and reliable rollback tests around the real `chasm.Node` transaction. It does not simulate the production persistence transaction.
- Deterministic single-threaded delivery does not reproduce task-processor concurrency. It makes interleavings explicitly controllable and shrinkable; concurrency testing remains separate.
- Retaining delivered tasks increases memory with the number of generated steps. The scheduler helper should use its configured step bound and retain only what redelivery properties need.
- Arbitrary backend decorators couple consumers to mock internals. Prefer explicit capabilities and keep fault injection at the operation or RPC boundary.
- Rapid remains a consumer test dependency; the base engine API does not depend on a property-testing framework.
- The harness introduces no production endpoint, authorization path, or persisted data and therefore does not expand the production security surface.

At ten times the default generated load, CPU and memory scale primarily with component-tree cloning, task history, and Rapid steps. Explicit step, drain, and retained-history limits bound all three. A crash or failed check discards the per-case engine; replay starts from a fresh engine using Rapid's reported seed.

### Implementation sequence

1. Refine engine construction and backend configuration without changing existing callers.
2. Implement transactional cloning, transition commit/abort behavior, and physical pure-task deletion.
3. Implement deterministic task inspection, execution, redelivery, draining, and reload.
4. Add the synthetic CHASM library and task-driver contract tests.
5. Add minimal typed `rpctest.Script` support and bind it to a generated unary client mock in its contract tests.
6. Keep Rapid orchestration consumer-local; extract only an unchanged capability demonstrated by a second consumer.
7. Run focused tests with `-tags test_dep`, then formatting and `make lint-code`.

### Acceptance criteria

- Existing `chasmtest` callers continue to compile and pass.
- The generic harness contains no imports from `chasm/lib/*`.
- Both `*testing.T` and `*rapid.T` can own an isolated engine.
- Task success, failure, staleness, duplication, reload, and rollback semantics are covered by deterministic tests.
- Typed RPC scripts plug into generated gomock clients, clone protobuf values, and expose deterministic outcome/call history.
- No shared weighted-action or speculative consumer API remains.
- No standalone-activity or matching behavior is implemented in Phase 1.
- Focused tagged tests and `make lint-code` pass.
- Required CI runtime remains within the existing unit-test job budget.

## Phase 2: Scheduler Adoption

### Objective

Make the CHASM scheduler the first production-library consumer of the aligned Phase 1 harness. Phase 2 uses a maintainable suite that exercises real scheduler components, registered task handlers, controlled external-service boundaries, snapshot reloads, and API operations under Rapid-generated sequences.

The scheduler owns all scheduler-specific configuration, generators, expected state, generated mock wiring, response domains, actions, and invariants. Phase 2 may reveal missing generic capabilities in Phase 1, but any framework change must be justified without reference to scheduler types and covered in `chasmtest` itself.

### Goals

- Prove that a production CHASM library can use the Phase 1 harness without privileged scheduler hooks in the shared framework.
- Cover scheduler lifecycle, timing, task interleavings, failures, redelivery, reloads, overlap policies, backfills, migration, and callback recovery.
- Check invariants after every generated state-machine action.
- Preserve exact boundary cases as deterministic or narrowly generated tests rather than relying on long random sequences to encounter them.
- Run a bounded, reproducible scheduler property suite in the existing required unit-test job.
- Produce minimized failures that include the Rapid seed, scheduler actions, CHASM task deliveries, service calls, and relevant state snapshots.
- Add a bounded, server-free scheduler reference model with an implementation-independent event and effect vocabulary.
- Run the same logical traces against the pure model and the CHASM scheduler driver.
- Demonstrate bug-finding ability against the documented Schedule V2 audit rather than treating passing randomized tests as evidence of adequate coverage.

### Non-goals

- Adding scheduler concepts to `chasmtest` or shared RPC support.
- Reimplementing the scheduler as a complete reference system; the pure model covers a bounded semantic core.
- Proving equivalence with the legacy workflow scheduler in required CI, except for focused conversion or differential oracles needed by named migration findings.
- Exercising real persistence, matching, frontend, history, or worker processes.
- Using high-volume campaigns as a required merge gate.
- Making scheduler test-support APIs part of the production scheduler API.

### Package structure

The reusable pure model is a scheduler-owned package. CHASM drivers, generated mock wiring, property orchestration, and conformance adapters remain in external test files so they are compiled only for scheduler tests:

```text
chasm/lib/scheduler/
  scheduler_property_env_test.go
  scheduler_property_services_test.go
  scheduler_property_snapshot_test.go
  scheduler_lifecycle_property_test.go
  scheduler_delivery_property_test.go
  scheduler_overlap_property_test.go
  scheduler_failure_property_test.go
  scheduler_migration_property_test.go
  scheduler_boundary_property_test.go

chasm/lib/scheduler/model/
  vocabulary.go
  model.go
  observable.go
  model_test.go
  explorer_test.go

chasm/lib/scheduler/model/conformance/
  conformance_test.go
```

The property and conformance files use package `scheduler_test`. The `model` package imports public schedule protobufs but does not import the CHASM scheduler implementation, CHASM engine, generated service mocks, Rapid, SDK workflow test environment, or a running Temporal server. Test-only exports in `export_test.go` are allowed only when a required behavior cannot be driven or observed through the scheduler's public CHASM/API surface.

### Scheduler test environment

One environment builds a real scheduler library for each Rapid case:

```go
type schedulerPropertyEnv struct {
	handler    schedulerpb.SchedulerServiceServer
	engineCtx  context.Context
	frontend   *workflowservicemock.MockWorkflowServiceClient
	history    *historyservicemock.MockHistoryServiceClient
	services   *schedulerServiceScripts
	config     schedulerPropertyConfig
	scheduleID string
}

func newSchedulerPropertyEnv(
	t *rapid.T,
	config schedulerPropertyConfig,
) *schedulerPropertyEnv
```

Construction performs the following work:

1. Create an event time source initialized to a fixed UTC instant.
2. Build scheduler configuration from bounded generated tweakables.
3. Create generated frontend and history mocks owned by the current `*rapid.T` and bind typed scripts to the methods used by scheduler task handlers.
4. Construct the real spec processor, scheduler handler, task handlers, and `scheduler.Library`.
5. Register `chasm.CoreLibrary` and the scheduler library in a fresh registry.
6. Create a `chasmtest.Engine` with the event time source and required namespace backend capability.
7. Create the schedule through the real scheduler handler.
8. Settle only the initial immediately-runnable work using a fixed drain limit.

The environment owns its small Rapid-facing delivery, retained-redelivery, time-advancement, reload, and diagnostic helpers. It also owns scheduler API helpers for create, describe, update, patch, trigger, backfill, delete, migration, matching-time queries, and Nexus completion delivery. If a later production-library consumer needs an unchanged subset, that subset can move to a shared package then.

### External-service doubles

Scheduler uses the generated frontend and history client mocks. Scheduler-owned setup connects the relevant methods to Phase 1 typed scripts:

```go
type schedulerServiceScripts struct {
	Start rpctest.Script[
		*workflowservice.StartWorkflowExecutionRequest,
		*workflowservice.StartWorkflowExecutionResponse,
	]
	Describe rpctest.Script[
		*historyservice.DescribeWorkflowExecutionRequest,
		*historyservice.DescribeWorkflowExecutionResponse,
	]
	Cancel rpctest.Script[
		*historyservice.RequestCancelWorkflowExecutionRequest,
		*historyservice.RequestCancelWorkflowExecutionResponse,
	]
	Terminate rpctest.Script[
		*historyservice.TerminateWorkflowExecutionRequest,
		*historyservice.TerminateWorkflowExecutionResponse,
	]
	Migrate rpctest.Script[
		*historyservice.StartWorkflowExecutionRequest,
		*historyservice.StartWorkflowExecutionResponse,
	]
}
```

The generated clients are wired once per Rapid case:

```go
ctrl := gomock.NewController(t)
frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
history := historyservicemock.NewMockHistoryServiceClient(ctrl)

frontend.EXPECT().
	StartWorkflowExecution(gomock.Any(), gomock.Any()).
	DoAndReturn(services.Start.Handle).
	AnyTimes()

history.EXPECT().
	DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
	DoAndReturn(services.Describe.Handle).
	AnyTimes()
```

Unwired client methods remain unexpected gomock calls. Scripts clone all recorded protos so later scheduler mutation cannot change test observations.

The doubles model only the response classes needed by scheduler behavior:

- success;
- retryable failure;
- rate-limited failure with a retry delay;
- non-retryable failure;
- already-started or use-existing success;
- not found;
- terminal workflow status from describe.

Unexpected calls or exhausted scripted outcomes fail the generated case with a descriptive message. Generic task retry and rollback behavior remains the responsibility of `chasmtest`; the scheduler model checks the resulting scheduler and service state.

Ordinary task-delivery actions use deterministic successful defaults. Failure-specific actions draw an outcome class, queue one responder on the relevant script, deliver a task expected to invoke that RPC, and assert the responder was consumed. This keeps RPC failures explicit in Rapid's shrunk action history.

### Scheduler snapshots

Snapshots separate observable API state, persisted component state, physical work, and external effects:

```go
type schedulerSnapshot struct {
	Now       time.Time
	API       schedulerAPISnapshot
	Internal  schedulerInternalSnapshot
	Tasks     []taskSnapshot
	Workflow  workflowServiceSnapshot
	History   historyServiceSnapshot
}
```

The snapshot includes only stable values needed by invariants:

- closed, sentinel, paused, limited-action, and migration state;
- conflict token, action counters, buffer counters, and recent actions;
- generator and invoker high-water marks;
- buffered starts and backfiller cursors;
- idle-close time and last completion result;
- non-visibility physical task type, category, and visibility time;
- cloned workflow/history requests and unique successful starts.

Snapshot collections are sorted by stable identity before comparison. Reload checks compare before and after snapshots while excluding values that are intentionally reconstructed or not persisted.

### Shared scheduler invariants

Every state-machine suite checks a common invariant set after each successful action:

- A live schedule has pending work or is legitimately held open by pause/backfill state.
- Closed and sentinel terminal states never reopen except through an explicitly valid create operation.
- Generator and invoker high-water marks never regress.
- Buffered starts do not exceed the configured limit.
- A task execution does not exceed the configured action budget.
- Buffer size, running workflows, recent actions, and action counts agree across internal and Describe state.
- Workflow request IDs are non-empty, stable, unique, and consistent with recorded callbacks.
- A successful logical start produces at most one unique external start for its request ID.
- Counters never become negative and completed recent actions remain within their retention bound.
- Read-only APIs do not change component state, physical tasks, or external calls.
- Reload does not change persisted/API state, physical tasks, or external calls.
- Failed tasks retain retryable work without publishing partial scheduler state.
- Redelivered stale tasks do not repeat external side effects.

Suite-specific models add stronger expectations without expanding the shared invariant set.

### Server-free scheduler reference model

The repository does not currently have the scheduler equivalent of Dan's standalone-activity model on `main`.

There are useful building blocks:

- `service/worker/scheduler.ProcessBuffer` is a pure overlap-policy function with focused unit tests;
- scheduler spec and calendar code can be tested without a server;
- the exploratory `prop-sch-chasm` branch has independent overlap expectations, scheduler snapshots, and a CHASM-versus-v1 trace normalizer.

Those pieces are not a single reference model. The exploratory models embed `*rapid.T`, the CHASM test environment, generated requests, or the SDK workflow test environment, and they split expected state across feature-specific structs. The legacy scheduler is another implementation, not an independent oracle.

Phase 2 adds a small pure model with the same architectural shape as the SAA model:

```go
func Transition(config Config, state State, event Event) (Outcome, error)

type Outcome struct {
	State    State
	Effects  []Effect
	Response Response
}
```

`Transition` is deterministic and total over every valid model state and event. It does not call a client, schedule a CHASM task, read a clock, draw a Rapid value, or mutate its input. Invalid client operations return a modeled API error without changing state or emitting effects.

#### Vocabulary

The initial external events are:

- create, update, pause, unpause, trigger, backfill, delete, and describe;
- advance logical time to an absolute instant;
- report workflow start success or a classified start failure;
- report workflow completion with status and optional result/failure;
- report cancel, terminate, describe, callback-attach, and migration outcomes where the selected Phase 2 suites require them.

The model emits declarative effects rather than executing infrastructure:

- start, cancel, terminate, describe, or attach a workflow callback;
- schedule a logical wakeup or retry at an absolute time;
- create or continue a backfill;
- request migration or close the schedule.

Each effect carries a stable logical identifier. The runner supplies a later outcome event for effects whose result is controlled by the environment. This makes retries and idempotency explicit without putting generated gRPC clients into the model.

The model state contains only semantic state needed to predict behavior:

- logical time, lifecycle, pause state, notes, and conflict token;
- schedule definition and a high-water mark for evaluated time;
- remaining-action limits and customer-visible counters;
- ordered buffered, starting, running, and recently completed logical actions;
- bounded backfill progress, retry attempt/deadline, and migration state.

`Observable` is smaller than `State`. It normalizes Describe fields and externally visible effects so the pure model, CHASM driver, and a possible future onebox driver can be compared without exposing component layout, physical CHASM tasks, or persistence metadata.

#### Spec boundary and oracle independence

The first model supports a deliberately small schedule domain: fixed intervals with phase, start/end bounds, catchup window, and the supported overlap policies. Matching instants are computed independently in the model. Full calendar, cron, timezone, and structured-calendar equivalence remains in focused spec tests until there is a credible independent oracle.

The model must not call the scheduler implementation's `ProcessBuffer`, spec processor, request-ID generator, or state helpers to calculate expected results. Those functions may inspire the vocabulary and test cases, but sharing them with the system under test would allow the same defect to satisfy both sides. Public protobuf types and stable constants may be shared.

#### Exploration and conformance

The pure package receives three kinds of tests:

1. Table tests for every transition and modeled error, including no-state-change failure cases.
2. A depth-bounded breadth-first explorer over a small canonical event alphabet to check totality, invariants, and path replay without Rapid.
3. Seeded Rapid traces over larger bounded values for shrinking and CI-friendly randomized coverage.

A scheduler-owned conformance runner applies one logical trace to two drivers:

```go
type Driver interface {
	Apply(context.Context, model.Event) (model.Observation, error)
	Quiesce(context.Context) ([]model.Effect, error)
}
```

The model driver calls `Transition`. The CHASM driver translates the same event into scheduler API calls, time changes, workflow completions, scripted client outcomes, and task deliveries through `chasmtest`. `Quiesce` drains only work causally required by that logical event, records normalized external calls, and returns an observation at a stable boundary. Infrastructure-only actions such as arbitrary task delivery, stale redelivery, failed transaction retry, and reload remain CHASM property actions; they assert preservation of the current model observation rather than becoming model business events.

This separation prevents the pure model from reproducing CHASM transaction mechanics while still checking that every implementation interleaving converges to the expected scheduler behavior.

#### Initial semantic slice

The first conformance slice covers:

- create and describe;
- interval time advancement;
- pause, unpause, update, and immediate trigger;
- all six overlap policies;
- workflow start success and retryable/non-retryable failure;
- workflow completion and recent-action retention;
- bounded backfill and limited actions;
- delete and idle closure.

Migration, callback recovery, sentinel replacement, arbitrary schedule specifications, and legacy-v1 equivalence are added only after the initial state/effect vocabulary remains stable. They continue to have direct CHASM tests even when absent from the pure model.

### Schedule V2 bug-audit capability matrix

The 62 retained findings in `../../sch-find-bugs/schedule-v2-bug-audit` are a design input and a measurable test of the methodology. A generic harness does not discover a bug by itself. Discovery requires all four of the following:

1. The tested boundary contains the faulty behavior.
2. Generators can reach the required state and minimal event/RPC sequence.
3. An oracle is independent of the faulty implementation.
4. The bounded CI campaign gives the sequence a meaningful probability, or a directed property covers the boundary exactly.

Maintain a checked-in scheduler test matrix with one row per audit ID and these fields:

- finding ID and corrected audit claim;
- evidence outcome: supported, qualified, or disproved;
- responsible boundary: pure spec/model, CHASM scheduler, generated RPC behavior, legacy scheduler, frontend/visibility, or multi-service handoff;
- required generated inputs, actions, interleavings, and RPC behaviors;
- invariant, differential oracle, or unresolved product contract;
- target phase and test name;
- result against the audit's pinned buggy baseline;
- result against current `upstream/main`: detected, fixed, contract-undecided, unreachable, or out of scope.

The matrix initially groups findings as follows.

#### Direct Phase 2 candidates

These are predominantly inside CHASM scheduler state, tasks, time, validation, spec evaluation, or migration conversion and should be expressible with the aligned in-memory harness and directed/model properties:

```text
SCH-009 SCH-014 SCH-017 SCH-018 SCH-019 SCH-020 SCH-023 SCH-024
SCH-025 SCH-027 SCH-029 SCH-033 SCH-035 SCH-036 SCH-037 SCH-038
SCH-041 SCH-043 SCH-044 SCH-048 SCH-052 SCH-053 SCH-054 SCH-056
SCH-057 SCH-058 SCH-059 SCH-061 SCH-062 SCH-064 SCH-070 SCH-074
SCH-076 SCH-080 SCH-081 SCH-082 SCH-083 SCH-084 SCH-085 SCH-088
SCH-089
```

Some are contract questions rather than automatically failing properties. In particular SCH-044, SCH-048, SCH-052, SCH-064, SCH-070, SCH-076, and SCH-085 require the matrix to name the intended contract before an oracle treats the behavior as a failure.

#### Phase 3 RPC-behavior candidates

These require generated or stateful external outcomes, response-dependent races, context observation, or ambiguous service results. They may have directed Phase 2 scripts, but Phase 3 is responsible for systematic exploration:

```text
SCH-026 SCH-028 SCH-030 SCH-039 SCH-049 SCH-055 SCH-073 SCH-077
SCH-086 SCH-087
```

#### Composed or production-boundary candidates

These require the legacy scheduler workflow, frontend validation, visibility, signal timing, or a distributed ownership handoff. They are not evidence for expanding `chasmtest.Engine` to simulate those systems:

```text
SCH-031 SCH-032 SCH-060 SCH-067 SCH-068 SCH-072 SCH-078 SCH-079
```

They belong in focused legacy-worker tests, frontend/visibility tests, or a later composed onebox/differential driver that can still reuse the event vocabulary and Phase 3 RPC behavior profiles.

#### Findings that must not become false-positive generators

- SCH-066 relies on a typed-nil protobuf request that normal gRPC transport cannot deliver. A remote request generator must not create it.
- SCH-071 was disproved at the audited baseline. Preserve a regression invariant if useful, but do not claim its discovery as a bug.
- SCH-046's nil repeated-message trigger is not wire-distinguishable from an empty message. Tests may explore the explicit empty-calendar contract, but must not report an impossible wire-level nil-element case.

### Phase 2 completion and post-Phase-3 follow-up

Phase 2 is complete with the bounded model, scheduler-owned CHASM conformance
suite, and all 62 audit findings classified in
`chasm/lib/scheduler/schedule_v2_audit_matrix.md`. The remaining direct-audit
work is deliberately deferred until after Phase 3 so descriptor-driven RPC
profiles can supply the missing external-outcome coverage.

The post-Phase-3 backlog contains 23 findings:

- 15 currently unverified findings: SCH-009, SCH-018, SCH-019, SCH-025,
  SCH-029, SCH-033, SCH-043, SCH-053, SCH-056, SCH-059, SCH-061, SCH-062,
  SCH-080, SCH-082, and SCH-089.
- 8 findings requiring an explicit product contract before an oracle can fail:
  SCH-037, SCH-044, SCH-048, SCH-052, SCH-064, SCH-070, SCH-076, and SCH-085.

Do not resume these 23 during Phase 3. Phase 3 should first deliver the
descriptor/profile machinery described below; the follow-up phase then uses
that machinery for the remaining migration-fidelity, temporal-spec, and
validation families. The 19 findings already classified out-of-scope remain
with their assigned Phase 3 or composed-production boundaries.

### Bug-finding acceptance

For each supported bug family, at least one property must fail against the audit's pinned buggy baseline or against a controlled mutation that recreates the same violated invariant. Merely porting the audit's deterministic reproduction is useful regression coverage but does not prove generated discovery; the matrix distinguishes directed regression tests from genuinely generated discovery.

Representative required discovery families are:

- identity and timestamp preservation across V1/V2 conversion round trips;
- monotonic backfill progress across zero capacity, retry, replacement, and redelivery;
- retry budget and request-idempotency preservation across duplicate task execution;
- terminal versus retryable classification of start, cancel, terminate, callback, and migration outcomes;
- conflict-token invalidation across automatic state changes;
- overlap behavior with multiple live actions and callback reordering;
- time boundaries around catchup, jitter, pause/unpause, retry, and idle close;
- protobuf validation and panic freedom for generated boundary values;
- bounded compute work and buffer occupancy;
- outbound request validity, size, field fidelity, and stable identity.

When a generated property finds a production bug, retain both the broad property and a minimal deterministic regression beside the fix. Record Rapid's minimized trace and seed in the matrix or regression description.

### Property suites

#### Lifecycle and timing

Generated configuration covers interval, phase, spec start/end, catchup window, initial pause state, limited actions, idle time, and bounded buffer/action limits.

Actions include:

- advance time across one or more matching intervals;
- pause and unpause;
- update schedule specification and metadata;
- trigger immediately;
- issue duplicate create and stale-conflict-token updates;
- add a pending backfill;
- advance before, at, and after idle deadlines;
- delete and query closed schedules;
- create, expire, recreate, and replace sentinels;
- reload.

The model predicts interval matches, action counts, remaining actions, conflict tokens, future action times, idle deadlines, and terminal state.

#### Task delivery and reload

This suite deliberately avoids draining after every operation. It composes the generic Phase 1 actions with scheduler API operations to explore task ordering:

- deliver one runnable task;
- advance to the next task;
- drain with a fixed limit;
- retain and redeliver a task;
- trigger, backfill, pause, or complete between task deliveries;
- reload with pure or side-effect tasks outstanding;
- query APIs between transitions.

It checks intermediate eligible, buffered, starting, running, completed, dropped, and stale states. Terminal subtests verify idle close, migration, callback recovery, and delete acknowledge or invalidate old physical tasks without repeating side effects.

#### Overlap and backfill

Run the same bounded model for allow-all, skip, buffer-one, buffer-all, cancel-other, and terminate-other policies.

Actions include interval advancement, immediate triggers, range backfills, and workflow completion. The expected model tracks running, completed, and deferred actions plus cancel/terminate targets.

Checks cover overlap-skipped counts, deferred-start ordering, cancellation/termination calls, recent actions, inclusive backfill bounds, backfill retry progress, concurrent backfillers, generator reserve, maximum buffer size, and limited-action independence for manual backfills.

#### Failure and idempotency

Generate start outcomes across success, retryable, rate-limited, non-retryable, and already-started classes. Generate pause-on-failure independently.

Actions include trigger, advance to retry, complete, duplicate completion, redeliver a saved task, inject a failure, and reload. The expected model tracks each request through backing off, running, completed, dropped, and evicted states.

Checks cover attempt limits, retry times, service-call cardinality, request-ID stability, pause-on-failure, last completion, atomic failure behavior, and absence of repeated effects after stale delivery.

#### Migration and callback recovery

Migration actions start or repeat migration, mutate allowed pause state while pending, reject forbidden mutations, deliver the migration task under each outcome class, redeliver it, and reload. Checks cover saved pre-migration state, one pending task, stable migration request IDs, exported legacy input, retry retention, and terminal closure.

Callback recovery actions describe the workflow, attach a callback, observe an already-closed workflow, inject describe/attach failures, deliver completion, duplicate completion, redeliver recovery work, delete, and reload. Checks cover workflow identity, callback attachment, completion status, recent/running views, retry retention, and service-call cardinality.

#### Deterministic boundaries

Exact boundaries remain focused tests with Rapid generating values around the boundary rather than long action sequences:

- interval, catchup, retry-backoff, idle, and sentinel deadlines at `-1ns`, exact, and `+1ns`;
- maximum start attempts;
- recent-action retention limit;
- limited-action exhaustion versus manual actions;
- buffer capacity and generator reserve;
- concurrent backfiller limits;
- same-visibility-time task ordering;
- start, describe, attach, cancel, terminate, and migration outcome matrices;
- updates interleaved with backfill retry;
- callback recovery after deletion.

When a property test discovers a production bug, add a minimal deterministic regression test alongside the fix. The broad property remains responsible for discovering related sequences, not for being the only regression coverage.

### Deferred differential suite

The exploratory CHASM-versus-legacy scheduler differential model is not part of the initial required Phase 2 suite. It introduces a second runtime, workflow test-environment behavior, normalization rules, and substantially more diagnostic complexity.

After the scheduler-native suites are stable, the differential model can be evaluated as an opt-in or scheduled campaign using the same generated action representation. It should not block landing the first framework consumer.

### Rapid composition

Each state-machine test creates a fresh scheduler environment inside `rapid.Check` and composes generic and scheduler-specific actions:

```go
rapid.Check(t, func(rt *rapid.T) {
	model := newSchedulerModel(rt, drawSchedulerConfig(rt))

	rt.Repeat(map[string]func(*rapid.T){
		"deliver":   model.DeliverRunnable,
		"advance":   model.AdvanceToNextTask,
		"redeliver": model.Redeliver,
		"reload":    model.Reload,
		"trigger":   model.Trigger,
		"complete":  model.Complete,
		"":          model.Check,
	})
})
```

Generators use bounded, semantically meaningful domains rather than arbitrary protos. Examples include supported overlap policies, short interval sets, boundary-adjacent durations, small buffer limits, and explicit error classes. This improves shrinking and prevents most checks from being consumed by invalid schedules.

### CI and campaign infrastructure

Required CI runs the scheduler properties through the existing unit-test coverage job. No scheduler-specific workflow is required initially.

The default suite should target:

- Rapid's default check count;
- bounded action sequences;
- a drain limit for every settling operation;
- small generated buffer and backfill limits;
- no sleeps or wall-clock dependencies;
- no parallel mutation of a generated model.

Add a focused Make target for local replay and stress without making it a separate required job:

```make
chasm-scheduler-property-test:
	go test -tags test_dep ./chasm/lib/scheduler \
		-run '^TestScheduler.*Propert' \
		-count=1 \
		-rapid.checks=$(CHASM_SCHEDULER_PROPERTY_CHECKS) \
		-rapid.steps=$(CHASM_SCHEDULER_PROPERTY_STEPS)
```

The target should forward additional Rapid arguments so a reported seed can be replayed directly. High-volume execution may shard stable top-level property tests, but sharding is not needed for the standard unit-test path.

Before enabling the complete suite in required CI, measure it under the same race and coverage settings used by `make unit-test-coverage`. The scheduler contribution should remain a small fraction of the existing 20-minute unit-test job. If it does not, reduce generated domains or split expensive scenarios into an opt-in campaign rather than increasing the global timeout.

### Failure handling and diagnostics

- Invalid generated operations reject themselves with `t.Skip` before consuming unrelated generated values.
- Expected API and service failures are represented in the model and asserted explicitly.
- Unexpected errors include the scheduler operation, current time, execution key, state snapshot, runnable tasks, and recent service calls.
- Drain-limit failures include the next runnable task and scheduler state required to diagnose a task cycle.
- Reload comparison reports the specific API, internal, task, or service section that changed.
- A failed side-effect task must remain available for retry; a stale redelivery must be dropped without another service call.
- Rapid's seed and shrink output remain the canonical replay mechanism; custom diagnostics supplement rather than replace them.

### Trade-offs and failure modes

- Independent expected-state models find more semantic bugs than invariant-only tests but require maintenance as intentional scheduler behavior changes. Models should track stable behavior, not mirror implementation fields unnecessarily.
- Using real scheduler handlers and task handlers increases setup cost but validates the actual CHASM registration and task-dispatch path.
- Random single-task delivery explores more interleavings than always draining, but generated cases take more steps to reach business outcomes. Use directed properties or scheduler-local action duplication only when measured coverage shows starvation.
- Small generated limits improve runtime and shrinking but do not provide load testing. Deterministic capacity tests cover exact limits; high-volume campaigns remain optional.
- Generated service mocks backed by typed scripts are deterministic and inspectable but do not reproduce network concurrency. Production concurrency and integration behavior remain covered by functional tests.
- Test-only exports create coupling to internals. Prefer API and persisted-state observations, and remove an export when it is no longer required.

At ten times the standard check count, work scales with checks, action steps, tree cloning, and snapshot size. Fixed drain, buffer, backfill, and retained-history bounds prevent an individual case from growing without limit. A crash or failed check discards its per-case registry, engine, mocks, and component state; replay reconstructs the case from Rapid's seed.

### Implementation sequence

1. Complete Phase 1A and obtain review of the supported engine contract and reduced API surface.
2. Rebase existing scheduler tests on the reduced harness without losing currently covered behavior.
3. Add the Schedule V2 audit capability matrix and classify all 62 findings.
4. Define or tighten the server-free scheduler event, state, effect, response, and observable vocabulary without importing the CHASM scheduler implementation.
5. Table-test the semantic transitions, followed by bounded BFS and model-only Rapid checks.
6. Keep the scheduler property environment's Rapid orchestration local and use generated frontend/history mocks with the minimal typed scripts.
7. Add common scheduler invariants and tests demonstrating that each invariant rejects a broken snapshot.
8. Verify the initial conformance slice at quiescent boundaries, then compose single-task delivery, redelivery, reload, and deliberate RPC failures around it.
9. Implement direct Phase 2 audit families in priority order: state concurrency, retry identity, backfill progress, terminal classification, migration fidelity, time boundaries, and validation/panic freedom.
10. Demonstrate representative properties fail against the pinned audit baseline or a controlled equivalent mutation.
11. Expand the model and snapshot only when required by the next property; keep infrastructure interleavings out of the semantic model.
12. Preserve exact boundary and discovered failures as focused deterministic regression tests.
13. Keep composed legacy/frontend/visibility findings out of `chasmtest`; assign them to their correct later drivers.
14. Measure focused and full scheduler runtime with `-tags test_dep`, race, and coverage settings.
15. Run formatting, focused tests, broader scheduler tests, and `make lint-code`.

### Framework and scheduler ownership

Phase 2 uses four explicit layers:

```text
chasmtest.Engine              CHASM transaction and task-delivery mechanics
chasmtest/rpctest             typed RPC outcome scripts behind generated mocks
scheduler/model               pure scheduler state, events, effects, and observations
scheduler property tests      scheduler drivers, APIs, mock wiring, and invariants
```

The ownership test is whether another CHASM library such as standalone activity can use a capability unchanged.

`chasmtest.Engine` owns only its documented in-memory subset:

- minimal test ownership shared by `*testing.T` and `*rapid.T`;
- backend decoration and controllable time;
- immutable physical-task inspection;
- deterministic test-host runnable-task ordering without asserting production cross-queue order;
- pure and side-effect task execution;
- acknowledgement, retry retention, stale delivery, and duplicate delivery;
- bounded draining;
- persistence-snapshot reload;
- transaction cloning, commit/abort transition counts, and pure-task deletion after commit.

`chasmtest/rpctest` owns:

- typed queues of generated-client RPC outcomes;
- cloned call and response recording;
- deterministic default responders and unused-response diagnostics;
- adapters suitable for generated gomock `DoAndReturn` hooks.

It does not own service-specific response domains or request semantics.

`scheduler/model` owns:

- the implementation-independent scheduler event and effect vocabulary;
- pure state transitions, modeled API responses, and normalized observations;
- model invariants, exhaustive bounded exploration, and model-only Rapid tests.

It does not own CHASM tasks, generated clients, scheduler component fields, or server drivers.

Scheduler tests own:

- scheduler configuration and library construction;
- namespace setup required by scheduler tweakables;
- generated mock wiring and scheduler-specific response domains;
- scheduler API requests and workflow completions;
- CHASM snapshots, conformance comparison, and infrastructure invariants;
- scheduler-specific input generators and error classes.
- Rapid action composition, retained deliveries, time advancement, reload operations, and minimized-trace diagnostics.

The scheduler layer must not retrieve a raw `chasm.Node` or `chasm.MockNodeBackend` to execute work. This prevents it from bypassing engine transaction, acknowledgement, and retry behavior. Backend decoration remains a narrow setup escape hatch for capabilities such as namespace lookup.

### Initial scheduler landing

The first scheduler property deliberately matches the reviewable scope of a lifecycle smoke test while using the Phase 1 execution path:

```go
rapid.Check(t, func(rt *rapid.T) {
	model := newSchedulerModel(rt, drawSchedulerConfig(rt))

	rt.Repeat(map[string]func(*rapid.T){
		"deliver":   model.DeliverRunnable,
		"advance":   model.AdvanceToNextTask,
		"redeliver": model.Redeliver,
		"reload":    model.Reload,
		"pause":     model.Pause,
		"unpause":   model.Unpause,
		"trigger":   model.Trigger,
		"":          model.Check,
	})
})
```

Its initial snapshot contains only the fields required for the first invariant set:

```go
type schedulerSnapshot struct {
	Now                    time.Time
	Closed                 bool
	Paused                 bool
	HeldOpen               bool
	PendingTasks           int
	IdleCloseTime          time.Time
	GeneratorWatermark     time.Time
	InvokerWatermark       time.Time
	BufferedStarts         int
	MaxBufferSize          int
	MaxActionsObserved     int
	MaxActionsPerExecution int
}
```

The initial invariant set covers progress, held-open idle behavior, terminal closure, monotonic watermarks, buffer limits, and per-task action budgets. Each predicate has a direct unit test containing an invalid snapshot. More detailed action, recent-workflow, migration, and callback fields are added only with the suites that require them.

### Relationship to standalone-activity model testing

Phase 1 and Phase 2 do not implement standalone-activity tests. Standalone activity is a future consumer used to keep framework boundaries honest: shared packages must not import scheduler types or encode assumptions that prevent an activity library from using the same task, Rapid, and RPC-script mechanisms.

Dan Davison's draft [SAA declarative functional-test PR](https://github.com/temporalio/temporal/pull/11149) introduces an implementation-independent event vocabulary and a real-server trace driver. His experimental [`dan/saa-model-tests-w-transitions-conformance-explorer`](https://github.com/temporalio/temporal/tree/dan/saa-model-tests-w-transitions-conformance-explorer) branch expands that proposal into:

- a server-free total transition function, `Transition(config, state, event) -> Outcome`;
- an abstract observable activity state and public Describe predictions;
- static model totality and implementation-transition conformance checks;
- a real onebox driver over frontend and matching;
- depth-bounded breadth-first traversal with path replay;
- a deterministic seeded random walk;
- directed wall-clock traces for timeouts, start delay, and retry backoff;
- positive and negative matching polls plus task-invalidation checks.

That design is complementary to this plan:

```text
Server-free behavioral model
  activity Transition(config, state, event)

Deterministic CHASM conformance
  Rapid + chasmtest + generated service mocks/scripts

Cross-service conformance
  onebox driver + real frontend/history/matching
```

A future SAA phase should reuse Dan's event vocabulary and transition model rather than introduce a competing activity oracle. The same event can be realized by two drivers:

```go
type ActivityDriver interface {
	Apply(context.Context, model.Event) (model.Observed, error)
}
```

- An in-memory driver would execute CHASM tasks through `chasmtest`, control time, and connect generated matching/history mocks to typed scripts.
- The onebox driver would issue real frontend RPCs and exercise actual matching, routing, long polling, and wall-clock timers.

For an in-memory SAA consumer, the real dispatch task handler would call a generated `matchingservicemock.MockMatchingServiceClient`. Its `AddActivityTask` method could be connected to a typed script that records the component ref, stamp, task queue, priority, and timeout and injects success or failure. A small activity-owned queue could expose recorded deliveries to an activity driver.

This would validate request construction, dispatch retry, stale or duplicate delivery, attempt-stamp invalidation, and reload around dispatch. It would not claim to validate matching partitioning, persistence, forwarding, synchronous matching, or long-poll behavior; those require the onebox layer.

No activity model, activity driver, matching queue, or SAA property is delivered in Phase 1 or Phase 2. Framework work for completeness is limited to generic contract tests such as binding `rpctest.Script` to a generated unary client mock. Any SAA-specific implementation is deferred to its own phase.

### Acceptance criteria

- Implementation begins on a new branch created from freshly fetched `upstream/main`; the exploratory branch is not merged or bulk cherry-picked.
- Scheduler is the first production-library consumer of the aligned `chasmtest.Engine` and `chasmtest/rpctest`; Rapid orchestration remains scheduler-local until a second consumer justifies extraction.
- The server-free scheduler model imports no CHASM scheduler implementation, engine, generated mock, Rapid, SDK workflow environment, or running-server dependency.
- The pure transition function is deterministic, input-preserving, total over valid states/events, and covered by tables plus bounded exploration.
- At least the initial semantic slice runs as the same logical trace against the model and CHASM driver and compares normalized observations and effects.
- Every generated case uses a fresh registry, engine, event time source, scheduler library, generated service mocks, and scripts.
- Common invariants run before and after every successful state-machine action.
- Lifecycle/timing, delivery/reload, overlap/backfill, failure/idempotency, migration/callback, and exact-boundary behavior are covered.
- Failed tasks, stale redelivery, duplicate completion, and reload behavior have explicit assertions.
- Discovered production fixes have minimal deterministic regression tests in addition to property coverage.
- The audit capability matrix classifies all 62 retained findings, and representative supported bug families demonstrably fail against the pinned buggy baseline or an equivalent controlled mutation.
- The legacy differential suite remains deferred or opt-in.
- The bounded scheduler suite passes through existing unit CI without increasing its timeout.
- Focused tagged tests, scheduler package tests, formatting, and `make lint-code` pass.
- No SAA production or test behavior is added; the documented future SAA path can use the shared APIs without changing their scheduler-independent contracts.

## Phase 3: Descriptor-Driven RPC Generation

### Objective

Generate shrinkable RPC requests, responses, and service behaviors from protobuf and gRPC descriptors, then let each use case select the behavior families that are meaningful for that method and state. Individual scheduler or SAA tests should not handwrite queues of every possible success and failure outcome.

This phase resembles CHASM API generation in its use of protobuf service descriptors, but produces test generators and runtime behavior profiles rather than production routing clients. It does not belong in dynamic-config generation and does not replace the repository's generated gomock clients.

### Separation of request and response generation

For an outbound dependency call, the system under test owns the request. The framework records that real request, checks generated invariants over it, and generates the dependency's response, error, delay, cancellation, or ambiguous result.

For an inbound API property, the framework may generate the request. Request generation distinguishes:

- wire-representable protobuf values;
- structurally valid values satisfying descriptor constraints;
- semantic boundary values selected by the use case;
- intentionally invalid but transport-representable values;
- impossible in-process values, such as a typed-nil top-level protobuf request, which remote generators must exclude.

Raw arbitrary protobuf generation is not a useful default. It would spend most checks on shallow validation and produce incoherent response state. Descriptor generation supplies structure; method profiles supply meaning.

### Generated method support

Generation consumes the same protobuf service and method descriptors used by generated clients. For each unary method it can provide:

- typed request and response generators;
- field-boundary and presence generators;
- protobuf cloning and stable rendering;
- the generated full method name;
- adapters for generated gomock clients and, if justified, `grpc.ClientConnInterface`;
- replayable behavior labels and serialized minimized values.

Streaming RPCs are out of the initial scope.

### Behavior families

The framework provides reusable generic families:

- successful response;
- retryable unavailable/deadline/resource-exhausted failure;
- terminal invalid/not-found/already-exists failure where selected by a profile;
- delayed response and context cancellation;
- response after the caller deadline;
- commit-then-response-lost ambiguous outcome;
- repeated retryable outcomes followed by success or terminal failure;
- wire-valid partial response and boundary-valued response;
- stateful response derived from the recorded request and prior calls.

Errors are not fully described by protobuf method descriptors. Temporal-specific service errors and semantic outcomes therefore live in a reusable method/domain profile registry. They are authored once per API method or coherent method family, not once per test case.

### Use-case profiles

A use case selects and constrains possible behavior rather than constructing individual outcomes:

```go
profile := rpcgen.Profile[
	*historyservice.DescribeWorkflowExecutionRequest,
	*historyservice.DescribeWorkflowExecutionResponse,
]{
	Behaviors: rpcgen.OneOf(
		schedulerRPC.RunningWorkflow(),
		schedulerRPC.CompletedWorkflow(),
		schedulerRPC.ContinuedAsNewChain(),
		rpcgen.RetryableErrors(),
		rpcgen.NotFound(),
	),
}
```

The scheduler profile layer defines coherent domains such as:

```text
DescribeWorkflowExecution
  running
  completed by status
  continued-as-new or retry chain
  not found
  retryable failure

StartWorkflowExecution
  started
  already started
  retryable failure
  terminal validation failure
  committed but response lost

Cancel/TerminateWorkflowExecution
  accepted
  already terminal/not found
  retryable failure
  terminal failure
```

Profiles may constrain fields and state transitions but must not call scheduler implementation helpers to calculate expected results. A profile describes the dependency's legal behavior; the scheduler model or invariant determines what the scheduler should do with it.

### Rapid integration and shrinking

- All draws occur on the property-test goroutine before invoking the operation.
- The minimized trace records method, behavior-family label, relevant request projection, generated response/error, and timing decision.
- Generators shrink toward smaller protobufs, shorter retry sequences, fewer state transitions, and simpler error classes.
- Stateful profiles expose a deterministic state snapshot so replay does not depend on goroutine timing.
- A context-aware responder receives and records the real RPC context, deadline, and cancellation rather than discarding them.

### Scheduler adoption

Replace Phase 2's per-test manual outcome queues incrementally. Start with the audit families that need systematic external behavior:

- callback Describe/attach races: SCH-026, SCH-028, and SCH-030;
- start attempt, catchup, and request validity: SCH-039, SCH-055, and SCH-086;
- migration already-started, retry, permanent failure, and ambiguous response: SCH-049 and SCH-077;
- cancel/terminate failure classification: SCH-073;
- deadline propagation and cancellation observation: SCH-087.

Keep deterministic Phase 2 regression cases. Phase 3 adds generated exploration around them; it does not make a minimized known-bug test less direct.

### Future SAA use

SAA can select matching/history behavior profiles without changing the generic generator. An outbound matching request remains produced by the real activity handler, while the selected profile generates accept, retry, rejection, timeout, duplicate delivery, or stateful matching behavior. Real partition routing, forwarding, persistence, and long polling remain onebox concerns.

### Non-goals

- Inferring business semantics solely from protobuf descriptors.
- Generating every possible protobuf value in required CI.
- Replacing generated service clients or gomock interface conformance.
- Simulating real networks, matching partitions, history shards, persistence, or visibility.
- Treating every generated service error as legal for every RPC method.
- Generating impossible values that normal protobuf/gRPC transport cannot represent.

### Implementation sequence

1. Define the descriptor-to-generator boundary and prove it on one unary history method.
2. Generate wire-representable request/response structures with presence and boundary controls.
3. Add context-aware generated-mock adapters and cloned call recording.
4. Define generic success, retryable, terminal, timeout, cancellation, retry-sequence, and ambiguous-commit behavior families.
5. Add a small Temporal semantic profile registry for scheduler-used frontend/history methods.
6. Integrate profiles with Rapid draws, shrinking, labels, and replay diagnostics.
7. Replace scheduler handwritten outcome matrices one method family at a time.
8. Demonstrate discovery of the Phase 3 audit candidates against the pinned baseline or controlled mutations.
9. Measure default CI runtime and keep broader method/value domains in an opt-in campaign.
10. Run generated-code verification if generation emits source, focused tests with `-tags test_dep`, race where appropriate, and `make lint-code`.

### Acceptance criteria

- A use case selects behavior families without manually constructing each response/error sequence.
- Generated outbound-dependency tests use the real request produced by the system under test.
- Generated inbound requests are transport-representable and clearly classified as valid, boundary, or intentionally invalid.
- Temporal semantic errors and coherent response states come from reusable method profiles rather than descriptor guesses.
- Context deadline and cancellation are observable by responders.
- Ambiguous commit, retries, and stateful races are deterministic and shrinkable.
- Minimized failures replay from the reported seed and behavior trace.
- The scheduler Phase 3 audit candidates have explicit discovery evidence.
- The default campaign remains bounded for standard CI; broader descriptor exploration is opt-in.
