# Umpire — Sparse Regression Plans

> **Status: current architecture; implemented with adoption ongoing.** The typed authoring API, compiler, executor,
> canonical Temporal protocol extension, artifacts, global rules, and functional proofs are
> implemented under `common/testing/umpire/regress` and `tests/umpire2`. Seven functional sparse
> proofs run today; deeper legacy-oracle retirement blockers are inventoried below.

## Thesis

A regression test should describe only the **interesting semantic key frames** of an execution.
Umpire should infer and drive everything routine between them.

```go
plan := regress.AllPaths(
	nexus.Complete("op", nexus.Succeeded),
	nexus.RespondStart("op", nexus.Async),
	nexus.State("op", nexus.Completed),
)
```

This plan says:

1. deliver a successful completion for Nexus operation `op`;
2. only afterwards, respond to its start request with an async result;
3. reach the operation's `completed` state.

It intentionally does not say how to create a namespace, workflow, task queue, endpoint, or
operation; how to poll the workflow and Nexus tasks; where the callback URL and token come from;
which concrete ids to use; when to wait; or how to clean up. The model's declared capabilities
contain that causal knowledge. The compiler connects the key frames with those capabilities, and
the executor drives the resulting plan while the Monitor applies the global rulebook.

The durable regression is therefore neither a hand-written RPC script nor a random seed. It is a
sparse, declarative outline over the same semantic model Umpire uses to plan and judge.

## Inspiration: key frames, not transcripts

TigerBeetle's [Random Fuzzy Thoughts][random-fuzzy-thoughts] describes interactive regression
tests as a series of predicates over system state: the author supplies the interesting points and
the simulator handles the auxiliary traffic between them. This avoids two failure modes:

- a concrete interactive trace is brittle when implementation details change; and
- a PRNG seed is not stable when the generator or system changes.

The post calls these predicates **key frames**. Umpire can take the idea further because it already
has explicit entity lifecycles, actions, relations, capabilities, and a Monitor:

- state and relation instructions are key-frame predicates;
- action instructions pin a mechanism only when the mechanism is essential to the regression;
- list order and composition operators form a partial order over key frames;
- the Planner fills gaps using the Actions model;
- the Monitor judges the execution using global rules; and
- each run records the completed semantic plan for diagnosis and same-version replay.

The sparse source is deliberately **re-expanded** against the current model on every normal run.
It preserves the meaning of the regression while allowing uninteresting mechanics to evolve.

[random-fuzzy-thoughts]: https://tigerbeetle.com/blog/2023-03-28-random-fuzzy-thoughts/

## Goals

- Express any functional-test behavior whose semantics exist in the Umpire model.
- Make the common authoring form a short, ordered list of typed Go instructions.
- Allow the author to pin either outcomes, actions, or both.
- Infer setup, intermediate actions, waits, dataflow, concrete identities, and cleanup.
- Express races as partial-order constraints rather than sleeps or imperative goroutine scripts.
- Express faults and holds as scoped model policies.
- Support one canonical satisfying path or every satisfying semantic path.
- Fail before execution when the requested behavior cannot be constructed from available model
  capabilities.
- Apply only global model rules and relational invariants; do not grow a second assertion system.
- Produce inspectable completed plans and grounded bindings for failure diagnosis and replay.

## Non-goals

- Arbitrary Go callbacks, custom realizers, or inline RPC calls in a regression plan.
- Test-local assertions over implementation details.
- Treating a sparse plan as a stable concrete schedule across model revisions.
- Silently skipping unavailable capabilities or truncating `AllPaths` enumeration.
- Using wall-clock sleeps as semantic ordering.
- Replacing unconstrained exploration or fuzzing. Regression plans and exploration share a model,
  but have different satisfaction criteria.

## The authoring surface

The primary API is a variadic constructor over a small `Instruction` vocabulary. The returned
value is ordinary typed Go, but its contents are declarative.

```go
plan := regress.OnePath(
	workflow.State("caller", workflow.Started),
	nexus.State("op", nexus.Started),
	nexus.Cancel("op"),
	nexus.State("op", nexus.Canceled),
)
```

List order establishes order only between the listed key frames. It does **not** prescribe all
events between them. The compiler may insert any model action needed to satisfy the next
instruction, provided the completed path respects all instructions, scopes, relations, and
capability constraints.

Two constructors select path satisfaction:

```go
regress.OnePath(instructions...)
regress.AllPaths(instructions...)
```

There is no fluent builder and no requirement to construct a graph by hand. Internally the
instruction list compiles to a constraint DAG, but that machinery is not the normal authoring API.

A test compiles the sparse value against the canonical model domain and a declared environment
profile, then hands the completed suite to the functional-test harness:

```go
domain, err := protocol.DefaultRegressionDomain()
require.NoError(t, err)
suite, err := regress.Compile(plan, domain, profile)
require.NoError(t, err)
require.NoError(t, regress.Run(ctx, suite, harness))
```

`Compile` is pure. `Run` creates the synthesized per-path environments and applies the Monitor's
global verdict.

### Instruction categories

The public vocabulary is intentionally small. Domain packages provide typed constructors for the
semantic nouns and verbs.

| Category | Meaning | Example |
|---|---|---|
| Outcome | A model predicate that must become true | `nexus.State("op", nexus.Completed)` |
| Action | A model action that must occur | `nexus.Complete("op", nexus.Succeeded)` |
| Relation | A relationship that must hold | `nexus.ChildOf("op", "caller")` |
| Binding | Give a derived typed value a symbolic name | `regress.Bind("x", workflow.RunID("handler"))` |
| Policy | Temporarily alter allowed environment behavior | `nexus.Drop(rpc.CancelNexusOperation)` |
| Composition | Add ordering or scope | `AnyOrder`, `During`, `Before` |
| Requirement | Restrict the environment/model variant | `regress.Require(capability.CHASM)` |

An instruction is not an assertion closure. Each constructor maps to a registered predicate,
action, policy, relation, or environment capability in the model catalog.

## Symbolic variables

Plans need to say that two references denote the same thing without knowing a concrete id. They
also need to refer to ids the server mints only after execution begins. Symbolic names provide that
indirection without requiring Go variables or declarations.

```go
regress.OnePath(
	nexus.State("op", nexus.Started),
	nexus.Cancel("op"),
	nexus.State("op", nexus.Canceled),
)
```

`"op"` is declared by first use. The `nexus.State` schema says it is a `NexusOperation`; later
instructions must use it compatibly. Reuse means identity equality. Different symbolic names mean
distinct entities unless an explicit equality relation says otherwise.

The compiler maintains a typed symbol table:

```text
op       : Entity<NexusOperation>
caller   : Entity<WorkflowRun>
x        : Value<RunID>
token    : Value<NexusCallbackToken>
```

A symbolic value is not a stringly-typed runtime lookup. The string is only its source-level name;
the instruction schema supplies its type, and compilation rejects inconsistent use.

### Entity symbols

Entity symbols are grounded in one of three ways:

- **Created by a synthesized or pinned action.** The planner selects an action with a fresh effect
  and binds the observed entity to the symbol.
- **Selected from observed state.** A predicate may bind an existing entity satisfying its type,
  state, relations, and distinctness constraints.
- **Bound by relationship.** A successor run, child activity, embedded operation, or reset fork is
  bound from an observed relation rather than a pre-known id. This reuses the lineage and
  bind-on-observation design in [`UMPIRE_IDENTITY.md`](./UMPIRE_IDENTITY.md).

### Value symbols

Value symbols range over typed ids, tokens, payloads, links, timestamps, enum values, and other
modeled data. Most dataflow remains invisible because action capabilities can pass their outputs to
dependent actions directly. Authors name a value only when its reuse or equality is part of the
regression.

For example, the declarative meaning of:

```go
regress.Bind("x", workflow.RunID("handler"))
```

is `x = handler.RunID`, with `x` grounded when the handler run is observed. An instruction whose
schema expects a `RunID` may consume `"x"`. Source-level forward references inside nested or
non-local constraints are allowed: the compiler resolves their data dependencies, subject to the
plan's explicit list order. If the required producer must occur after its consumer, compilation
fails. An ungroundable or multiply-inconsistent value is also a planning error.

Concrete literals remain possible through typed domain constructors, but they are values rather
than symbolic names. The API must keep the two syntactically distinct wherever ambiguity is
possible.

## Outcomes are implicit assertions

A plan does not need a separate test-specific assertion language. An outcome instruction serves
two roles:

1. it is a goal the compiler must find a route to; and
2. it is a predicate the executor must observe at runtime.

```go
nexus.State("op", nexus.Completed)
```

Compilation proves that the goal is reachable in the abstract model. Execution proves that the
real system reached it. The Monitor simultaneously checks every global safety, liveness,
conformance, data, and relational rule.

Outcomes are not limited to lifecycle states. Relations and typed semantic predicates are also
outcomes:

```go
regress.OnePath(
	nexus.StartActivity("op", "activity"),
	activity.State("activity", activity.Completed),
	nexus.LinkedToActivity("op", "activity"),
	activity.LinkedToNexusOperation("activity", "op"),
)
```

If an assertion in an existing functional test cannot be expressed as a reusable model predicate
or caught by a global rule, the model is incomplete. The fix is to add the missing predicate,
relation, action, or invariant—not an assertion callback in this API. This is the deliberate
expressiveness boundary:

> A sparse plan can fully express any behavior represented by model capabilities and predicates;
> reaching full functional-test coverage therefore requires making the model semantically
> complete.

## Actions are optional key frames

An outcome-only plan gives the compiler maximum freedom:

```go
regress.OnePath(
	nexus.State("op", nexus.Completed),
)
```

The compiler may choose any canonical model route to a completed operation. This is appropriate
when only the resulting behavior matters.

An action instruction pins a mechanism when that mechanism is the regression:

```go
regress.OnePath(
	nexus.Complete("op", nexus.Succeeded),
	nexus.State("op", nexus.Completed),
)
```

The compiler still synthesizes the action's preconditions and passes its outputs forward. Pinning
one action never requires the author to spell out its surrounding mechanics.

This distinction is essential for translating functional tests:

- “an operation eventually completes” is an outcome;
- “completion arrives before the start response” pins two actions and their order;
- “a callback is retried after one failed delivery” pins a policy and an action;
- “both sides carry matching links” uses relational outcomes.

## Ordering and partial order

The list form is sequential by default:

```go
regress.OnePath(a, b, c)
```

means `a < b < c` for the semantic occurrence or satisfaction of those key frames. Synthesized
actions may occur anywhere consistent with those constraints.

Three composition operators cover non-linear cases.

### `AnyOrder`

```go
regress.AnyOrder(a, b)
```

adds no order between `a` and `b`, while surrounding list entries remain before and after both.
`OnePath` chooses one canonical satisfying ordering. `AllPaths` includes each semantically distinct
satisfying ordering unless partial-order reduction proves that they are equivalent.

### `During`

```go
regress.During(policy, body...)
```

arms a model-declared policy before the first body key frame and disarms it after the last. The
policy remains active while the executor drives all synthesized behavior needed by the body.

```go
regress.OnePath(
	nexus.State("op", nexus.Started),
	regress.During(
		nexus.FailNext(rpc.CancelNexusOperation),
		nexus.Cancel("op"),
	),
	nexus.State("op", nexus.Canceled),
)
```

Policies are always disarmed during cleanup, including after a drive failure or timeout.

### `Before`

`Before` is the uncommon escape hatch for a non-local edge that cannot be expressed cleanly by
nesting lists, `AnyOrder`, and `During`. Step labels are symbolic strings, so it still does not
require Go variables:

```go
regress.AnyOrder(
	regress.Step("a", instructionA),
	regress.Step("b", instructionB),
	regress.Step("c", instructionC),
	regress.Before("a", "c"),
	regress.Before("b", "c"),
)
```

This group leaves `a` and `b` unordered while requiring both before `c`. Labels name instruction
occurrences only within their plan; they are separate from entity/value symbols. `Before` never
accepts an arbitrary Go callback. Most tests should never need it.

Together these constructs describe a partial-order DAG while keeping ordinary tests visually
equivalent to a list of instructions.

## Path satisfaction

Sparse instructions generally admit several valid completions. Path mode defines what it means to
satisfy the plan.

### `OnePath`

`OnePath` compiles one canonical satisfying semantic path. Selection is deterministic under a
given model version and environment capability set. Canonical selection should prefer, in order:

1. the fewest semantic actions;
2. the fewest created entities and resources;
3. the lowest stable action-catalog order; and
4. a stable lexical tie-breaker over the normalized IR.

The exact tie-breakers are part of the compiler contract so repeated runs do not drift randomly.
The sparse plan is nevertheless re-expanded after model changes; the previously selected route is
not a hidden source-level constraint.

### `AllPaths`

`AllPaths` compiles a deterministic suite containing every distinct satisfying semantic path. The
suite is satisfied only if every compiled path executes successfully and passes the global rules.

“All” means all paths that satisfy the sparse instructions. It does not mean every possible system
execution, including executions that avoid a requested outcome. “The outcome is inevitable on all
possible executions” is a universal model property and belongs in a separate operation such as
`MustReach`, not in regression path enumeration.

The enumeration domain is:

- abstract model states, relations, and semantic actions;
- generated concrete ids and values quotiented out;
- routine polling, waits, cleanup, and unmodeled transport bookkeeping quotiented out; and
- independent action permutations collapsed when they are equivalent in the model.

Two paths remain distinct when they traverse different model states or relations, use different
semantic actions, exercise an ordering whose effects are not independent, select different model
variants, or cross a policy boundary differently.

### Finiteness and cycles

By default `AllPaths` enumerates simple paths between explicit key frames: a normalized abstract
world state is not revisited within a gap. This gives “all” a finite meaning in cyclic models.

Repeated behavior must be requested explicitly, for example by listing the action twice or with
`regress.Repeat(3, action)`. `Repeat` requires a positive finite count; the compiler rejects a
zero or negative count as an unbounded cyclic request.
It never silently caps depth, path count, runtime, or combinations. If complete enumeration is too
large, it reports the exact or conservatively proven lower-bound size and asks the author to narrow
the plan or choose `OnePath`.

## Model-only capabilities

The planner may use only capabilities registered by the model. A plan cannot define an action,
resource adapter, worker behavior, RPC call, or assertion inline.

This restriction is what keeps sparse instructions meaningful. The compiler can fill a gap only
because every available capability has declared semantics:

```text
Capability {
    name
    typed parameters
    preconditions
    effects
    output bindings
    required environment capabilities
    realization
    cleanup
}
```

The existing [`UMPIRE_ACTIONS.md`](./UMPIRE_ACTIONS.md) `Action` is the foundation. Regression
planning requires extending the catalog to cover resources, policies, typed value flow, and
relations, but it should not introduce a separate causal model.

The boundary has two useful consequences:

- a compiled plan is explainable entirely in model vocabulary; and
- failure to translate a functional test becomes concrete evidence of a model gap.

## Setup is synthesized too

To replace functional tests rather than merely their central RPC sequence, environment setup must
participate in planning. Namespaces, workers, task queues, endpoints, handlers, feature gates, and
dynamic configuration are model resources with typed providers and lifetimes.

An action that requires a worker-target Nexus endpoint may cause the compiler to synthesize:

```text
namespace
  └── task queue
        ├── worker with required workflow/handler behavior
        └── Nexus endpoint targeting that worker
```

Resources are deduplicated by symbolic identity and compatible configuration. Their creation must
precede consumers, and cleanup follows reverse dependency order.

Most plans never mention these resources. An explicit requirement is needed only when the
environment variant is part of the regression:

```go
regress.OnePath(
	regress.Require(capability.CHASM),
	regress.Require(capability.ActivityCallbacks),
	nexus.StartActivity("op", "activity"),
	activity.State("activity", activity.Completed),
)
```

The requirement selects from model-supported environment variants; it does not run an arbitrary
configuration closure.

## Compilation

Compilation is pure: it reads a sparse plan, a model catalog, and an environment capability set and
returns an executable plan suite or a structured error. It does not start a cluster or issue
traffic.

```text
sparse instruction list
        │
        ▼
typed symbols + normalized predicates/actions/scopes
        │
        ▼
partial-order constraint DAG
        │
        ▼
goal regression over registered capabilities
        │
        ▼
resource synthesis + value/identity grounding constraints
        │
        ▼
canonical path selection or complete path enumeration
        │
        ▼
validated executable plan suite
```

### 1. Type and normalize

The compiler walks instructions, creates the symbol table, rejects incompatible symbol uses, and
lowers list/nesting syntax to milestones, predicates, scopes, and order edges. It detects direct
contradictions such as two distinct symbols forced to the same unique entity or mutually exclusive
states required at one key frame.

### 2. Regress goals

For each outcome or pinned action, the compiler searches backward for capabilities whose effects
satisfy it. Their preconditions become new goals. Search continues until the goals are provided by
earlier key frames, synthesized initial resources, or already-grounded observations.

Pinned actions constrain this choice; outcome-only gaps leave it open. The planner joins solutions
across entities through relations and shared value symbols rather than planning each lifecycle in
isolation.

### 3. Synthesize resources and dataflow

Selected actions contribute resource requirements and typed input/output terms. The compiler
chooses compatible providers, orders resource lifetimes, unifies values, and determines which
symbols bind from driver results versus observed facts or relations.

### 4. Enumerate and reduce

`OnePath` selects the canonical solution. `AllPaths` enumerates every satisfying semantic solution,
using sound partial-order reduction only when actions are declared independent. Reduction must not
collapse a race the model says can affect state, relations, outputs, policies, or global rules.

### 5. Validate

Every completed path is checked before execution:

- all symbols are consistently typed and groundable;
- all action preconditions have producers;
- every requested outcome is reached abstractly;
- ordering and policy scopes are acyclic and satisfiable;
- resource lifetimes cover every consumer;
- required environment capabilities are present;
- all path enumeration is finite and complete; and
- no realization outside the model catalog appears.

## Diagnostics

Planning errors should explain the causal gap, not merely report “unreachable.” For example:

```text
cannot satisfy instruction 2: nexus.Complete("op", Succeeded)
  requires NexusOperation("op") @ started
  candidate action nexus.RespondStart("op", Async) is constrained after instruction 2
  no other registered capability establishes started before instruction 2
```

Error categories are stable and structured:

- symbol type conflict;
- contradictory predicate or ordering;
- unreachable outcome;
- missing action or resource capability;
- unavailable environment capability;
- ambiguous grounding that changes semantics;
- unbounded cycle;
- incomplete `AllPaths` enumeration; and
- invalid policy lifetime.

Errors identify the source instruction, the unsatisfied predicate, candidate capabilities that were
rejected, and the shortest missing causal chain. This makes model gaps actionable.

## Execution

Each compiled path runs in an isolated environment. The executor is generic over model
capabilities; it does not rediscover how to perform domain actions.

For one path it:

1. creates synthesized resources in dependency order;
2. installs standing/reactive actions and enters initial policy scopes;
3. fires proactive semantic actions when their observed preconditions hold;
4. grounds symbols from results, facts, and relations;
5. waits on model predicates to cross each key frame;
6. reconciles declared effects and footprints with observations;
7. continuously applies the global Monitor rulebook;
8. leaves scopes and drains to quiescence;
9. resolves global liveness obligations; and
10. cleans up resources in reverse dependency order.

Waiting is observation-driven, using model generation changes or bounded predicate polling. It
never uses `time.Sleep` to create semantic ordering. A timeout is an execution bound and produces a
diagnostic; it is not evidence that an expected state was reached.

### Satisfaction

A compiled path is satisfied only when:

- every pinned action occurs in the required order;
- every outcome predicate is observed while its scope is active;
- every synthesized action reconciles with its declared effects;
- all global safety and relational rules pass at every observation;
- all global liveness obligations resolve at teardown; and
- execution reaches quiescence without a leaked policy or resource.

`OnePath` succeeds when its one path is satisfied. `AllPaths` succeeds only when every path is
satisfied. An unavailable capability is never success: compilation returns an explicit unsupported
result, and the surrounding test harness decides whether that environment should fail or skip.

### Failure and crash handling

Policies and resources register cleanup as they are installed, so partial setup can unwind. Each
path uses a fresh namespace/environment, preventing one failed path from contaminating later ones.

The executor writes its artifact incrementally. A process crash should leave at least the selected
completed path, model identity, actions begun, grounded bindings, active scopes, and observations
flushed so far. Cleanup after a process crash is best-effort; isolation makes orphaned state bounded
and externally discoverable.

## Replay and evolution

There are two intentionally different artifacts:

- **Sparse plan:** source-controlled intent, recompiled against the current model on every normal
  run.
- **Completed plan:** a grounded semantic execution selected by a particular model version,
  suitable for diagnosis and exact same-version replay.

A completed artifact records:

```text
plan name and path mode
normalized sparse instructions
model/catalog version
environment capability set
completed semantic DAG
resource choices
grounded symbol bindings
policy intervals
realized action order
fact log and rule verdicts
```

Seeds may be recorded for exploratory choice below the semantic plan, but a seed alone is never the
regression artifact. This follows the TigerBeetle observation that generator changes can silently
change a seed's meaning.

Normal regression runs re-expand rather than pin the previous completed path. If a new model action
creates a shorter canonical route, `OnePath` may select it. If a new satisfying route is added,
`AllPaths` includes it. This is desirable: the sparse contract remains stable while its coverage
tracks the current model.

## Representative functional tests

The proposed API is best judged against structurally different cases in
[`tests/nexus_workflow_test.go`](./tests/nexus_workflow_test.go), not only ordinary happy paths.
These examples are intended API shapes; exact domain constructor names should mirror the final
model vocabulary.

### Async completion before start response

```go
regress.AllPaths(
	nexus.Complete("op", nexus.Succeeded),
	nexus.RespondStart("op", nexus.Async),
	nexus.State("op", nexus.Completed),
)
```

The ordering of the two pinned actions is the regression. Operation creation, callback capture,
and all task traffic are synthesized.

### Two callers share one handler workflow

```go
regress.AllPaths(
	regress.AnyOrder(
		nexus.Start("left", nexus.HandlerWorkflow("handler")),
		nexus.Start("right", nexus.HandlerWorkflow("handler")),
	),
	workflow.State("handler", workflow.Completed),
	nexus.State("left", nexus.Completed),
	nexus.State("right", nexus.Completed),
)
```

The repeated `"handler"` symbol requires both operations to relate to the same handler workflow.
`AllPaths` covers semantically distinct caller orderings and attach/start routes.

### Cancellation is retried

```go
regress.OnePath(
	nexus.State("op", nexus.Started),
	regress.During(
		nexus.FailNext(rpc.CancelNexusOperation),
		nexus.CancelWithRetry("op"),
	),
	nexus.CancelRequestFailed("op"),
	nexus.State("op", nexus.Canceled),
)
```

The one-shot policy is armed across the cancellation action and makes the handler reject the first
cancel request non-retryably. The public standalone cancellation snapshot proves the failed request
before the harness delivers the operation's terminal cancellation outcome. Embedded operations use
the corresponding public-history event. Transport-retry coverage remains distinct because a
retryable delivery failure deliberately does not emit `NexusOperationCancelRequestFailed`.

### Start-to-close timeout

```go
regress.OnePath(
	nexus.Schedule("op", nexus.StartToClose(2*time.Second)),
	nexus.RespondStart("op", nexus.Async),
	nexus.State("op", nexus.TimedOut),
)
```

The timeout configuration is action input. A global transition/data rule validates that the
observed timeout kind and failure metadata agree with the configured domain semantics.

### Bidirectional Nexus/activity links

```go
regress.OnePath(
	regress.Require(capability.ActivityCallbacks),
	nexus.StartActivity("op", "activity"),
	activity.State("activity", activity.Completed),
	nexus.LinkedToActivity("op", "activity"),
	activity.LinkedToNexusOperation("activity", "op"),
)
```

The link checks are reusable relation predicates. The global rulebook owns link well-formedness and
bidirectional consistency.

### Callback after caller completion

```go
regress.OnePath(
	nexus.State("op", nexus.Started),
	workflow.State("caller", workflow.Completed),
	nexus.Complete("op", nexus.Succeeded),
	nexus.State("op", nexus.CallbackFailed),
)
```

This pins the relative lifecycle key frames and completion action. The model owns the expected
callback terminal semantics; the plan does not assert a transport-specific error string.

## Architecture

The design calls for four deep modules with narrow interfaces.

### `regress`: authoring and normalized IR

Responsibilities:

- the `Instruction` abstraction and `OnePath` / `AllPaths` constructors;
- list and composition syntax;
- typed symbolic term collection;
- source locations and plan formatting; and
- lowering to a domain-independent constraint DAG.

It depends on abstract model vocabulary, not Temporal clients or test environments.

### Domain catalog

Responsibilities:

- typed predicate and relation schemas;
- action capabilities with preconditions/effects;
- resource providers and lifetimes;
- policy capabilities and scopes;
- typed input/output value flow;
- environment requirements; and
- action independence declarations used by partial-order reduction.

This extends the canonical Umpire protocol declaration compiled by `protocol.Default`. Monitor
facts/entities, executable Actions, and sparse capabilities are exposed by that one protocol;
realization identifiers are shared with the harness rather than repeated as string switches.

### Compiler

Responsibilities:

- type checking and constraint normalization;
- cross-entity goal regression;
- resource and dataflow synthesis;
- canonical path selection;
- complete path enumeration and sound reduction;
- static validation; and
- causal diagnostics.

Its inputs and outputs are immutable values, making it deterministic, cacheable, and testable
without a cluster.

### Executor and artifact writer

Responsibilities:

- resource lifecycle;
- action realization through registered capabilities;
- observation-driven waits and binding;
- policy scope safety;
- effect/footprint reconciliation;
- Monitor integration;
- bounded parallel execution of path suites; and
- incremental artifacts and replay.

The executor consumes only validated completed plans. It does not contain a second planner.

## Testing strategy

### Pure instruction/IR tests

- first use declares a correctly typed entity or value symbol;
- reuse enforces identity and type equality;
- distinct symbols remain distinct;
- conflicting uses identify both source instructions;
- list, `AnyOrder`, `During`, and `Before` lower to the expected DAG;
- invalid or leaking policy scopes fail normalization; and
- formatting is stable and round-trippable as an artifact representation.

### Pure compiler tests

- an outcome-only gap synthesizes all required actions and resources;
- a pinned action narrows otherwise valid routes;
- server-minted identity binds from an observed relation;
- value outputs flow to dependent actions without author declarations;
- `OnePath` selects the same canonical plan repeatedly;
- `AllPaths` finds every finite semantic route;
- independent permutations reduce while meaningful races remain;
- explicitly bounded cycles enumerate completely;
- unbounded cycles fail;
- missing environment/model capabilities produce a causal explanation;
- contradictory outcomes and orderings fail before execution; and
- no budget or path cap silently drops a route.

### Executor tests with a fake domain

- proactive actions wait for observed preconditions;
- reactive actions and policies install before relevant traffic;
- key-frame outcomes are observed in order;
- relation/value bindings wake dependent actions;
- policies disarm on success, error, cancellation, and timeout;
- effect and footprint drift fail the path;
- a global safety violation stops the path with the first useful verdict;
- liveness obligations resolve only at quiescence;
- artifacts remain useful after an injected crash point; and
- path environments cannot contaminate one another.

### Functional proofs

Migrate a deliberately varied slice of Nexus tests:

1. ordinary sync or async completion;
2. completion before start response;
3. cancel failure and retry;
4. two callers sharing a handler;
5. a real timeout;
6. cross-entity bidirectional links; and
7. a callback after caller completion.

Each migration should delete equivalent imperative mechanics. If it cannot, classify the blocker
as a missing predicate, action, relation, resource, policy, or global rule. That inventory measures
model completeness more honestly than counting translated test functions.

Current migration inventory:

- `TestNexusOperationStartToCloseTimeout` is migrated. Its start-to-close configuration, timeout
  kind, and failure metadata are now public-history facts checked by a global rule.
- `TestNexusOperationStartsStandaloneActivityBidirectionalLinks` is migrated. Public Activity and
  Nexus link snapshots feed reusable relation predicates and the bidirectional consistency rule.
- `TestNexusCallbackAfterCallerComplete` is migrated. Caller/handler ordering and callback failure
  are expressed by lifecycle outcomes and the completion action.
- A focused sparse cancellation proof now observes a failed cancel request before terminal
  cancellation. The model imports both the embedded public-history event and the standalone public
  cancellation snapshot; the original imperative test remains because it additionally compares
  two embedded operations' cancellation state and event metadata.
- Ordinary sync completion retains its imperative test for result payload, handler-supplied
  workflow-event links, and state-machine deletion. These need modeled payload/link predicates and
  a terminal-storage invariant before that test can be removed.
- Completion-before-start and shared-handler coverage retain their imperative test for callback
  attachment request references, event timestamps, and duplicate start-response idempotency. These
  need typed callback-reference predicates and an idempotency rule.

## Trade-offs

### Performance

Compilation adds search, but its inputs are pure and keyed by normalized plan + model version +
capability set, so completed templates can be cached. Live functional execution is expected to
remain the dominant cost. Grounded ids and environment instances are deliberately excluded from
the cache key.

### Scalability

`AllPaths` is inherently combinatorial. Symbolic search, constraint propagation, and sound
partial-order reduction reduce redundant work, but cannot make a truly exponential semantic space
linear. Successful compilation reports the exact path count and never returns a partial suite. Path
execution uses bounded parallelism and isolated environments; a 10× increase in path count
increases required execution capacity predictably instead of increasing shared-environment
interference.

### Complexity

The compiler is substantially more complex than a fluent RPC builder. The payoff is concentrated
behind a much smaller authoring interface and a reusable causal model. Keeping only sequence,
`AnyOrder`, `During`, and rare `Before` avoids exposing planner complexity to test authors.

### Security and capability honesty

Forbidding arbitrary plan-local callbacks prevents tests from bypassing capability restrictions,
running unexpected code during compilation, or encoding unreviewable side effects. Environment
profiles decide which registered realizations are allowed. Unsupported behavior is explicit.

### Model coupling

Sparse plans are more stable than RPC transcripts but intentionally coupled to semantic model
names. Renaming or changing a predicate/action is a model API change and should produce a compile
failure. That is preferable to a concrete test silently exercising different behavior.

## Failure modes

### The model is incomplete

Compilation fails with the shortest missing causal chain. This is expected during adoption and
becomes the backlog for model completeness. The system must not fall back to an opaque callback.

### Declared effects do not match reality

Execution reconciliation fails and records the divergence. A plan is not satisfied merely because
the driver returned no error.

### The live system chooses an unmodeled ordering

The Monitor reports conformance or global-rule drift. If the ordering is valid, add it to the model;
if invalid, the regression found a defect.

### A policy leaks

Scope cleanup runs on every exit path and the path cannot be satisfied while a policy remains
armed. Fresh environments contain damage after process-level crashes.

### `AllPaths` becomes too large

Successful compilation reports the exact complete count. If enumeration exhausts an explicit
caller-supplied compilation resource limit, compilation fails and returns no executable suite; a
proven lower bound may accompany the diagnostic. The author chooses narrower key frames, stronger
constraints, explicit cycle bounds, or `OnePath`. There is no implicit semantic path budget.

### The system does not converge

After finite planned perturbations, policies return the environment to its reliable mode and the
executor drains toward quiescence. Failure to settle or resolve a `MustProgress` obligation is a
liveness failure, not a timeout to ignore.

## Relationship to existing Umpire pieces

- [`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md) supplies observed entity state, relations, facts, and
  the global rulebook that judges every path.
- [`UMPIRE_ACTIONS.md`](./UMPIRE_ACTIONS.md) supplies the causal operators from which gaps are
  filled. Regression planning extends their typed data, resource, policy, and relation semantics.
- [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md) supplies route search. Sparse regression compilation
  generalizes it from one entity/target to ordered cross-entity predicates and pinned actions.
- [`UMPIRE_IDENTITY.md`](./UMPIRE_IDENTITY.md) supplies observation-grounded identity and lineage
  bindings for symbolic entity variables.
- [`UMPIRE_MATRIX.md`](./UMPIRE_MATRIX.md) controls which environment/model variants a suite
  covers. `AllPaths` is path quantification inside one selected variant, not a replacement for the
  test matrix.
- [`UMPIRE_ERR.md`](./UMPIRE_ERR.md) supplies modeled rejection and divergence outcomes; sparse
  plans name those outcomes rather than asserting transport errors locally.
- [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md) supplies capability-honest realization and replay seams.

## Bottom line

The proposed API is deliberately smaller than the system behind it:

```go
regress.AllPaths(
	interestingKeyFrame,
	interestingAction,
	interestingOutcome,
)
```

The author dots the outline. Symbolic names express identity and data reuse without concrete ids or
Go variable declarations. The current model supplies every omitted resource and causal action.
`OnePath` finds one canonical witness; `AllPaths` checks every finite satisfying semantic route.
Outcomes are implicit assertions, and the global Monitor rules judge the rest.

The hard work is therefore not adding syntax. It is making the model complete enough that a short
instruction list genuinely contains all the information needed to reproduce the behavior of a
large functional test.
