# Umpire package design review

## Scope and standard

This review covers the current working tree under `common/testing/umpire` and
`tests/umpire2`, plus direct callers needed to determine ownership. It evaluates:

1. whether reusable framework concepts live under `common/testing/umpire` and Temporal-specific
   behavior lives under `tests/umpire2`;
2. whether packages and files have coherent ownership;
3. whether modules are deep: a small interface hiding substantial behavior; and
4. whether implementation used by one main module is nested or otherwise hidden behind it.

The important measure is caller knowledge, not lines of code. A package is deep when callers can
exercise a large amount of behavior through a small interface and tests can verify that behavior at
the same seam. An `internal` directory is useful only when it hides a real implementation cluster;
creating many tiny public packages would make the structure shallower.

## Executive verdict

The high-level split is directionally correct, but the package design is not yet deep enough.

| Criterion | Verdict | Main reason |
| --- | --- | --- |
| Framework versus Temporal ownership | Mostly correct | Dependencies point from `tests/umpire2` to `common/testing/umpire`, but the generic root still owns a Temporal payload domain and Temporal logging. |
| Package structure | Needs consolidation | Several packages are pass-throughs, catalogs, or implementation details of one Temporal facade. |
| Deep modules | Needs substantial work | `common/testing/umpire` exposes 139 top-level identifiers; `Monitor` then exposes its stores, causing callers to program against the implementation. |
| Nested ownership | Needs substantial work | Model, fact decoding, rules, assurance, kitchen-sink execution, and regression execution are public siblings even though they mainly implement Umpire2. |

The strongest existing deep module is `tests/umpire2/protocol`: it compiles and validates a
declaration, keeps indexed state private, and returns defensive copies. The preferred direction is
to make that pattern the norm: a few public entry points, with model, observation, rule, execution,
and tool-specific structures hidden behind them.

## Evidence from the current tree

Production Go code, excluding tests and generated protobuf code:

| Package | Files | Lines | Top-level exported identifiers | Assessment |
| --- | ---: | ---: | ---: | --- |
| `common/testing/umpire` | 17 | 4,710 | 139 | Generic catch-all; far too much caller knowledge |
| `common/testing/umpire/regress` | 7 | 3,334 | 67 | Correct domain, broad compiler/runtime seam |
| `common/testing/umpire/verify` | 9 | 3,511 | 69 | Correct domain, but schema, projection, interpretation, and persistence share one interface |
| `common/testing/umpire/verify/runner` | 6 | 2,536 | 9 | Substantial implementation with a reasonable entry point, but owned only by the generator command |
| `common/testing/umpire/campaign` | 1 | 1,075 | 16 | Generic concept, but currently speculative: no production caller |
| `common/testing/umpire/canary` | 1 | 459 | 13 | Generic concept, but currently speculative: no production caller |
| `tests/umpire2` | 4 | 725 | 4 | Promising facade, but it leaks framework stores |
| `tests/umpire2/action` | 16 | 3,537 | 58 | Temporal-specific as intended, but combines several independently changing concerns |
| `tests/umpire2/protocol` | 23 | 5,117 | 20 | Cohesive and comparatively deep; authoring internals are unnecessarily exported |
| `tests/umpire2/fact` | 23 | 1,522 | 39 | Correctly Temporal-specific, but mostly an implementation of observation/modeling |
| `tests/umpire2/model` | 14 | 1,802 | 40 | Correctly Temporal-specific, but concrete model state leaks to callers |
| `tests/umpire2/rule` | 8 | 663 | 8 | Correctly Temporal-specific; only the assurance catalog consumes it in production |
| `tests/umpire2/planner` | 2 | 105 | 10 | Shallow re-export plus a second Temporal model catalog |
| `tests/umpire2/ksdriver` | 2 | 169 | 4 | One kitchen-sink acceptance-test adapter placed at the top level |

Several files are also large enough to impede locality: `regress/compiler.go` is 1,271 lines,
`verify/family.go` is 1,202, `campaign/campaign.go` is 1,075,
`verify/runner/runner.go` is 1,072, `verify/runner/native_trace.go` is 974,
`action/regression_observation.go` is 738, and `action/action.go` is 657. File size does not by
itself make a module shallow, but these files contain multiple phases or policies that change for
different reasons.

## What is already right

### The dependency direction is mostly sound

`tests/umpire2` imports the generic framework; the generic framework does not import Umpire2. The
following concepts are appropriately placed in `common/testing/umpire`:

- facts and entity identity;
- lifecycle transition semantics;
- relation cardinality and storage semantics;
- rule evaluation and violation values;
- structural planning and action execution semantics;
- evidence profiles, normalized traces, coverage, and constrained matrices;
- sparse regression compilation/execution; and
- bounded formal-model representation and interpretation.

Temporal request/response decoding, Temporal entity models, Nexus/workflow actions, Temporal rules,
and protocol declarations are appropriately under `tests/umpire2`.

### `protocol.Protocol` hides useful complexity

`tests/umpire2/protocol` is the best-shaped package in the reviewed scope:

- `Compile` validates facts, entity factories, lifecycles, action bindings, gaps, relations,
  regression declarations, and causal footprints before runtime;
- `Protocol` owns private indexes and declaration order;
- methods return defensive copies; and
- planning, coverage, runtime registration, matrix generation, regression projection, and formal
  verification all derive from the same compiled value.

Deleting this module would spread validation and catalog logic across the monitor, planner,
actions, generator command, and tests. It is earning its keep.

### Several implementations are already hidden well

The unexported `evidenceIngestor` and `executionTrace` in `tests/umpire2` are good examples. They
contain meaningful orchestration behind `Monitor` and are tested through observable behavior.
Similarly, backend-specific files under `verify/runner` keep process and trace parsing details out
of the core verification model, even though their package ownership can be improved.

## Priority findings

### P0: stop exposing the Monitor's storage implementation

`tests/umpire2.Monitor` has useful high-level operations—`CheckNamespace`,
`CheckNamespaceSafety`, and `PurgeNamespace`—but also returns raw mutable implementation objects
through `ModelState()`, `FactLog()`, and `Relations()`. The `tests/testcore/monitor.Monitor`
interface institutionalizes two of those getters. Callers then:

- type-assert concrete `model.Workflow`, `model.NexusOperation`, and `model.WorkflowRun` values;
- query generation counters directly;
- inspect relation store internals; and
- reconstruct semantic observations from fact logs.

This makes `ModelState`, `FactLog`, `RelationStore`, concrete Temporal entities, facts, and registry
keys part of the effective Monitor interface. An internal refactor of any one of them requires
changes in `tests/probe`, `tests/umpire_test.go`, `tests/umpire_probe_test.go`, and the regression
harness.

Replace the getters with scope-aware semantic queries or a defensive snapshot, for example:

```go
type Snapshot struct {
    Entities  []EntitySnapshot
    Relations []RelationSnapshot
    Facts     []FactSnapshot
}

func (m *Monitor) Snapshot(namespaceID string) Snapshot
func (m *Monitor) Await(ctx context.Context, namespaceID string, predicate Predicate) error
```

The exact interface should be designed from existing caller queries, not as a generic dump of
every internal field. State mutation and fact routing should remain private. Move
`RequireRulePassed(testing.TB, ...)` into a test helper; the runtime Monitor should return data or
errors rather than own assertions.

### P0: make Umpire2 one deep Temporal module instead of a federation of internals

`fact`, `model`, `rule`, and `assurance` are not independent user-facing modules. Together they
implement the default Umpire2 protocol and Monitor:

- `model` consumes `fact`;
- `protocol` consumes `model`, `fact`, and `action`;
- `rule` consumes `model` and `protocol`; and
- `assurance` exists to register `rule` implementations and project their inventory.

Only `Monitor` and `cmd/umpire-genmodels` consume `assurance` in production. Only `assurance`
consumes `rule` in production. External use of `model` is primarily a consequence of the Monitor
store leak described above.

Preferred end state: callers import `tests/umpire2` for monitoring, planning, execution, coverage,
and verification projection. Concrete facts, entities, rules, decoder tables, relation derivation,
and assurance registration become unexported files in that module or packages below a shared
`tests/umpire2/internal` directory. Keep a separate public subpackage only for a genuine authoring
language whose names are useful to test authors.

Do not mechanically create `internal/fact`, `internal/model`, and `internal/rule` if that merely
preserves the current shallow seams. First define the small Umpire2 interface, then choose internal
packages only for implementation clusters that can be tested through narrow internal seams.

### P0: split the generic root by interface, not by filename

`common/testing/umpire` currently presents model state, lifecycle construction, rule registration,
planning, action realization, input mutation, coverage, evidence profiles, tracing, OTEL
instrumentation, and gRPC interception as one package interface. Its 139 exported top-level
identifiers make it a toolbox rather than a deep module.

Introduce a generic runtime facade that owns the stores and orchestration:

```go
engine, err := umpire.New(protocol, options)
err = engine.Ingest(ctx, facts...)
result := engine.Check(ctx, scope, umpire.Final)
engine.Purge(scope)
```

Domain extensions should depend on small read-only contexts rather than concrete stores. For
example, rules need semantic entity and relation queries, not a mutable `ModelState` and
`RelationStore`. The engine can keep state routing, dirty generations, conformance deduplication,
fact retention, relation mutation, coverage, and trace recording private.

Preserve the genuinely reusable value vocabulary—facts, entity paths, lifecycle specifications,
violations, action declarations, evidence profiles—but remove construction and mutation methods
that only the engine needs.

### P1: remove concrete Temporal dependencies from the generic root

The package comment calls `common/testing/umpire` Temporal-agnostic, but:

- `action_domain.go` imports `go.temporal.io/api/common/v1` for `PayloadDomain`; and
- `rule_registry.go` imports Temporal's server `log` and `tag` packages.

`PayloadDomain` has one production consumer, `tests/umpire2/action/reject.go`; move it beside that
Temporal protobuf reflection logic. `CanonicalProtoDigest` is protobuf-generic, but all of its
production consumers are Umpire/Temporal code. It can either remain in a small generic protobuf
adapter or move behind a Umpire2 normalization interface.

No Umpire2 rule reads `ruleContext.Logger`; the framework uses it only to log returned violations.
Remove logging from rule evaluation and let the Temporal Monitor log the violations it receives.
This both removes the Temporal dependency and follows the preferable shape of returning results
instead of producing an extra side effect.

gRPC and OTEL are general technologies, but they are adapters rather than the model-testing core.
Place their implementations under clearly owned adapter packages, or expose them only through the
Temporal facade if there is no second direct consumer. Today `umpire2.NewUnaryServerInterceptor`
is a pass-through while testcore bypasses it and calls the common implementation directly; choose
one seam.

### P1: collapse shallow and duplicate packages

The following fail the deletion test or have only one narrow owner:

- `tests/umpire2/planner/planner.go` is almost entirely aliases and variables re-exporting the
  generic planner. `Models` also builds a second catalog from `model.DefaultEntities`, while the
  canonical `protocol.Protocol` already provides `PlanTo`, `Lifecycle`, and executable
  `PlanEdge`. Use the compiled protocol and remove the wrapper/catalog.
- `tests/umpire2.NewUnaryServerInterceptor` delegates directly to
  `common/testing/umpire.NewUnaryServerInterceptor`. Either make the Temporal wrapper the owned
  seam or remove it.
- `ModelState.RegisterEntity(factory, subscribesTo ...Fact)` does not use `subscribesTo`; removing
  that parameter would not change runtime behavior. Subscription validation belongs in the
  compiled protocol, and the runtime interface should not pretend to own it.
- `tests/umpire2/entity_key.go` has no current caller. Remove it or keep key construction inside the
  semantic query implementation rather than as an unused public builder.
- `tests/umpire2/ksdriver` is used only by the kitchen-sink acceptance cases in
  `tests/umpire_test.go`. Nest route compilation and execution under the kitchen-sink/action
  implementation and expose one scenario-running operation.
- `tests/umpire2/regress/rpc` is an eight-line name wrapper. Fold those names into the Nexus
  authoring vocabulary unless another regression domain needs a transport-neutral namespace.

### P1: deepen `tests/umpire2/action`

The action package combines at least five modules:

1. the canonical Temporal action catalog and edge planning;
2. the live `Ctx`/`Oracle`/`Resolver` drive adapter and Nexus response policy;
3. fault learning, variants, and scheduling;
4. protobuf mutation and rejection domains; and
5. the sparse-regression environment, harness, observations, and realizations.

Its external interface includes six environment interfaces, `Ctx`, `Oracle`, `Resolver`,
`ResponsePolicy`, regression harness construction, dozens of action globals/constructors, and
separate validation helpers. Tests must manually assemble those pieces and call the generic
`Drive` loop.

Provide one high-level Temporal execution operation that owns policy, bindings, polling, endpoint
confirmation, and cleanup. Keep the canonical action declarations available to the protocol, but
hide realization types. Move sparse regression execution behind a Temporal `RunRegression` facade;
its current `RegressionEnvironment` even exposes `*testing.T`, which is a test-runner detail rather
than execution capability.

The small capability-specific environment interfaces are useful internal seams. They should stay
internal unless callers really provide independent adapters. The current functional environment
and its test fake justify a seam; exporting every capability from the authoring package does not.

### P1: hide verification runners and backend generators behind `verify`

`verify/fizz`, `verify/ivy`, `verify/p`, `verify/tla`, and `verify/runner` are directly consumed in
production only by `cmd/umpire-genmodels` (the runner also composes the generators). The command
therefore knows backend identifier rules, generated files, tool paths, process plans, native trace
formats, and result classification.

Keep the generic verification model in `common/testing/umpire/verify`, but provide a small facade
such as `Generate`, `PlanChecks`, and `Check`. Nest backend renderers, process execution, and native
trace decoders under `verify/internal`. Backend-specific mutation tests should move with those
implementations; command tests should verify command behavior through the facade.

This preserves reusable formal verification while making the command ignorant of implementation
layout.

### P2: decide whether `campaign` and `canary` are real modules

Their concepts are general-purpose and therefore belong under `common/testing/umpire` if retained.
Both have a high-level `Run`, meaningful validation, and substantial hidden implementation. But
neither has a production caller outside its own package tests, so their environment seams and
request structures are hypothetical.

Either integrate each through a real Umpire2 adapter and test it at that seam, or remove/defer it
until a caller exists. Do not grow these interfaces based solely on anticipated environments.

### P2: separate generated artifacts from source packages

`tests/umpire2/genmodels` is not a Go package, but it sits beside source packages and contains 134
tracked generated files. Prefer `tests/umpire2/testdata/genmodels` for checked deterministic
artifacts and a temporary or ignored `testdata/results` location for tool-run output. This makes
source ownership explicit and lets Go tooling naturally ignore the artifact tree.

## Preferred target structure

The exact moves must preserve an acyclic Go import graph, but the ownership should converge on the
following shape:

```text
common/testing/umpire/
  engine.go                    # small generic runtime facade
  protocol.go                  # minimal extension vocabulary and read-only contexts
  lifecycle.go                 # reusable semantic values, not mutable engine stores
  action.go                    # reusable action declaration values
  internal/
    state/                     # entity, fact, relation, and generation storage
    rules/                     # rule scheduling, liveness, conformance, deduplication
    trace/                     # normalized trace implementation
  adapters/
    grpc/                      # only if direct generic consumers remain
    otel/                      # only if direct generic consumers remain
  regress/
    regress.go                 # authoring + compile/run facade
    internal/{normalize,compile,execute,artifact}/
  verify/
    verify.go                  # model/family + projection/check facade
    internal/{project,interpret,runner,backend}/
  campaign/                    # retain only with a real adapter
  canary/                      # retain only with a real adapter

tests/umpire2/
  umpire.go                    # Temporal facade: construct, observe, check, purge, snapshot
  protocol.go                  # default planning/coverage/verification operations
  action.go                    # high-level Temporal execution operations
  regression.go               # high-level Temporal regression execution
  internal/
    observation/               # gRPC, OTEL, history, and in-process decoding
    model/                     # Temporal entity implementations and relations
    assurance/                 # Temporal properties and rule registration
    drive/                     # live action realizations, faults, rejection mutation
    kitchensink/               # protobuf, worker, route compiler, client executor
  regress/
    activity/                  # public typed authoring vocabulary
    capability/                # public typed authoring vocabulary
    nexus/                     # public typed authoring vocabulary; include RPC names
    workflow/                  # public typed authoring vocabulary
  testdata/genmodels/          # deterministic generated verification artifacts
```

This is an ownership sketch, not a requirement to preserve each listed internal directory. If two
internal directories would exchange most of their types, merge them. The goal is three small
external seams—generic engine, Temporal facade, and test-authoring vocabulary—not a larger package
count.

## File organization within the retained modules

- Keep one file per stable responsibility, not one file per type and not one thousand-line phase
  pipeline. File names should let a maintainer find validation, compilation, execution, and
  persistence without scanning unrelated code.
- Split `regress/compiler.go` by compilation phase, but keep helpers unexported and in the same
  package unless a narrow independently tested seam emerges.
- Split `verify/family.go` into declaration validation, target projection, closure, and ownership
  implementation files. Do not export those phases.
- Split `verify/runner/runner.go` into planning, invocation, and classification implementation;
  keep `Check` as the external operation.
- Split `action/action.go` into canonical declarations, live context, response policy, realizers,
  and plan constructors. Then hide the latter four behind the high-level execution operation.
- Split `action/regression_observation.go` by semantic projection (workflow, Nexus, callback,
  activity) while keeping a single regression-path interface.
- The existing protocol file names are generally good: coverage, matrix, planning, regression,
  relations, and verification are discoverable. Prefer unexporting declaration/compiler details
  over moving each file into a new public package.
- Add package documentation to retained public packages (`fact`, `model`, and `rule` currently have
  none if they remain public). Internal packages need only concise ownership comments where the
  package name is not self-explanatory.

## Migration order

1. **Lock the desired dependencies.** Extend `tests/umpire2/layout_test.go` or add equivalent AST
   checks for forbidden generic-to-Temporal imports and forbidden external imports of new internal
   packages. Add interface-level tests around Monitor check/purge, planning, action execution, and
   verification generation.
2. **Deepen Monitor first.** Inventory every `ModelState`, `FactLog`, and `Relations` query; add the
   smallest semantic snapshot/query operations that cover them; migrate callers; remove the raw
   getters and `testing.TB` assertion method.
3. **Make the compiled protocol the only Temporal catalog.** Migrate planner callers to
   `Protocol.PlanTo`/`PlanEdge`, remove `tests/umpire2/planner`, and make declaration/compiler types
   private where no external caller exists.
4. **Deepen Temporal execution.** Add one action-running facade, move `Ctx`, `Oracle`, `Resolver`,
   response policy, kitchen-sink machinery, and regression harness behind it, then collapse
   `ksdriver` and the unused key builder.
5. **Internalize Temporal implementation.** Once external model/fact/rule imports are gone, hide or
   merge those packages under the Umpire2 facade. Move assurance inventory onto the compiled
   protocol/facade.
6. **Deepen the generic engine.** Hide mutable stores and rule scheduling, remove Temporal logging,
   move `PayloadDomain`, and establish explicit gRPC/OTEL adapter ownership.
7. **Deepen verification and regression.** Internalize compiler phases, completed execution
   machinery, backend renderers, process execution, and native trace decoding without changing
   artifact formats.
8. **Resolve speculative modules and artifacts.** Integrate or remove campaign/canary, then move
   generated models under `testdata` without changing deterministic contents.

Each step should preserve existing comments when moving code, keep old interfaces only for the
shortest practical migration window, and delete compatibility wrappers once all callers move.

## Verification and failure modes

### Tests to preserve the seams

- generic engine tests ingest facts and assert snapshots/violations without accessing stores;
- protocol tests compile invalid declarations and exercise planning/coverage/verification through
  the compiled interface;
- Monitor tests cover best-case ingestion, decode failure, relation conflict, partial evidence,
  namespace purge, and concurrent scopes;
- action tests run through the new execution facade with a local fake environment;
- regression tests verify cleanup on setup failure, action failure, cancellation, timeout, and
  artifact-write failure;
- verification tests compare every backend through the same facade, including malformed native
  evidence and missing toolchains; and
- layout tests ensure generic code cannot import Temporal-specific packages and external callers
  cannot reach Umpire2 internals.

Run affected unit tests with `-tags test_dep`, then `make fmt-imports` and `make lint-code` for code
moves. Generated-model moves also require the existing generator determinism checks.

### Trade-offs

- **Performance:** facade calls should be zero- or low-overhead delegation. Defensive snapshots
  introduce copying; keep them namespace-scoped and provide targeted queries for hot polling paths
  rather than copying global state.
- **Scalability:** a 10x increase in observations should remain batch-ingested under engine-owned
  locking. Do not expose locks or mutable maps to optimize callers; add indexed semantic queries
  inside the engine when measurements require them.
- **Complexity:** the migration is substantial, but the end state removes duplicate catalogs,
  pass-through packages, manual runtime assembly, and cross-package knowledge. Avoid a big-bang
  directory move; deepen one callable seam at a time.
- **Security and evidence retention:** hiding raw facts, protobuf payloads, process invocations, and
  tool output reduces accidental retention and unsafe command construction. Public snapshots must
  remain normalized and non-secret.
- **Crashes and cancellation:** package moves must not weaken the current reverse-order cleanup,
  uncancelled cleanup context, per-path isolation, or artifact flush behavior. These are runtime
  invariants and belong inside the deep regression/canary modules.

## Definition of done

The redesign is complete when:

- `common/testing/umpire` has no Temporal application imports and exposes a small runtime seam;
- a normal Temporal test needs only the Umpire2 facade plus an optional typed authoring vocabulary;
- no external caller receives `ModelState`, `FactLog`, `RelationStore`, concrete Temporal entities,
  or mutable protocol catalogs;
- the compiled protocol is the sole source for monitoring, planning, action lookup, coverage,
  regression compilation, and verification;
- shallow wrappers (`planner`, `ksdriver`, interceptor pass-throughs, unused key builders) are gone;
- backend/tool and kitchen-sink implementation is nested behind its single owner;
- campaign/canary each has a real adapter or is absent; and
- tests exercise behavior through the same interfaces used by callers rather than reaching past
  them.
