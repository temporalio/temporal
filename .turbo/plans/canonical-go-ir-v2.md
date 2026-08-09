---
status: done
---

# Plan: Canonical Go IR v2

## Context

Umpire's monitored entity catalog, registered fact set, lifecycle planner catalog,
and edge-to-action registries are currently related by convention rather than by
one validated declaration. Add a parallel `tests/umpire/protocolv2` package that
compiles those existing declarations into a canonical, immutable protocol and
derives monitor registration plus lifecycle/action planning from it.

Phase 1 is compatibility-backed and behavior-preserving. It reuses existing
facts, entity factories, lifecycles, actions, and route planning; it does not
modify existing packages, migrate callers, or add relations, properties, or
refinement. The approved design is in
`docs/plans/2026-08-09-canonical-go-ir-v2-design.md`.

## Pattern Survey

### Analogous Features
- `tests/umpire/model/register.go:14` — `DefaultEntity` pairs an existing entity factory with fact subscriptions; `DefaultEntities` is the current canonical five-entity monitor catalog.
- `tests/umpire/model/register.go:63` — `defaultFacts` separately declares the facts registered with `ModelState`, including broadcast/settlement facts not owned by one entity.
- `tests/umpire/model/register.go:88` — `RegisterDefaultEntities` is a thin adapter that registers facts first, then entity factories.
- `tests/umpire/planner/models.go:31` — `DefaultModels` derives the planner lifecycle catalog from `model.DefaultEntities`, filtering through `umpire.Lifecycled`.
- `tests/umpire/action/plan.go:18` — Nexus action resolution already uses the exact discriminators proposed for v2: source state, event, and hosting.
- `tests/umpire/action/plan.go:71` — `planEdge` validates hosting, delegates route construction to the generic planner, and maps every route event plus the target edge to an action.
- `tests/umpire/action/workflow.go:175` — Workflow supplies a second entity-specific action registry while reusing the same entity-agnostic edge planner.
- `tests/umpire/ksdriver/compile.go:37` — `Compile` is an existing pure, server-free compiler boundary that validates declarations/options and returns contextual errors before execution.
- `common/testing/umpire/lifecycle.go:651` — `Lifecycle.Validate` is the existing static-validation gate for reachability, terminals, declared states, and lifecycle traits.
- `tests/umpire/action/kitchensink.go:68` — `ValidateKitchensinkMappings` cross-validates one catalog against another and rejects missing realizations before a drive starts.
- `tests/umpire/action/mutation_gate.go:44` — `ValidateMutationCoverage` distinguishes implemented coverage from consciously deferred gaps and reports unclassified omissions.
- `tests/umpire/model/fact_decoder.go:19` — The runtime decoder maintains its own request/span factory catalog, including explicit aliases where one telemetry event yields multiple facts.

### Reusable Utilities
- `common/testing/umpire/entity.go:62` — `Fact`, `Entity`, and `EntityFactory` — existing compatibility types for canonical fact probes and entity construction.
- `common/testing/umpire/lifecycle.go:709` — `Lifecycled` — type-erased access to an entity’s existing lifecycle.
- `common/testing/umpire/lifecycle.go:446` — `Lifecycle.Initial` — exposes the structural planning root.
- `common/testing/umpire/lifecycle.go:525` — `Lifecycle.EdgeHosting` — reads the existing `HostedIn` trait for cross-catalog hosting checks.
- `common/testing/umpire/lifecycle.go:568` — `Lifecycle.States` and `Lifecycle.Events` — return copied, stable-sorted structural catalog data.
- `common/testing/umpire/lifecycle.go:596` — `Lifecycle.Destination` — resolves an event’s declared destination.
- `common/testing/umpire/lifecycle.go:605` — `Lifecycle.Edges` — enumerates exact `(from, event, to)` edges in stable order.
- `common/testing/umpire/lifecycle.go:651` — `Lifecycle.Validate` — reusable lifecycle validation rather than duplicating graph checks.
- `common/testing/umpire/planner.go:107` — `PlanTo` — existing pure route planner with capability, hosting, event, state, and depth constraints.
- `common/testing/umpire/model_state.go:40` — `ModelState.RegisterEntity` — existing monitor registration seam and entity type/factory consistency check.
- `common/testing/umpire/model_state.go:59` — `ModelState.RegisterFact` — existing fact registration seam and fact name/type consistency check.
- `common/testing/umpire/action.go:57` — `Action` — existing executable action declaration, including mutable precondition, effect, entry, footprint, and rejection fields.
- `tests/umpire/action/action.go:370` — Exported Nexus action values and constructors provide the existing executable realizers without duplicating them.
- `tests/umpire/action/workflow.go:155` — Exported Workflow action values provide the existing executable realizers without duplicating them.
- `tests/umpire/model/register.go:20` — `DefaultEntities` — exported v1 catalog suitable for behavioral/catalog equivalence comparisons.

### Convention Anchors
- Generic/Temporal dependency boundary: domain-agnostic model and planner primitives live in `common/testing/umpire`, while Temporal facts, entities, actions, and catalogs live below `tests/umpire` (`common/testing/umpire/doc.go:1`, `tests/umpire/action/action.go:1`).
- Leaf adapters: generic code does not import Temporal registries; Temporal-side adapters compose generic primitives with concrete models (`common/testing/umpire/planner.go:3`, `tests/umpire/planner/models.go:1`).
- Declaration followed by derivation: the monitor and structural planner already derive from `model.DefaultEntities`; action planning is the remaining separate registry (`tests/umpire/model/register.go:8`, `tests/umpire/planner/models.go:26`, `tests/umpire/action/plan.go:13`).
- Fail-fast static validation: structural inconsistencies are checked without a server before runtime work begins (`tests/umpire/model/lifecycle_validate_test.go:11`, `tests/umpire/action/kitchensink.go:65`).
- Contextual errors: package/function context prefixes errors, and delegated errors are wrapped with `%w` (`tests/umpire/ksdriver/compile.go:39`, `tests/umpire/action/plan.go:81`, `tests/umpire/planner/models.go:58`).
- Exact action resolution: event alone is insufficient; source state distinguishes synchronous from asynchronous completion, and hosting distinguishes standalone from embedded scheduling (`tests/umpire/action/plan.go:18`).
- Explicit unsupported coverage: existing validators treat conscious deferrals separately from accidental omissions, while edge planning reports missing atomic realizations as errors (`tests/umpire/action/mutation_gate.go:17`, `tests/umpire/action/plan.go:91`).
- Fresh structural models: entity factories construct new lifecycle instances, while `DefaultModels` currently retains one representative lifecycle per entity (`tests/umpire/model/nexus_operation.go:30`, `tests/umpire/planner/models.go:31`).
- Defensive structural access: lifecycle collection accessors return copies and graph enumeration is stable-sorted (`common/testing/umpire/lifecycle.go:552`, `common/testing/umpire/lifecycle.go:568`, `common/testing/umpire/lifecycle.go:605`).
- Fact catalogs are presently split: `model.defaultFacts` is unexported and differs from `FactDecoder` registration; notably `ChasmTransition` is decoder-only, while rejection is imported through a dedicated path (`tests/umpire/model/register.go:63`, `tests/umpire/model/fact_decoder.go:44`, `tests/umpire/model/fact_decoder.go:87`).
- Subscription metadata is declaration-only today: `ModelState.RegisterEntity` accepts `subscribesTo` but stores only the factory; runtime routing follows `Fact.TargetEntity`/`BroadcastType` (`common/testing/umpire/model_state.go:40`, `common/testing/umpire/model_state.go:75`).
- Registration probes concrete types: entity and fact identity are checked through pointer element struct names, with invalid declarations currently causing panics (`common/testing/umpire/model_state.go:38`, `common/testing/umpire/model_state.go:57`).
- Test structure: package-level behavior is commonly tested through external `_test` packages, table-driven cases, and `testify/require`; lower-level validation tests stay in-package (`tests/umpire/action/plan_test.go:1`, `common/testing/umpire/lifecycle_test.go:209`).
- No reusable action deep-copy helper exists in the surveyed Umpire packages; current action sequences copy the outer slice but retain nested declaration slices (`tests/umpire/action/fault.go:141`, `tests/umpire/action/schedule.go:61`).

### Proposed Alignment
Blend the established patterns: keep `protocolv2` as a Temporal-side leaf that owns declaration/validation while delegating lifecycle structure, route finding, registration, and executable realization to existing types. The existing patterns leave reconciliation of the split fact catalogs, explicit action gaps, and defensive copying of mutable `Action` fields within the new package boundary.

## Implementation Steps

1. **Define the protocol declaration and compiler through validation-first tests**
   - Add `tests/umpire/protocolv2/protocol.go` with `Declaration`,
     `EntityDeclaration`, `ActionKey`, `ActionBinding`, `ActionGap`, and the
     unexported compiled indexes held by `Protocol`.
   - Add in-package fixtures and table-driven cases to
     `tests/umpire/protocolv2/compile_test.go` before implementing the compiler.
     Cover valid monitor-only and active entities plus duplicate facts/entities,
     nil factories, factory/type mismatches, missing subscribed facts, invalid
     lifecycles, unknown edges, duplicate bindings, overlapping gaps, hosting
     mismatches, missing matching effects, and empty gap reasons.
   - Add `tests/umpire/protocolv2/compile.go` with `Compile`. Validate fact and
     entity concrete types before calling the existing `ModelState` registration
     methods so malformed declarations return contextual `protocolv2` errors
     instead of reaching their panic paths.
   - Reuse `umpire.Lifecycled`, `Lifecycle.Validate`, `Lifecycle.Edges`, and
     `Lifecycle.EdgeHosting` for structural validation. Normalize actions and
     gaps into exact `ActionKey` maps; do not introduce wildcard keys or a second
     graph representation.
   - Implement a private action clone helper that copies `Requires`, `Effects`,
     `Entry`, `Footprint`, and `Reject` while retaining the opaque `Realize`
     implementation. Exercise it with mutation-after-compile and
     mutation-after-read tests.

2. **Add the monitor and structural-planning adapters**
   - Add `tests/umpire/protocolv2/monitor.go` with `(*Protocol).Register`, keeping
     the existing registration order: decoder facts first, then each entity
     factory with its subscriptions.
   - Add `tests/umpire/protocolv2/planner.go` with
     `(*Protocol).Lifecycle`, `(*Protocol).Action`, and
     `(*Protocol).PlanTo`. Construct a fresh entity/lifecycle on every lifecycle
     request and delegate routing to `umpire.PlanTo`.
   - Extend `compile_test.go` or add focused `monitor_test.go` and
     `planner_test.go` cases for unknown and monitor-only entity errors, fresh
     lifecycle instances, defensive action returns, successful entity creation
     through a registered `ModelState`, and unchanged planner constraints.
   - Keep all accessors read-only. Do not add caching, global mutable state, or
     exported catalog maps.

3. **Implement protocol-backed edge-to-action planning**
   - Add `(*Protocol).PlanEdge` to
     `tests/umpire/protocolv2/planner.go`, mirroring the existing
     `tests/umpire/action.planEdge` flow: check target-edge hosting, plan the
     prefix route to `from`, resolve each exact edge key, advance through
     `Lifecycle.Destination`, and append the target action.
   - Require concrete hosting for action planning where standalone and embedded
     bindings differ. Return contextual errors containing entity, source state,
     event, and hosting for unknown entities, invalid edges, explicit gaps, and
     missing bindings.
   - Add table-driven planner tests using local fake declarations to pin route
     assembly, exact hosting selection, gap errors, missing-action errors, and
     propagation of existing route-planner failures.

4. **Build the default structural catalog and NexusOperation action matrix**
   - Add `tests/umpire/protocolv2/default.go` with a private
     `defaultDeclaration` and public `Default() (*Protocol, error)`. Build a fresh
     compiled protocol per call; do not introduce a singleton.
   - Declare exactly the five factories and subscriptions currently returned by
     `model.DefaultEntities`. Mirror the fact probes from
     `model.defaultFacts` as the v2 monitor-registration set; do not absorb
     `FactDecoder`-only entries such as `ChasmTransition` or change rejection
     registration behavior.
   - Declare exact NexusOperation bindings for both relevant hostings using the
     existing exported `action` values and constructors: standalone/embedded
     scheduling, async acknowledgement, retryable attempt failure, synchronous
     and asynchronous settlement, forced timeout, and standalone termination.
   - Represent every unsupported result in the characterized v1
     `(from, event, hosting)` matrix as an `ActionGap` with a reason. Preserve the
     observed matrix even if it exposes a mismatch with current comments; do not
     repair v1 semantics in this phase.
   - Add in-package `default_test.go` coverage comparing entity types,
     subscriptions, lifecycle states/events/edges/traits/terminals, and active
     binding/gap classification against the current catalogs.

5. **Add Workflow bindings and public behavioral equivalence tests**
   - Extend `defaultDeclaration` with exact Workflow bindings using
     `action.StartWorkflow` and `action.CompleteWorkflow`. Leave WorkflowRun,
     TaskQueue, and WorkflowTask monitor-only.
   - Add external-package
     `tests/umpire/protocolv2/equivalence_test.go` tests that enumerate Workflow
     and NexusOperation edges and relevant hostings, call the public v1 and v2
     planning APIs, and compare success/error classification.
   - For successful plans, compare complete declarative action content with
     `require.Equal`: names, kinds, hosting, preconditions, effects, entries,
     footprints, and rejection metadata. Compare realizer concrete types rather
     than internal values.
   - Include the existing high-value routes from `action/plan_test.go` and
     `action/workflow_test.go`, then extend the matrix to all current edges so the
     test detects omissions rather than only spot-checking known paths.

6. **Prove monitor equivalence and close the Phase 1 catalog**
   - In `equivalence_test.go`, register separate v1 and v2 `ModelState` instances,
     feed representative targeted and broadcast facts through both, and compare
     created entity types plus lifecycle outcomes. Reuse real fact types and
     existing entity query methods instead of adding test-only inspection APIs.
   - Add a default-protocol completeness test proving every declared entity
     subscription belongs to the v2 registration fact set and every active
     edge/hosting combination is either bound or explicitly gapped.
   - Confirm the implementation changes are confined to
     `tests/umpire/protocolv2`; leave `common/testing/umpire`, `tests/umpire/model`,
     `tests/umpire/action`, `tests/umpire/planner`, and their comments unchanged.

## Verification

- Run `go test -tags test_dep ./tests/umpire/protocolv2`; expect compiler,
  adapter, default-catalog, and v1/v2 equivalence tests to pass.
- Run the focused existing baselines with
  `go test -tags test_dep ./tests/umpire/action ./tests/umpire/model ./tests/umpire/planner`;
  expect no v1 behavior regressions.
- Run `make fmt-imports`; expect only new `tests/umpire/protocolv2` Go files to
  change.
- Run `make lint-code`; expect no formatting, static-analysis, or test-lint
  failures.
- Inspect `git diff --name-only`; expect no existing Umpire source file to be
  modified.

## Verification Results

- `go test -count=1 -tags test_dep ./common/testing/umpire ./tests/umpire/rule ./tests/umpire/protocolv2 ./tests/umpire/action ./tests/umpire/model ./tests/umpire/planner` passed, including HTTP-level v1/v2 async-completion payload parity.
- `go test -count=1 -race -tags test_dep ./tests/umpire/protocolv2 -run TestDefaultProtocolSupportsConcurrentReads` passed.
- The configured linters passed for `./tests/umpire/rule/...` and, with a fresh analyzer cache, `./tests/umpire/protocolv2/...` with zero issues. Explicit forwarding methods on the rule contexts work around golangci-lint's stale dependency analysis while preserving the rule API.
- `make fmt-imports` completed; its unrelated repository-wide rewrites were restored, and the same formatter was applied directly to `tests/umpire/protocolv2`.
- The rule-related blocker and the stale `service/frontend/workflow_handler_test.go` constructor call are resolved; the focused frontend suite passes.
- The exact `make lint-code` command now reaches analysis completion and reports 253 pre-existing branch-wide findings because its default `--new-from-rev=main` scope includes unrelated WIP across the repository. Its automatic unrelated rewrites were restored.
- `make GOLANGCI_LINT_FIX=false GOLANGCI_LINT_BASE_REV=HEAD lint-code` passes with zero task-scoped issues, including the repository vet phase.
- The remaining worktree diff contains only the approved rule-context compatibility change, its rule-test lint fixes, the frontend test call-site repair, and this verification update.

## Context Files

Files to read in full before starting implementation:

- `docs/plans/2026-08-09-canonical-go-ir-v2-design.md` — approved scope,
  interface, validation rules, and exit criteria.
- `common/testing/umpire/entity.go` — compatibility fact/entity interfaces and
  factories.
- `common/testing/umpire/lifecycle.go` — lifecycle structure, stable graph
  accessors, hosting traits, validation, and `Lifecycled`.
- `common/testing/umpire/action.go` — action declaration shape and mutable fields
  that require defensive copying.
- `common/testing/umpire/model_state.go` — monitor registration ordering,
  concrete-type checks, and fact routing behavior.
- `common/testing/umpire/planner.go` — structural `PlanTo` behavior and planning
  constraints reused by v2.
- `tests/umpire/model/register.go` — current entity subscriptions and exact
  ModelState fact registration set.
- `tests/umpire/model/fact_decoder.go` — separate decoder factory catalog that
  must not be conflated with ModelState fact registration.
- `tests/umpire/planner/models.go` — current lifecycle catalog derivation and
  unknown-entity behavior.
- `tests/umpire/action/plan.go` — current Nexus action-resolution matrix and
  entity-agnostic edge-planning algorithm.
- `tests/umpire/action/action.go` — exported Nexus action values and constructors.
- `tests/umpire/action/workflow.go` — Workflow bindings and exported actions.
