# Umpire Developer Experience

Date: 2026-08-15

Status: Analysis and recommendations; no interface changes are implemented here.

## Scope

This review follows the developer-facing paths exercised by:

- `tests/umpire_test.go`
- `tests/umpire_probe_test.go`
- `tests/umpire_regress_test.go`

It traces those calls into `common/testing/umpire`, `common/testing/umpire/regress`, `tests/probe`, and the `tests/umpire2` protocol, planner, action, and kitchensink packages.

## Summary

Umpire's core is already flexible. Its generic planner, action runtime, sparse compiler, execution harness, observation interfaces, and environment profiles provide useful low-level seams. The main developer-experience problem is that ordinary tests must assemble too many of those modules themselves.

The root tests currently expose at least four overlapping authoring interfaces:

1. structural planning plus a bespoke `Driver`;
2. low-level `Action` execution through `Drive` and `Reconcile`;
3. the fluent `probe.Umpire` facade;
4. the sparse regression DSL plus explicit compiler and harness setup.

There is also a kitchensink adapter for planned routes. Each interface is individually defensible, but there is no canonical starting point and their defaults do not consistently come from the canonical `protocol.Protocol`. As a result, callers routinely wire protocol selection, environments, timeouts, polling, identity lookup, reconciliation, coverage, teardown, and assertions.

The recommended direction is:

- preserve the explicit, Temporal-independent core as the escape hatch;
- make the compiled `tests/umpire2/protocol.Protocol` the single catalog behind every Temporal convenience interface;
- add one author-facing Temporal test module that owns the common session, execution, observation, and cleanup policy while delegating to the existing planner, regression, and campaign modules;
- make the sparse domain vocabulary the preferred 99% authoring interface;
- retain custom environment, realizer, oracle, observer, fault, and profile seams for the remaining cases;
- record every default, bound, skipped case, and unsupported capability in the result rather than silently hiding it.

Flexibility should come from composable seams in the core, not from forcing every caller to perform the composition. Convenience should select reasonable policies without erasing them. It should reuse `regress` and `campaign`, not create another planner or exploration engine beside them.

## Design principles

### Keep three deliberate levels

The developer interface should have three levels, each implemented as an adapter over the level below:

| Level | Intended caller | Interface shape |
| --- | --- | --- |
| Flexible core | Framework and unusual environment authors | Error-returning planner, compiler, runtime, observer, oracle, and harness interfaces with no dependency on `testing` |
| 90% Temporal runner | Tests needing a custom plan, environment, profile, or assertion | Canonical protocol plus an explicit request/result and override options |
| 99% test affordance | Ordinary semantic acceptance and regression tests | `testing.TB`-aware helpers with canonical defaults, isolated environments, automatic judging, and useful diagnostics |

The 99% interface must not become a second implementation. It should build requests for the 90% runners. Those runners should use the existing core interfaces. A single facade package may expose several coherent operations—run a sparse regression, execute an action plan, run a campaign—without forcing them into one universal request type.

### Defaults must be inspectable

A convenience interface may choose the canonical protocol, shortest route, local in-process evidence, CHASM enabled, serial execution, standard polling, a timeout, and a bounded fault budget. The returned result or retained artifact should state those choices. It should also state any calls, edges, paths, or capabilities omitted by a bound.

This lets the common case ignore edge cases without making them invisible.

Infrastructure failure and semantic outcome must also stay separate. Invalid configuration, environment allocation failure, and cleanup failure are Go errors. Recovered, degraded, violated, unsupported, and inconclusive claims are structured results. A `testing.TB` adapter may require a conforming result for the ordinary case, while model and probe tests must be able to inspect non-conforming results intentionally.

The runner contract should make each phase unambiguous:

| Condition | Core runner behavior | Ordinary test affordance |
| --- | --- | --- |
| Invalid protocol, plan, preset, capability, or bound | Return an error before allocating an environment | Fail immediately with the contextual error |
| Unexpected realization, transport, observation, or artifact failure | Return the partial result and an error | Fail after registering/finishing cleanup |
| Expected modeled failure or degradation | Return a qualified semantic result, no infrastructure error | Apply the helper's declared assertion policy |
| Rule or reconciliation violation | Return a violated claim and diagnostics | Fail for conformance helpers; remain inspectable for probe/model tests |
| Unsupported or inconclusive evidence | Return that claim status and reason | Never report success; fail or skip only according to an explicit helper policy |
| Cleanup failure | Mark cleanup incomplete and return/join an error | Fail even if the semantic claim passed |

### Keep domain vocabulary typed and mechanics hidden

The sparse Nexus, Workflow, Activity, capability, and RPC packages are the strongest existing authoring pattern. Calls such as `nexus.State`, `nexus.RespondStart`, `workflow.State`, `nexus.StartToClose`, and `nexus.HandlerWorkflow` hide schema construction while preserving typed normalization and compile-time package ownership.

That pattern should expand upward to targets, plans, and test execution. Callers should normally name semantic intent. Endpoint creation, worker registration, identity binding, observation polling, reconciliation, and cleanup are realization mechanics and belong behind a deep module.

## Current authoring paths

### Plan, bespoke drive, bespoke judge

The basic workflow test performs the entire lifecycle manually:

1. create an environment and register a workflow;
2. call `planner.DefaultModels().PlanTo("Workflow", "completed", planner.Shortest, planner.Constraints{})`;
3. implement and instantiate a `workflowDriver`;
4. call `plan.Run`;
5. build a namespace entity key;
6. poll `ModelState.QueryEntities` with hard-coded intervals;
7. inspect the concrete entity and lifecycle;
8. call `CheckNamespace`.

This is a good framework integration test because it demonstrates each seam. It is not a good template for ordinary test authors. The module has low depth at this call site: the test knows almost as much about orchestration as the implementation.

The two kitchensink tests remove the bespoke driver but still require registration, a large `RunOptions` value, entity strings, polling, entity inspection, and explicit judging. `ksdriver.RunPlan` is useful, but it only deepens the drive step.

### Low-level action runtime

The probe tests repeat the following assembly:

```go
policy := action.NewResponsePolicy()
endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())
rc := action.NewCtx(env.TestEnv, endpoint, policy, iteration)
defer rc.Cleanup()
oracle := action.Oracle{Env: env.TestEnv}
err := umpire.Drive(ctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan)
drift := umpire.Reconcile(oracle, rc, plan)
```

Across the three reviewed files, `Drive`, `Reconcile`, `NewCtx`, `Oracle`, and `Resolver` each appear seven or eight times. The repeated `50*time.Millisecond` is runtime policy masquerading as call-site data.

`Drive` and `Reconcile` should remain available. They are appropriate flexible-core interfaces. The repeated assembly around them should become a Temporal runtime adapter that returns one structured result.

### Probe facade

The fluent probe shape is close to a useful 99% interface:

```go
report := probe.Umpire(t).
    Reach("NexusOperation", "succeeded").
    Execution(exec).
    Timeout(10 * time.Second).
    MaxFaults(6).
    FaultEachObservedCall().
    Judge()
```

It successfully hides scenario isolation, fault arming, observation, target inspection, coverage recording, and reporting. The remaining problems are architectural:

- `tests/probe` imports the compatibility `tests/umpire1/model` and `tests/umpire1/planner`, while its callers and monitors use `umpire2`.
- `Reach` validates a route from one catalog, but `Execution` may drive an unrelated plan. Several test comments explicitly describe `Reach` as “plan validation only.”
- `EnvFunc` returns `*testcore.TestEnv`, making the nominal facade inseparable from the local functional harness.
- configuration errors call `Fatal` inside the builder, so there is no reusable error-returning core beneath the test affordance.
- `Judge` mutates builder state by appending learned faults, making the builder effectively single-use without expressing that invariant.
- ambient-call exclusions and the default fault cap are logged but not retained in `Report`; machine consumers cannot tell that exploration was truncated.
- the probe maintains its own transition coverage beside canonical protocol-derived semantic coverage. The exploration test has to install and inspect both.
- target entity and state names, fault method names, and report terminals are raw strings.
- judging selects the first terminal entity of the requested type; it cannot express which symbolic or concrete entity is intended and does not reject an ambiguous match.
- cleanup intentionally suppresses workflow termination errors, so a report can look complete without representing cleanup failure.
- seeded exploration, fault selection, budgets, omissions, and coverage overlap substantially with `common/testing/umpire/campaign`, but the two modules do not compose.

The facade demonstrates the right amount of leverage, but it sits over the wrong seam. It should run against an injected compiled protocol and an environment adapter, and it should compile the execution intent and observation selector into one immutable request before allocating an environment. Its bounded exploration behavior should delegate to `campaign` where the semantics overlap.

### Sparse regression

The sparse DSL is the best current authoring interface. Tests state outcomes, actions, partial order, policies, and requirements in domain language, while the compiler fills gaps and validates the result before execution.

The shallow part is the local runner. Every root regression goes through helpers that must:

- obtain `protocol.DefaultRegressionDomain`;
- call `regress.Compile`;
- maintain a `Profile` and repeat capability names already required by the sparse plan;
- construct `action.NewRegressionHarness`;
- configure the Umpire monitor;
- coordinate six CHASM-related dynamic configuration values and callback addresses;
- create and cancel a one-minute context;
- call `regress.Run`.

Those choices describe one local environment preset, not the semantic regression. They should live together so available drive capabilities, dynamic configuration, evidence profile, realizations, and harness behavior cannot drift independently. A plan should declare what it requires; the selected environment preset should declare what it provides. The test should not have to copy requirements into both.

The flexible compiler and `Harness` interfaces should remain unchanged. A Temporal regression runner should own the common local preset, with a clearly asserting `RequireRegression` affordance for ordinary tests.

## Findings and recommendations

### P0: Make the compiled protocol the only canonical Temporal catalog

The codebase already has the right deep module: `tests/umpire2/protocol.Protocol` compiles entities, facts, actions, gaps, relations, regression vocabulary, footprints, coverage, and planning. The reviewed tests nevertheless use all of these sources:

- `planner.DefaultModels()` for structural planning and lifecycle lookup;
- `action.AutoCoverPlans()` for executable exploration;
- `protocol.Default()` for semantic coverage;
- `protocol.DefaultRegressionDomain()` for sparse compilation;
- `tests/probe`'s internal `umpire1` planner for `Reach` and judging.

This weakens the “one semantic source” guarantee and makes a caller choose a catalog without realizing it.

Recommended changes:

1. Back all Temporal planning, target lookup, action planning, coverage, probing, and sparse compilation with one `*protocol.Protocol` supplied to the runner.
2. Add a cached default accessor for test use. Keep `protocol.Default() (*Protocol, error)` for compilation tests and custom declarations; add a `MustDefault(tb)` or equivalent only in the `testing.TB`-aware facade.
3. Expose a defensive regression-domain view or a `Protocol.CompileRegression` operation. Today only `DefaultRegressionDomain` is public, so a runner cannot use an injected custom protocol consistently across planning and sparse compilation.
4. Migrate high-level callers directly to `Protocol`, retain the old catalogs only as compatibility surfaces, and delete them after their callers move. Do not make `action` or `planner` import `protocol`: `protocol` already imports their declarations, so that shortcut would create an import cycle.
5. Remove all `umpire1` knowledge from `tests/probe`.

This is primarily a locality improvement: a model registration or planning change should be fixed in one compiled declaration, not synchronized across authoring surfaces.

### P0: Separate structural targets from runtime entity selection

The pair `(entity type, state)` is passed repeatedly as unrelated strings, but simply wrapping that pair in a `Target` is insufficient. Planning asks a structural question—whether an entity type can reach a state. Judging asks a runtime question—which concrete entity, among possibly many, must be in that state. The current probe collapses these questions and accepts the first terminal entity of the requested type.

Use two related values:

```go
type PlanTarget struct {
    Entity EntityType
    State  string
}

type EntitySelector struct {
    Entity EntityType
    Ref    string // symbolic binding; an explicit ID adapter is also possible
}

type Expectation struct {
    Subject EntitySelector
    State   string
}
```

The names and exact representation need design work. The important invariant is that planning never pretends to select a runtime instance. The 99% affordance may default to “exactly one entity of this type,” but it must fail with a useful ambiguity diagnostic if the observation contains zero or multiple matches. Domain packages can provide constructors so ordinary Nexus and Workflow tests do not assemble these values or mix states from different entity families.

The canonical protocol validates the structural target once. A plan-derived execution carries its symbolic bindings into the observation selector. A custom execution must supply its selector explicitly and the result must record that its traffic was not derived from the planned route. This removes the current possibility that `Reach("NexusOperation", "succeeded")` validates one route while `Execution` drives and judges an unrelated entity.

### P0: Add a deep Temporal session and operation-specific runners

Add a Temporal-specific authoring module—illustratively `tests/umpire2/umpiretest`—that owns the standard session policy:

- canonical protocol selection;
- environment allocation and namespace isolation;
- observation, polling, quiescence, and deadline policy;
- target selection and namespace rule evaluation;
- cleanup ordering and artifact capture.

Use operation-specific runners over that session rather than one request containing every possible knob:

- an action runner owns `Ctx`, `Oracle`, `Resolver`, `Drive`, semantic reconciliation, rejection reconciliation, and optional footprint reconciliation;
- a regression runner delegates compilation and execution to `regress`;
- a campaign runner delegates bounded discovery, omissions, minimization, and replay to `campaign`.

This distinction matters because an `[]umpire.Action` does not declare all resources needed to create an endpoint, worker, or participant. The generic action runner should accept a prepared session adapter. A Nexus 99% affordance or the sparse regression resource catalog may prepare those resources. Do not make the runner infer undeclared requirements from concrete realizer types.

Each runner should return an operation-specific result that embeds the same phase structure:

```go
type Result struct {
    Metadata  RunMetadata
    Plan      PlanResult
    Execution ExecutionResult
    Judgment  JudgmentResult
    Cleanup   CleanupResult
}
```

The exact names need design work. The invariant is more important: validation fails before allocation; execution records bindings, rejections, and traces; judgment records qualified claims, drift, and violations; cleanup is always represented. Do not flatten execution failure, semantic drift, footprint drift, rule violation, unsupported evidence, inconclusive evidence, unreached targets, and cleanup failure into one formatted error or a single verdict enum.

The reusable runners return `(Result, error)` and have no dependency on `testing`. Thin 99% adapters should accept `testing.TB`, call `Helper`, register cleanup, and apply an explicit assertion policy. A normal regression helper may require conformance automatically. A probe helper must return violated or degraded results without failing immediately because tests use those outcomes to verify Umpire itself.

### P1: Make sparse regression the preferred semantic test affordance

Add a local convenience operation along these lines:

```go
umpiretest.RequireRegression(t, coreregress.OnePath(
    nexus.State("op", nexus.Started),
    coreregress.During(
        nexus.FailNext(rpc.CancelNexusOperation),
        nexus.CancelWithRetry("op"),
    ),
    nexus.State("op", nexus.Canceled),
))
```

For the default case it should choose the canonical regression domain, standard realizations, an isolated local environment, the Umpire monitor, the in-process evidence profile, a safe local capability preset, CHASM enabled, serial path execution, and a documented deadline. The plan's existing `Require` instructions state what it needs; the preset states what is available. The helper validates the two instead of making the caller repeat a capability name in `Profile`.

Options should expose the real variations already present in the tests:

- CHASM enabled or disabled;
- a named environment preset or custom environment/evidence profile;
- path parallelism;
- artifact sink;
- timeout and other explicit bounds;
- custom environment factory.

The local preset must configure evidence sources, available drive capabilities, dynamic configuration, and the actual environment from the same value. Evidence capability and drive authority are distinct fields even when one preset selects both. It should validate contradictions before environment allocation—for example, a plan requiring activity callbacks while the selected preset disables them.

Do not remove `regress.Compile`, `regress.Run`, `RunWithOptions`, or the harness interfaces. They remain the flexible layer for deployments, canaries, replay, and custom adapters.

### P1: Converge probe and campaign instead of adding another exploration engine

`common/testing/umpire/campaign` already owns bounded selection, seeds, matrix and lifecycle exploration, fault targets, semantic coverage, omissions, execution records, minimization, replay, and regression candidates. The current probe independently owns a smaller baseline/fault loop and a second result vocabulary.

Keep the productive probe affordances—learn a happy-path footprint, transiently drop or hold each call, isolate scenarios, and summarize model-derived outcomes—but route the overlapping work through `campaign`:

1. Add a Temporal `campaign.Executor` adapter backed by the session and regression runner.
2. For learned-footprint campaigns, run one explicit baseline phase, retain its trace and omissions, then supply the derived fault targets to `campaign.Request`.
3. Use canonical protocol coverage and `QualifiedClaim` as the stored semantics. Human-facing recovered/degraded/flagged labels may remain a view, not another source of truth.
4. Add a thin `testing.TB` facade that builds the common bounded request and prints `campaign.Result.Summary()`.
5. Keep a small action-plan probe only for cases that cannot yet be represented as a sparse `regress.Suite`; do not force a lossy action-to-regression conversion merely to reuse campaign.
6. Keep custom execution as an escape hatch, but record that it was not plan-derived and require an explicit observation selector.

Any retained probe request should be immutable before execution, return `(Report, error)` below the test adapter, accept a narrow environment/session factory instead of `*testcore.TestEnv`, and retain protocol version, profile, seed, budgets, skipped calls, capped targets, unsupported cases, and cleanup failures.

This approach deepens the existing campaign module and prevents two engines from gradually disagreeing about budgets, omissions, evidence qualification, and promotion.

### P1: Encapsulate local environment presets and process-global instrumentation

The current tests know that a CHASM Nexus run requires several dynamic configuration values, a callback address allowance, `TEMPORAL_OTEL_DEBUG`, a monitor factory, and sometimes process-global tracer-provider wiring. These are load-bearing invariants with concurrency implications.

Introduce a local environment preset that owns them as one tested module. It should:

- derive dynamic configuration, evidence profile, and available drive capabilities from one preset;
- install the monitor and required telemetry before traffic;
- avoid per-test mutation of process-global tracer or environment state;
- install unavoidable global instrumentation once per test binary, or reject parallel execution through an explicitly serialized adapter;
- allocate unique workflow, endpoint, operation, and task-queue names;
- own cleanup ordering and report cleanup failures.

Restoring one test's global tracer provider while another parallel test is using it cannot be made safe with `t.Cleanup`. Prefer environment-owned tracer injection or a process-wide routing provider. If neither is available, the preset must declare serialization as a requirement rather than hiding the race.

The preset is a 99% convenience, not the only environment. The runner should continue accepting custom environment factories for HSM comparisons, public-API profiles, deployments, canaries, and specialized participants.

### P1: Add a generic judge and typed inspection affordances

Ordinary tests should not poll `ModelState.QueryEntities`, cast concrete entities, read `FSM`, and then call `CheckNamespace`. Add a protocol-backed judge that can:

- wait for a target state or terminal disposition;
- select an entity by a bound symbolic reference or typed identity and enforce the expected match cardinality;
- wait for a defined observation/quiescence condition rather than relying on unrelated sleeps or immediate teardown;
- evaluate namespace rules;
- qualify the claim against the selected evidence profile;
- return the matched entity snapshot, violations, unsupported/inconclusive status, and observation timeout diagnostics.

Support three inspection levels:

1. the runner's `JudgmentResult` and qualified claim for the 99% case;
2. typed selectors for fields such as Nexus attempt count or WorkflowRun lineage;
3. raw monitor/model access for framework and observation-specific tests.

The third level matters. A telemetry test that proves `Attempt` was observed is intentionally below a generic terminal-state assertion and should not be forced into a universal facade.

### P2: Simplify planning defaults without removing explicit planning

The common call is verbose:

```go
protocol.PlanTo(entity, state, umpire.Shortest, umpire.Constraints{})
```

The 99% interface should make shortest route and no additional constraints implicit. The 90% interface should accept one coherent request or option style rather than the current mixture of positional mode, a `Constraints` struct, and functional options for the seed.

For example:

```go
plan, err := protocol.Plan(umpire.PlanRequest{Target: planTarget})
plan, err := protocol.Plan(umpire.PlanRequest{
    Target:      planTarget,
    Mode:        umpire.AllRoutes,
    Constraints: constraints,
    Seed:        seed,
})
```

The existing pure `PlanTo` function can remain as the lowest-level operation.

Also tighten the multi-route invariant: `Plan.RunWith` currently resets between routes only if the driver happens to implement `Resetter`; otherwise it silently drives later routes against existing state. The 99% runner should allocate a fresh session per route. The 90% runner should reject a multi-route plan without an explicit isolation or reset strategy. The core behavior can be preserved for compatibility if necessary, but the result should make the assumption visible.

### P2: Add constructors for common actions while retaining the open action value

`umpire.Action` is intentionally expressive, but its exported fields encode non-obvious invariants involving `Kind`, `Hosting`, proactive versus reactive realization, fresh and linked references, rejection semantics, entries, and footprints. `Realizer` also requires both `Install` and `Fire`, leading many adapters to implement one meaningful method and one no-op.

Keep the open struct and interfaces for custom actions. Add constructors for the common forms:

- proactive action with a fire function;
- reactive action with an install function;
- standing fault policy;
- expected rejection;
- fresh entity effect;
- observation-bound successor effect.

Constructors should validate or make illegal combinations unrepresentable. Domain packages should continue exposing semantic constructors such as `CompleteWith`, `Hold`, and sparse Nexus/Workflow instructions instead of asking ordinary callers to build raw actions.

Avoid placing Temporal-specific constructors in `common/testing/umpire`; they belong in the Temporal adapter or domain packages.

### P2: Make phase-specific bounds and omissions first-class

The current probe correctly bounds fault count and skips ambient traffic, but only logs those decisions. Random and coverage-guided tests similarly manage seeds, iterations, budgets, and dropped scenarios at the call site.

Do not solve this with one generic `Bounds` struct. Planning depth, sparse path count, execution parallelism, observation timeout, campaign candidates, fault count, and minimization attempts have different semantics and validation rules. A universal struct permits nonsensical combinations and would duplicate existing `regress.CompileLimits`, `regress.RunOptions`, and `campaign.Bounds`.

Keep bounds with the phase that enforces them:

- planning request: route mode, maximum depth, and seed;
- execution policy: action/observation/cleanup deadlines, polling or quiescence policy, and parallelism;
- sparse compilation/execution: the existing compile limits and run options;
- discovery: the existing campaign bounds plus any learned-footprint budget.

Every result should retain the applicable phase options. Use `campaign.Result.Omitted` for campaign selection and add equally explicit omissions only where no existing result owns them, such as skipped ambient calls during footprint learning.

An “unbounded” choice must also be explicit where it can produce combinatorial or operational risk. Exhaustive modes should continue failing rather than truncating. Exploratory modes may truncate, but the report and artifact must retain what was omitted.

### P2: Make Go documentation identify the supported authoring path

Current package documentation still points from generic actions to `tests/umpire1`, while `UMPIRE.md` declares `umpire2` canonical. `tests/umpire2/planner` presents itself as the authoring surface even though `protocol.Protocol` now owns the canonical catalog, and `tests/probe` calls itself the single Umpire interface while describing itself as a prototype.

Update package comments and examples as interfaces migrate. Mark compatibility surfaces explicitly and give developers one start-here example from the 99% facade, one customization example from the 90% runner, and one escape-hatch example from the core. Package discovery is part of the interface; contradictory Go documentation will keep callers on legacy seams even after better affordances exist.

## Suggested module layout

```text
common/testing/umpire
  Flexible Temporal-independent model, planner, action, observation, and coverage interfaces.

common/testing/umpire/regress
  Flexible sparse compiler and executor interfaces.

common/testing/umpire/campaign
  Bounded discovery, selection, omissions, minimization, replay, and promotion.

tests/umpire2/protocol
  The single compiled Temporal semantic catalog.

tests/umpire2/action and tests/umpire2/regress/*
  Temporal adapters and typed domain vocabulary.

tests/umpire2/umpiretest
  Temporal sessions, local environment presets, action/regression/campaign adapters,
  judge and selectors, and thin testing.TB-aware 99% affordances.
```

This adds one author-facing module without moving implementation details into the generic framework or duplicating its engines. It also avoids a giant `Environment` interface becoming the universal seam. The test module can accept local functional and explicit remote/deployment session adapters while each implementation uses narrower internal capability interfaces. Canary execution remains behind the existing canary module and its safety envelope.

## Migration order

1. Add protocol-backed default access, `PlanTarget`, and explicit observation selectors without changing existing callers. Characterize zero-, one-, and multiple-match judgment behavior.
2. Add the session abstraction and unit-test an action runner around the existing `Ctx`, `Oracle`, `Resolver`, `Drive`, and `Reconcile` implementations. The runner accepts prepared resources rather than inferring them from actions.
3. Migrate the repeated low-level action blocks in `tests/umpire_probe_test.go` to the action runner. Compare bindings, drift, rejections, footprints, qualified claims, and cleanup behavior.
4. Add the validated local environment preset, error-returning regression runner, and `RequireRegression` test affordance; migrate the root regression helper without requiring duplicate capability declarations.
5. Add a Temporal `campaign.Executor` adapter and a learned-footprint phase. Move bounded fault exploration onto `campaign` while preserving a small compatibility wrapper for action-only probes.
6. Replace manual terminal polling and namespace judging in ordinary tests with the generic judge; retain direct model queries in telemetry-specific tests.
7. Migrate high-level `DefaultModels`, per-entity action planning, and coverage callers directly to `Protocol`. Do not introduce reverse imports from `action` or `planner` to `protocol`; remove compatibility catalogs after their callers are gone.
8. Update package documentation and migrate root tests to the 99% affordances. Keep one explicit plan/drive/judge integration test as documentation and as a test of the low-level seams.

At each step, replace tests at the new module's interface instead of layering duplicate tests over both the old assembly and the new facade.

## Trade-offs and failure modes

### Complexity

A new facade adds another named package, but it removes several packages and invariants from each ordinary call site. It earns its keep only if it owns session policy, execution, judging, and cleanup—not if it merely forwards arguments to `Drive` or reimplements `regress` and `campaign`.

The main design risk is a “god builder” with many unrelated options. Prefer explicit request values and a few environment/fault adapters. Keep advanced compiler and runtime interfaces available rather than exposing every internal knob through the convenience layer.

### Performance and 10x scale

Protocol compilation can be cached for the default immutable declaration. Live environment creation dominates test cost and should remain isolated per scenario or path. A 10x increase in actions or scenarios needs explicit path/fault budgets and bounded concurrency; it should not cause a convenience default to launch unbounded work.

The runner may offer parallel path execution, but serial execution should remain the safe local default until environment and process-global instrumentation isolation are guaranteed.

### Crashes, cancellation, and cleanup

The runner should validate protocol, plan, capabilities, realizations, and bounds before allocating a cluster. Once allocation starts, cleanup should use an uncancelled, bounded context and run for every exit path. Cleanup failures belong in the result or returned error, not only logs.

The completed plan, selected profile, bounds, and prepared resource metadata should be retained before execution. Generated identities and bindings should be recorded as soon as they are observed so a crash or timeout still leaves the most complete replayable diagnostic artifact available.

### Security and deployment safety

Local fault and in-process observation capability must not imply deployment or canary authority. Convenience defaults should select only the isolated local adapter. Remote execution remains an explicit 90% adapter. Canary execution should continue through `common/testing/umpire/canary` so the facade cannot bypass action/fault allowlists, budgets, evidence requirements, or cleanup authority.

### Assumption drift

The largest failure mode of a convenience interface is that a default silently stops matching reality. Prevent this by deriving the runner's catalog, profile, capabilities, dynamic configuration, coverage denominator, and artifacts from the same compiled protocol and environment preset. Record those selections in every result.

## What not to do

- Do not replace the generic interfaces with a test-only fluent builder.
- Do not move Temporal environment or SDK knowledge into `common/testing/umpire`.
- Do not use one `(entity, state)` value as both a structural planning target and an ambiguous runtime selector.
- Do not infer endpoint, worker, or participant requirements from concrete action realizers.
- Do not introduce a universal bounds type or flatten all phases into one verdict.
- Do not mutate process-global tracing or environment state per parallel test.
- Do not keep `umpire1` as an implicit dependency of the canonical probe path.
- Do not infer that a custom execution reached the declared target without reconciling observed state.
- Do not silently cap exhaustive work or discard unsupported cases.
- Do not require every specialized evidence assertion to fit the 99% facade.
- Do not expose every internal seam as a facade option; keep the low-level modules available instead.

## Recommended first increment

The highest-leverage, lowest-risk first increment is the sparse regression convenience because the domain interface, compiler, executor, resource catalog, and harness already exist. Add one vertical affordance:

1. an error-returning regression runner plus `RequireRegression(testing.TB, coreregress.Plan, ...RegressionOption)` in the author-facing Temporal module;
2. a cached canonical protocol and regression domain behind it;
3. a validated `LocalCHASM` default preset that owns monitor, callback, dynamic configuration, evidence profile, available drive capabilities, serial execution, and deadlines;
4. validation of the plan's `Require` instructions against the preset without duplicate capability declarations;
5. retained suite/profile/artifact/cleanup diagnostics, with the test adapter requiring conformance for the ordinary case;
6. migration of `runSparseRegressionWithCHASM` as the characterization call site, preserving an explicit HSM preset for its comparison cases.

That increment removes a complete block of repeated assembly without inventing a new execution engine or narrowing the core. The next increment can add prepared sessions, runtime selectors, and the action runner; campaign convergence can follow once those adapters return qualified execution records.
