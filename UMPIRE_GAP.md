# Umpire gap analysis

Assessment date: 2026-08-13.

## Executive summary

Umpire has a substantial behavioral-testing foundation, but it is not yet the environment-spanning
platform described in [UMPIRE_VISION.md](./UMPIRE_VISION.md). It can define and observe selected
Temporal behavior, compile concise regressions, inject selected faults, retain replay evidence, and
run bounded formal checks in CI. These capabilities are strongest for Workflow and Nexus behavior
in a controlled test cluster.

The main gap is integration and reach. Coverage, exploration, fault scheduling, replay, and formal
verification exist, but they do not yet form one workflow that autonomously finds a behavior,
minimizes it, and promotes it to a durable regression. Execution is not portable across local,
deployment, and production environments, and there is no explicit black-box mode that qualifies
claims according to the evidence available.

Directionally, Umpire is about two-thirds of the way through its core behavioral foundation and
about one-third of the way to an operational product. The next milestone should be a complete
discovery-to-regression loop in CI. Black-box execution and guarded canaries should follow only
after evidence limits and safety controls are explicit.

## Assessment scale

- **Established:** implemented and exercised end to end for a meaningful behavioral slice.
- **Partial:** important mechanisms exist, but coverage or workflow integration is incomplete.
- **Missing:** no supported end-to-end capability exists yet.

## Vision coverage

| Behavioral concern | Status | Current position | Gap to the vision |
| --- | --- | --- | --- |
| Define behavior once and use it consistently | Partial | One compiled protocol supplies runtime monitoring, planning, regression vocabulary, coverage, and formal projections. | Not all behavior is derived uniformly, and several runtime properties, delivery semantics, and executable actions still have separate or incomplete representations. |
| Express regressions as behavioral intent | Established for a narrow slice | Sparse regressions describe outcomes, relations, policies, and symbolic entities; live Nexus regressions demonstrate the approach. | Migrate a representative breadth of existing regressions and reject every missing realization or resource before execution. |
| Make plans deterministic and executions replayable | Partial | Planning and bounded generation are deterministic or seeded. Artifacts retain completed paths, bindings, observations, facts, and verdicts. | A distributed rerun cannot reproduce the original schedule exactly. Replay must preserve and validate the semantic experiment while clearly reporting observation or scheduling drift. |
| Run the same behavioral tests across environments | Partial | Local test-cluster execution and CI-based generated-model verification exist. | Define portable environment capabilities and runners for deployment validation and guarded production canaries without changing test intent. |
| Work in white-box and black-box modes | Partial, early | Umpire combines gRPC interception, telemetry, histories, responses, and normalized in-process facts. | Add an explicit public-interface-only mode and qualify every property according to whether its required evidence is available. Missing evidence must produce an unsupported or inconclusive claim, not success. |
| Support identities and ordering that emerge at runtime | Partial | Symbolic entities can be grounded from observations, including server-generated Run IDs, and typed relations connect entities discovered at different steps. | Extend this consistently across the modeled domain and make ambiguous identity, missing lineage, and conflicting observations first-class outcomes. |
| Treat faults, retries, concurrency, and timing as behavior | Partial | Fault policies can drop or hold selected calls, regressions can scope faults, and bounded fault scheduling prioritizes distinct targets. | Expand beyond internal test hooks, model more concurrent and timing outcomes, and connect fault exploration to minimization, coverage, and regression promotion. |
| Remain sound under clock skew and partial ordering | Partial, early | Facts can retain source event time, while traces prefer causal or source ordering over wall-clock order. | Represent uncertainty between distributed observations explicitly and ensure properties do not infer causality from incomparable clocks. |
| Support programmable workers and controlled participants | Partial | The kitchensink workload can realize selected Workflow and Nexus routes with pre-programmed worker behavior. | Cover the principal entity types and allow the same behavioral plan to select environment-appropriate participant implementations. |
| Guide exploration from developer intent | Partial | Seeded route exploration, all-path compilation, constrained pairwise generation, coverage catalogs, and bounded verification targets exist. | Provide one campaign interface that combines a behavioral template, explicit bounds, environment capabilities, and a stable exploration result. |
| Discover and prioritize unknown behavior automatically | Partial, early | Semantic coverage identifies unmet behavior and fault scheduling gives limited novelty priority. | Select new scenarios from coverage and risk, maintain a useful corpus, avoid redundant executions, and report why each candidate was chosen. |
| Turn discoveries into durable regressions | Missing | Failure artifacts and replay primitives provide much of the required evidence. | Automatically minimize a failing execution, produce stable behavioral intent, verify that it still fails, and make it ready for human-reviewed promotion. |
| Keep authoring and diagnosis developer-friendly | Partial | APIs are typed and behavioral, and artifacts expose semantic names rather than only wire traffic. | Reduce the number of concepts needed for common cases, provide concise failure explanations, and make unsupported behavior actionable at authoring time. |
| Make assurance limits explicit | Partial | Formal results distinguish bounded evidence, proof, counterexample, unsupported, and inconclusive outcomes; generated targets record bounds and abstractions. | Complete backend limit classification, unsupported-semantics enforcement, counterexample normalization, trace replay, and property coverage. Apply the same discipline to live runs and observation modes. |

## Cross-cutting gaps

### Behavioral breadth

The framework is broader than its proven end-to-end coverage. Workflow and Nexus have meaningful
actions and live regressions, while Activity, Callback, Workflow Task, Task Queue, routing, and
delivery behavior are less complete or primarily observational. A broad catalog is not enough: each
important behavior needs an executable route, observable evidence, properties, faults where
applicable, and at least one non-vacuous test.

### One coherent workflow

Umpire has most of the individual mechanisms needed for guided behavioral discovery, but callers
must assemble them. There is no single bounded campaign that moves from declared behavior through
selection, execution, evaluation, minimization, replay, and regression promotion. Until that loop
exists, exploration remains a collection of useful tools rather than a product capability.

### Evidence-aware portability

Environment differences are currently handled mostly by test setup and available hooks. The vision
requires behavioral intent to remain stable while each environment declares what it can drive and
observe. Every verdict must include those capabilities so a black-box run cannot silently make the
same claim as a richer white-box run.

### Distributed reproducibility

Deterministic planning does not make a distributed execution deterministic. Umpire should promise
replay of the completed semantic experiment, with recorded identities, observations, ordering,
faults, bounds, and environment profile. It should detect divergence from the original evidence
rather than imply that a seed can reproduce an uncontrolled schedule.

### Operational safety

Production canaries require controls that ordinary tests do not: strict traffic and fault budgets,
namespace and tenant isolation, cleanup guarantees, secret-safe evidence, auditability, stop
conditions, and a restricted action catalog. None should be implicit or inherited from a local test
harness.

## Prioritized outcomes

### 1. Make current claims trustworthy

- reject incomplete regression suites during compilation, before they reach execution;
- qualify live and formal verdicts by model version, capabilities, observations, and bounds;
- finish unsupported and limit classification;
- retain replayable, non-secret evidence for every failure; and
- demonstrate detection power with seeded behavioral mutations.

### 2. Complete the CI discovery loop

- accept a behavioral template, risk focus, and explicit exploration budget;
- select scenarios using semantic novelty and declared risk;
- execute them in isolated environments with deterministic planning;
- minimize failures while preserving the violated behavior; and
- emit a replayable behavioral regression candidate for review.

### 3. Broaden meaningful behavior

- add executable and observable slices for Activity, Workflow Task, Callback, and task delivery;
- extend programmable worker behavior to those slices;
- add concurrency, retry, timeout, and fault cases that distinguish real correctness outcomes; and
- keep every exploration space bounded and every omission visible.

### 4. Add evidence-aware black-box execution

- define observation profiles for public API, history, telemetry, and in-process evidence;
- state which properties each profile can establish;
- represent ambiguous ordering and identity explicitly; and
- run the same behavioral regression without embedding environment-specific mechanics.

### 5. Introduce guarded deployment and canary use

- start with read-only observation and non-destructive workloads;
- enforce explicit traffic, time, fault, and cleanup budgets;
- isolate all created resources and redact retained evidence;
- stop on invariant violations, missing safeguards, or observation loss; and
- expand capabilities only after CI and deployment runs establish reliability.

## Completion criteria

The vision is materially realized when:

- a behavior is declared once and its monitoring, driving, regression, coverage, and verification
  views cannot drift silently;
- the same behavioral regression runs unchanged in local, CI, deployment, and approved canary
  profiles, with differences expressed as capabilities;
- every result identifies what was explored, what was observed, what was omitted, and what claim is
  justified;
- an exploration campaign can find a seeded unknown failure, minimize it, replay it, and produce a
  stable regression candidate;
- distributed observation uncertainty, including clock skew, cannot be mistaken for a proven
  causal order;
- principal Temporal behaviors have executable, observable, non-vacuous coverage; and
- canary execution cannot exceed its declared safety envelope.

## Boundaries

Umpire should not promise deterministic distributed scheduling, exhaustive exploration of an
unbounded system, or equal assurance from unequal observation modes. It should preserve behavioral
intent, make uncertainty and bounds explicit, and retain enough evidence to diagnose or replay a
semantic experiment.

Exact wire compatibility, authorization, performance, schema migration, metrics, and low-level
synchronization tests should remain specialized when their contract is below Umpire's behavioral
abstraction.

## Consolidated plan order

Execute the remaining work in this order:

1. Make generated verification claims trustworthy by enforcing unsupported semantics and complete
   limit classification.
2. Continue the model-boundary plan with Workflow and Activity delivery adapters and the
   speculative Workflow Task target.
3. Finish compile-time validation of sparse-regression realizers and resources.
4. Add FizzBee only after the shared result and evidence contracts are reliable across the existing
   backends.

The sections below preserve the complete remaining-work lists, architectural decisions,
implementation outlines, validation plans, risks, and sources from the superseded planning
documents.

## Umpire sparse regressions: remaining work

- [x] Validate completed suites in `Compile` before returning them.
  - Call `ValidateSuite` after constructing the completed `Suite`.
  - Return a structured compilation error if validation fails.
  - Test that every successfully compiled suite passes `ValidateSuite`.

- [ ] Report missing realizers and resources as structured compilation errors.
  - Validate selected action, policy, and resource realizations before execution.
  - Validate action and policy resource references and resource dependency chains.
  - Include the source instruction and a stable error category in each failure.
  - Test missing realizations, missing resources, and resource dependency cycles.

## Umpire generated verification models: remaining work

- [ ] Complete verification coverage of the canonical protocol.
  - Migrate `SpeculativeTaskCreation`, `NexusOperationClosure`,
    `NexusOperationTimeoutSemantics`, and `WorkflowTaskStarvation` into the shared property algebra.
  - Add refinements for regression semantics still marked as outside the initial bounded slice, or
    classify them as permanent exclusions.

- [x] Enforce unsupported semantics consistently.
  - Preserve progress and fairness semantics in each backend or emit an explicit unsupported result.
  - Return `unsupported` when a backend cannot provide the requested guarantee.
  - Prevent smoke and nightly runs from reporting success for an unsupported semantic subset.

- [x] Complete normalized counterexample evidence.
  - Recover action bindings from every backend trace.
  - Populate state and relation deltas for every normalized step.
  - Validate normalized traces by replaying them through the Go interpreter when supported.

- [x] Complete limit classification.
  - Detect depth, state, step, memory, schedule, timeout, interruption, and tool limits.
  - Ensure every reached limit produces `inconclusive`, never a success status.

- [ ] Expand backend equivalence and mutation testing.
  - Compare tiny reachable worlds with TLA+, P, Ivy, and Apalache results.
  - Seed missing guards, missing frame clauses, reversed cardinality, omitted choices, identity reuse,
    weakened properties, and incorrect refinements.
  - Require every supporting backend to expose each mutation.

- [ ] Make tool pins a single source of truth.
  - Derive CI installation and reported runner versions from the manifest's versions and checksums.
  - Fail verification when workflow, runner, and manifest tool metadata drift.

### Normalized counterexample evidence design

#### Scope and assurance policy

Normalize counterexamples from TLC, Apalache, P, PEx, and Ivy. A failed Apalache proof obligation
uses the same Apalache evidence path; SANY and successful proof obligations do not produce a trace.
Normalization is fail-closed: Umpire reports an authoritative `counterexample` only when it maps the
failed property, reconstructs every action and binding, derives every state and relation delta, and
replays the complete path through the canonical Go interpreter. Missing, malformed, ambiguous, or
unreplayable evidence becomes `inconclusive` with termination `evidence-failure`. The native trace,
stdout, stderr, and replay command remain available so the failed conversion can be diagnosed.

#### Semantic normalizer

Add a deep verification module with this conceptual interface:

```go
type TraceEvidence struct {
    Initial *ModelState
    Steps   []ObservedTraceStep
}

type ObservedTraceStep struct {
    Action   string
    Bindings Bindings
    After    *ModelState
    Deltas   []StateDelta
}

func NormalizeCounterexample(model Model, property string, evidence TraceEvidence) ([]TraceStep, error)
```

Each observation may provide an action and bindings, a resulting state, or both. The normalizer
starts from `Interpreter.InitialState`, verifies an observed initial state when present, enumerates
enabled canonical transitions, and filters them against the evidence. Exactly one transition must
survive at every step. An action and complete bindings make this constant-sized in the common case;
a state-only observation may require bounded enumeration of enabled actions and identities. Multiple
branches or bindings that reach indistinguishable observed states remain ambiguous and fail rather
than being guessed. A nil binding map means that bindings were not observed; a non-nil empty map is
complete evidence for an action with no parameters. Likewise, a non-nil empty delta slice is native
evidence for a no-op action and must be validated.

After the final step, evaluate the mapped property against the unique canonical state, including
quiescence when required, and require the reported property to be violated. This also permits a
zero-action counterexample when the canonical initial state itself violates the property. The
reverse property vocabulary covers declared model properties and generated structural relation
invariants so backend-specific invariant names never leak into the normalized result.

The matched before and after states produce deterministic `StateDelta`s:

- entity creation records `Entity`, `ID`, an empty `FromState`, and the created `ToState`;
- entity state changes record both `FromState` and `ToState`;
- relation additions and removals record `Relation`, `Source`, `Target`, and `Added`;
- a valid no-op action has an empty delta list.

Sort entity deltas by entity and identity and relation deltas by relation, source, and target. The
normalizer also accepts already populated deltas and rejects them if they disagree with replay,
making the same module usable by future backends with native delta output.

#### Runner data flow and native evidence

`runner.Request` carries the projected canonical `Model` plus a deterministic reverse vocabulary
for backend identifiers. `Toolchain.Plan` is the authoritative constructor for both. Thin backend
decoders translate native evidence into `TraceEvidence`; they do not apply protocol semantics.

- **P and PEx:** parse the existing `UMPIRE_ACTION` records and their complete bindings. The Go
  interpreter supplies and validates resulting states. A branched transition without native branch
  evidence is ambiguous unless all branches converge to the same state.
- **TLC:** request its pinned JSON trace dump and combine the decoded states with the existing native
  action labels and bindings. Preserve the JSON file in `tlc-native`.
  Compatibility note: pinned `tla2tools` 1.7.4 predates `-dumpTrace json`, so the current decoder
  consumes the complete native textual trace. Keep JSON as the upgrade target when the pin moves to
  a release that supports it.
- **Apalache and Apalache proof:** read the pinned tool's emitted ITF JSON. Decode generated entity
  sets, state functions, and relation sets through the reverse vocabulary, then infer the unique
  canonical transition between adjacent states. Keep each obligation's native trace separately.
- **Ivy:** decode the textual symbolic trace's action and state valuations through the reverse
  vocabulary. Because Ivy checks inductiveness, a counterexample that does not begin at the
  canonical initial state or cannot form a reachable canonical path is `inconclusive`, not a
  reachable Umpire counterexample.

Add `NativeTrace string` to `Result` for the exact counterexample payload used by the decoder. Native
artifact directories remain the primary lossless evidence when configured; `NativeTrace` keeps a
direct `runner.Check` result self-contained. Bound native payload reads to 4 MiB. Never execute the
recorded native replay command during normalization.

#### Classification and errors

Keep interruption and resource limits ahead of counterexample handling. When a recognized failure
marker is present, map the property and decode and normalize the trace before returning
`counterexample`. On failure, return `inconclusive` with `evidence-failure` and a diagnostic beginning
with one stable category:

- `native-trace-missing`;
- `native-trace-malformed`;
- `native-trace-too-large`;
- `property-unmapped`;
- `property-not-violated`;
- `initial-state-mismatch`;
- `transition-unreplayable`;
- `transition-ambiguous`; or
- `delta-mismatch`.

Include the backend and step number after the category when applicable. Do not replace a missing
property with `unknown`. A nonzero tool exit remains acceptable for a fully normalized
counterexample. Decoder or replay failures are result data, not `runner.Check` errors, so artifact
writing still runs. Filesystem failures while collecting or writing evidence remain ordinary Go
errors because Umpire cannot promise that the evidence was retained.

#### Testing and operational trade-offs

Use strict red-green cycles at the public seams:

- `verify` tests cover creation, state change, relation add/remove, no-op transitions, supplied-delta
  validation, state-only inference, invalid initial state, missing bindings, divergent branches,
  ambiguous transitions, and an unreplayable later step;
- generator tests cover deterministic reverse vocabularies, escaped identifiers, and collisions;
- runner fixture tests cover valid and malformed TLC JSON, Apalache ITF, P/PEx action records, and
  Ivy symbolic traces, plus property mapping and every stable failure category;
- classification tests prove partial evidence can never remain `counterexample` and prove raw native
  evidence survives failure;
- environment-gated seeded-tool tests require complete bindings, deltas, and successful Go replay
  whenever the pinned binaries are available.

Normalization runs only on counterexamples. With native action and bindings its time is linear in
trace length; state-only evidence adds the bounded action-and-identity enumeration already used by
the interpreter. Memory is linear in the trace and capped native payload. A 10x increase in trace
length therefore causes approximately 10x normalization work, while a 10x identity bound can grow
state-only inference combinatorially; explicit bindings avoid that cost, and ambiguous or excessive
evidence fails closed. The change adds parser and vocabulary complexity but no third-party library,
does not alter generated transition semantics, and does not affect clean verification runs.

## Umpire model boundaries

### Recommendation

Umpire should have **one authoritative model family, not one monolithic state space**.

The protocol should remain the single source of entity, action, relation, property, and refinement
vocabulary. From that source, Umpire should derive several bounded semantic verification targets:

1. a reusable **task-delivery foundation**;
2. separate **feature models** for Workflow, Activity, Nexus, Callback, and later features;
3. small **feature + foundation compositions** for cross-layer properties; and
4. focused **foundation submodels** for algorithms such as backlog acknowledgement, routing, and
   ownership fencing.

These are separate generated verification targets, not independently authored specifications. They
integrate through explicit interfaces and refinement mappings. The same action or relation must not
quietly acquire a different meaning in different targets.

This is the useful middle ground:

| Shape | Advantage | Failure mode | Verdict |
| --- | --- | --- | --- |
| One closed model containing every feature and infrastructure detail | Can state arbitrary global properties | State-space product grows quickly; feature work destabilizes infrastructure proofs; most explored combinations are irrelevant | Do not use as the default |
| Independent models with separately maintained vocabularies | Each model is small | They drift, duplicate semantics, and leave feature/infrastructure refinement unchecked | Do not use |
| One source with modules, contracts, targets, and selected compositions | Reuses semantics while controlling each state space | Requires first-class composition and refinement in the verification IR | Recommended |

The user's intuition is directionally correct: infrastructure has the highest marginal value for
formal model generation. Task persistence, matching, ownership, acknowledgement, retry, and routing
contain exactly the identity, concurrency, and failure interleavings that testing samples poorly.
Feature models remain valuable, particularly for lifecycle and cross-entity safety, but they should
usually consume an abstract delivery contract rather than embed the matching implementation.

### Organizational ownership and Conway's law

The model family should follow stable capability boundaries as well as technical boundaries.
History, Matching, Workflow, Activity, Nexus, and Callback evolve under different ownership and
release pressures. A model which centralizes their semantics behind one review queue will either
become a coordination bottleneck or drift from the systems it claims to describe.

"One authoritative model family" therefore means one compiled semantic graph, not one source file,
package, or central modeling team. Declarations should be federated across capability-owned
packages and joined by the protocol compiler. The stable ownership identifiers are capabilities,
not current organization or team names:

| Capability owner | Owns in the model | Important boundary |
| --- | --- | --- |
| `history` | Durable mutation/outbox semantics, shard fencing, and start-authorization mechanics | Exports durable intent and authorization outcomes to delivery and feature adapters |
| `matching` | Sync match, durable backlog, reservation, routing, dispatch, response correlation, and acknowledgement | Consumes valid intent; exports delivery outcomes without owning feature lifecycle policy |
| `workflow` | Workflow and Workflow Task product semantics and their delivery adapter | Consumes the delivery contract and History authorization mechanism |
| `activity` | Activity lifecycle, attempt/retry policy, result races, and its delivery adapter | Consumes the delivery contract without inheriting Workflow-specific policy |
| `nexus` | Nexus Operation lifecycle, handler semantics, operation-side relations, and Nexus adapters | Uses the common work contract only where its synchronous relay preserves that contract |
| `callback` | Callback lifecycle, transport-specific retry/acknowledgement policy, and callback-side adapters | Integrates with Nexus or Workflow through an explicit relation and adapter contract |
| `umpire-framework` | Protocol compiler, verification IR, projection, exporters, and result plumbing | Supplies modeling machinery but does not own another capability's semantics |

An illustrative source layout is therefore federated even though compilation produces one graph:

```text
tests/umpire2/protocol/
    history/                 owner: history
    matching/                owner: matching
    workflow/                owner: workflow
    activity/                owner: activity
    nexus/                   owner: nexus
    callback/                owner: callback
    integration/nexusactivity/  co-owners: nexus, activity
```

Interfaces should live with their provider declaration or in a contract package with an explicit
provider owner. A catch-all shared package with no semantic owner merely recreates central
ownership under another name.

The repository maps those stable identifiers to current reviewers through
[`CODEOWNERS`](./.github/CODEOWNERS). Reorganizations update that mapping without renaming semantic
modules, interfaces, targets, or generated artifacts. Capability ownership is organizational;
runtime values such as `OwnerGeneration` continue to mean persistence fencing and are unrelated.

Ownership rules should be enforceable rather than ceremonial:

- every module has one capability owner responsible for its declarations, local properties,
  adapters, and non-vacuous verification target;
- every interface identifies a provider owner and its consumer owners; the provider owns its
  guarantees, while each consumer owns declaring the assumptions and adapter by which it uses
  them; generation requires every such assumption to be discharged by a provider guarantee;
- integration targets are co-owned by every capability whose behavior they compose, with no
  fallback assumption that an Umpire infrastructure owner supplies missing product semantics;
- a change confined behind an unchanged interface can use the owner's local checks, while an
  exported action, identity, assumption, or guarantee change must run affected consumer and
  integration targets and require their owners' review;
- common compiler or exporter changes run all affected targets, but framework ownership does not
  grant authority to redefine capability semantics; and
- generation rejects unowned modules, unknown consumers, undischarged cross-owner assumptions, and
  integration targets without explicit co-owners.

Generated manifests should retain stable owner IDs, interface providers and consumers, and the
targets exercised. They should not embed mutable team names. This makes responsibility visible in
counterexamples and CI results while leaving organization mapping to repository policy.

### What “deep enough” means

The model should be deep enough to distinguish executions that have different correctness outcomes,
but no deeper.

Include a boundary or state distinction when at least one of these is true:

- it survives a crash or ownership change;
- retry can repeat it, skip it, or create a new attempt;
- it lies on one side of an asynchronous or transactional boundary;
- another component may observe it before the next transition;
- it changes which actions are enabled;
- an identity, generation, route, or cardinality is needed by a property; or
- collapsing it would hide a known failure mode.

Do not include a distinction merely because it exists as a Go type, RPC, goroutine, cache entry,
metric, or protobuf field. Those are realization details unless they alter the abstract choices
above.

A practical stop rule is:

> If replacing a subsystem with a nondeterministic component satisfying a small contract preserves
> every property in the current target, keep the subsystem behind that contract.

This gives four useful infrastructure depths:

| Depth | Content | Use |
| --- | --- | --- |
| L0: delivery contract | A semantic work obligation is pending, accepted, completed, retried, expired, or terminal | Interface consumed by feature models |
| L1: durable dispatch | History intent/transfer task, sync match or persisted backlog, reservation, start handshake, dispatch, acknowledgement, retry | Default foundation model; highest priority |
| L2: topology and routing | Logical/physical queues, partitions, forwarding, ownership generation, poller compatibility, build/version routing | Separate targets and selected integration checks |
| L3: queue algorithms | Read/ack levels, backlog GC, fair/priority readers, batching and rate limits | Focused algorithm models, not the default global model |

L1 is the minimum useful infrastructure depth. L0 alone can verify feature lifecycles but cannot
find task loss, premature acknowledgement, duplicate acceptance, or stale-owner bugs. L2 and L3
should be added property by property; expanding every run to those depths would mostly multiply
states without strengthening the property under test.

### What the current model actually contains

The canonical source is `tests/umpire2/protocol`, lowered through the
[verification IR](./common/testing/umpire/verify/model.go) to
the compatibility target's
[`model.ir.json`](./tests/umpire2/genmodels/protocol-atomic/model.ir.json) and generated TLA+, P,
and Ivy files.
[`UMPIRE.md`](./UMPIRE.md) and the
[generated-verification backlog](#umpire-generated-verification-models-remaining-work) correctly describe these as
derived views of one protocol.

At the time of this analysis, the generated snapshot has:

- 7 entity types, 6 relations, 70 actions, and 10 properties;
- 40 lifecycle actions marked unrealized;
- 63 recorded abstractions and 70 action refinement records;
- feature lifecycle coverage for Activity, Nexus Operation, Workflow, Workflow Run, and Workflow
  Task; and
- one lowered cross-feature regression action, `nexus.start_activity`, plus a strengthening property
  coupling the terminal Nexus Operation and Activity states.

Of the 10 properties, 3 are safety properties and 7 are quiescent-progress properties. Of the six
inventoried rule families, only `EntityProgress` and `NexusActivityLinkConsistency` are lowered;
`SpeculativeTaskCreation`, `NexusOperationClosure`, `NexusOperationTimeoutSemantics`, and
`WorkflowTaskStarvation` remain outside the property algebra. The regression inventory similarly
lowers only 1 of 24 actions. This is an honest and useful gap inventory, but it means a successful
generated run currently establishes only the selected lifecycle/relation slice. It does not yet
establish a matching, persistence, ownership, routing, or task-loss invariant.

`TaskQueue` currently has identity and observational data but no lifecycle. `WorkflowTask` has a
useful coarse path through created, added, stored, polled, discarded, and terminated, yet it is not
related to a task queue, workflow run, delivery attempt, poller, backlog position, or ownership
generation. Its transitions are abstract environment actions in the generated kernel. That means
the model recognizes a sync-versus-stored shape, but it does not explain why either path is safe.

Before this split, the IR was also flat. `verify.Model` has entities, relations, actions, properties,
abstractions, inventory, and refinements, but no module, imported interface, composition, or
verification target. The structural `ModelFamily` layer now projects those closed models while the
existing refinement records continue to connect verification actions to lifecycle and regression
vocabulary; later delivery adapters must express that a concrete delivery execution refines a
feature-level `WorkAccepted` step.

The generators faithfully expose that flat shape:

- [TLA+](./tests/umpire2/genmodels/protocol-atomic/tla/Umpire.tla) emits one global state and one `Next`
  disjunction;
- [P](./tests/umpire2/genmodels/protocol-atomic/p/Umpire.p) emits one `UmpireWorld` machine which steps itself
  through atomic actions; and
- [Ivy](./tests/umpire2/genmodels/protocol-atomic/ivy/Umpire.ivy) emits a flat set of actions and invariants rather
  than isolates with contracts.

The one-world P model is an appropriate equivalence target for the current atomic protocol. It is
not yet a model of Temporal's actors, queues, messages, or failure schedules. This distinction must
remain explicit; introducing P mailboxes without a refinement boundary would silently change the
source semantics.

### Runtime model versus verification targets

The split does not require separate live Umpire monitors. A live test follows one concrete execution,
so retaining a unified runtime entity/relation graph is useful and does not create the formal
state-space cross-product. It lets a Nexus observation link to an Activity or Callback without
moving evidence between model instances.

Formal verification is different: it generates possible entities, choices, failures, and schedules.
Its cost grows with the product of enabled domains. Verification should therefore project the same
compiled protocol into a closed target before lowering it to the Go interpreter, Ivy, TLA+, or P.
Coverage and planner scenarios may select the same targets, but the facts and vocabulary remain
globally canonical.

In short: keep one runtime graph; split exploration and proof obligations.

The existing command already uses `profile` for the `smoke` and `nightly` execution budgets. Keep
that meaning. A **verification target** selects semantic roots, closure, properties, bounds, and
backend requirements; an **execution profile** selects schedule count, exploration depth, timeout,
and CI cadence. `cmd/umpire-genmodels` should therefore gain `-target` while retaining `-profile`
for `smoke` or `nightly`. A run is identified by both values, for example
`-target foundation-delivery-safety -profile smoke`.

`generate` and `check-generated` always iterate every declared target so the checked-in tree is
complete. `-target` applies only to `verify`, which accepts one target or `all` and defaults to
`all`. Consequently
`make umpire-verify-smoke` and `make umpire-verify-nightly` run every target with the corresponding
execution budget, while backend requirements may cause a target to select only the sound backends
for its proof shape.

Generated artifacts should be target-scoped:

```text
tests/umpire2/genmodels/
    manifest.json                 target index and hashes
    protocol-atomic/              compatibility target for the current flat kernel
        manifest.json
        model.ir.json
        tla/
        p/
        ivy/
    foundation-delivery-safety/
        ...
```

The top-level manifest inventories targets. Each target manifest records its semantic owner or
co-owners, interface providers and consumers, selected roots and properties, target-specific bounds,
backend requirements, and the source-model hash. `smoke` and `nightly` results remain external run
artifacts and must identify both the target and execution profile. During migration,
`protocol-atomic` must produce behavior equivalent to the current generated snapshot so the split
does not silently change the atomic kernel.

### The foundation model

The first L1 infrastructure model should represent the durable History-to-Matching delivery path
shared by Workflow and Activity Tasks. Nexus should reuse the L0 work contract through a sibling
synchronous-relay implementation, not pretend to use that durable path. Temporal's
[History architecture](./docs/architecture/history-service.md) and
[Workflow lifecycle](./docs/architecture/workflow-lifecycle.md)
describe the key seam: History atomically persists mutable-state changes and History tasks; a
transfer processor eventually creates the corresponding Matching task; a worker polls Matching; and
History validates the start before the task is delivered. This is a transactional-outbox protocol,
not a single lifecycle edge. The current Matching
[`AddTask` interface](./service/matching/task_queue_partition_manager_interface.go) explicitly tries
synchronous matching before durable spooling, while the
[backlog reader](./service/matching/pri_task_reader.go) distinguishes retry-in-memory, respooling,
and acknowledgement outcomes. History also distinguishes definite write failure, ownership loss,
and an ambiguous write that must be resolved after reacquiring a fresh `RangeID` in
[`handleWriteErrorLocked`](./service/history/shard/context_impl.go). Those are semantic branches,
not implementation trivia.

“Task processing” needs one further boundary. The foundation owns creation, persistence, matching,
reservation, dispatch authorization, delivery, response correlation where Matching participates,
and retry. The worker's user-code computation is an environment step. Handling its completion in
History belongs to the Workflow or Activity feature model because it changes product state. Nexus's
[synchronous response relay](./docs/architecture/nexus.md)
is a foundation adapter because Matching holds the dispatch request while the worker responds. This
prevents “infrastructure” from becoming a second copy of every feature state machine.

The foundation is itself a composition, not a requirement for one giant infrastructure module:

- a **History outbox** module, owned by `history`, atomically creates valid delivery intent and
  keeps it eligible for submission until a declared terminal outcome;
- a **Matching delivery** module, owned by `matching`, chooses sync match or durable backlog,
  reserves work, and routes it;
- a **History start-authorization** module, owned by `history`, fences and commits dispatch against
  current persisted state;
- feature-validity adapters, owned by `workflow`, `activity`, or another applicable feature,
  define which feature attempt that authorization may accept; and
- a **response/acknowledgement** module, owned by `matching`, correlates the outcome and decides
  retry or retirement under the exported feature policy.

The L1 foundation target composes these modules because task-loss properties cross their seams.
Ownership, backlog arithmetic, and advanced routing can still be substituted by their contracts in
that target and checked concretely in L2/L3 targets.

#### Foundation entities

Names should stay semantic rather than copy implementation structs:

- `WorkObligation`: feature-level requirement to perform a unit of work;
- `DeliveryTask`: durable or synchronously offered representation of that obligation;
- `DeliveryAttempt`: a particular try to reserve and start the work;
- `TaskQueue`: logical destination, task type, and namespace;
- `QueuePartition`: optional L2 physical routing/ownership unit;
- `Poller`: worker-side recipient with routing compatibility;
- `OwnerGeneration`: optional L2 fencing identity scoped to an ownership domain; History shard
  `RangeID` and Matching queue `RangeID` are distinct sorts and must never be compared as one global
  counter; and
- `BacklogPosition`: an abstract ordered position used only by the L3 queue-algorithm target.

An attempt must be separate from an obligation. Retries preserve the semantic obligation but may
create a new attempt; deduplication and at-most-once acceptance are generally attempt- or
generation-scoped, not global statements about a feature operation.

There are also two retry identities. A delivery retry after transport failure may retry the same
feature attempt; an Activity retry after user-code failure creates a new feature attempt under the
same Activity obligation. The adapter must preserve both distinctions instead of using one generic
retry counter.

#### Foundation relations

The important structure is relational:

- a delivery task realizes exactly one work obligation;
- a task belongs to exactly one logical queue and task type;
- an attempt belongs to exactly one delivery task;
- a reservation, if present, is owned by one compatible poller;
- at L2, a physical queue is owned under one current generation;
- at L3, a persisted task occupies at most one live backlog position; and
- a dispatch authorization identifies the feature attempt it authorizes.

The model need not carry arbitrary payloads. Namespace, queue, task type, attempt identity, route
class, and owner generation are enough when those are the fields used by the properties.

#### Foundation transitions

The L1 action vocabulary should cover observable semantic choices, not method calls:

1. attempt to persist a feature transition and its work obligation atomically, with distinct
   success, definite-failure, and ambiguous-outcome branches;
2. after an ambiguous result, obtain a fresh ownership view and resolve the persisted outcome before
   a semantic retry; L1 treats freshness as a contract, while L2 refines it with concrete owner
   generations and fencing;
3. offer the delivery task for synchronous matching;
4. fall back to or read from the durable backlog;
5. reserve an eligible task for a compatible poller;
6. request that the relevant owner authorize dispatch (the History start handshake for Workflow and
   Activity, or the target's corresponding authorization for another task type);
7. accept or reject that authorization because of current feature state or a duplicate attempt; L2
   adds rejection under a stale owner generation;
8. dispatch an authorized task;
9. acknowledge a completed delivery attempt;
10. release, respool, or retry after a failure; and
11. expire or terminally invalidate work according to the feature contract.

Crash, timeout, RPC loss, and ownership transfer should normally be nondeterministic outcomes on
these actions. Model a concrete failure mechanism only when its ordering or persistence semantics is
itself under examination.

#### Foundation properties

The initial L1 target is useful if it can state and falsify at least these properties without
embedding concrete owner-generation or queue-level arithmetic:

- **No split commit:** a committed feature transition and its required History task appear together,
  or neither appears.
- **Ambiguous commit is resolved:** an indeterminate persistence response cannot cause the semantic
  transition to be blindly repeated under the same ownership view.
- **No phantom dispatch:** every dispatched task is backed by a valid feature obligation and any
  authorization required by that task type.
- **Coarse retirement safety:** an eligible task remains live until the delivery contract records
  acceptance, terminal invalidation, expiry, or another enumerated terminal outcome.
- **Single accepted start:** two live attempts cannot both receive start authorization for the same
  scoped feature attempt.
- **Failed start is not success:** rejection, timeout, or transport failure during the start
  handshake cannot acknowledge or silently drop the task.
- **Retry preserves obligation:** a retry may replace an attempt but cannot change the work's
  feature identity or destination.
- **Path equivalence:** sync match and persisted-backlog delivery refine the same abstract delivery
  contract.
- **Destination isolation:** work cannot change its namespace, logical task queue, or task type while
  moving through the abstract delivery path.
- **No resurrection:** terminal or invalid feature work cannot re-enter delivery.

The deeper targets refine those contracts rather than duplicating their machinery in L1:

- **L2 ownership fencing:** a persistence mutation guarded by an old owner generation cannot commit
  after the new generation is authoritative.
- **L2 routing isolation:** forwarding, partition ownership, poller compatibility, and build/version
  routing cannot move work across a declared route boundary.
- **L3 backlog safety:** read/ack advancement and garbage collection cannot pass a backlog position
  without a recorded dispatch, invalidation, expiry, or other declared terminal drop outcome.

Progress claims require explicit assumptions. “Every eligible task is eventually dispatched” is
false without some combination of a compatible poller, continued ownership, successful storage and
RPCs, retry, and fair scheduling. The model should name those assumptions and otherwise make the
claim bounded or quiescent, following the qualification already used by Umpire.

### Feature models

Feature models should own product semantics: the legal lifecycle, operation identity, retry policy,
terminal outcomes, and relations to other feature entities. They should not own the implementation
of matching.

Each feature declaration belongs to its stable capability owner. The `activity` owner can evolve
Activity attempts and retry policy without importing Nexus internals; the `nexus` owner can evolve
operation and callback behavior without owning Activity delivery. Shared relations name a schema
owner, while properties spanning those relations live in a co-owned integration target.

Each feature instead adapts its vocabulary to the L0 delivery interface:

| Feature event | Foundation meaning |
| --- | --- |
| Workflow Task scheduled | Create a workflow-task work obligation |
| Activity scheduled or retried | Create an activity-attempt work obligation |
| History start-authorization transaction commits | Accept the corresponding feature attempt |
| Worker completion/failure/timeout | Resolve the attempt according to feature policy |
| Nexus handler dispatch | Create/route a handler work obligation and wait for its permitted response |
| Callback dispatch | Create a callback delivery obligation when callback transport enters scope |

This adapter is a refinement mapping, not just a name mapping. It must establish which feature state
authorizes the work, how identity is preserved, which concrete steps may stutter at the feature
level, and which completion changes the feature state.

For Workflow and Activity Tasks, Matching reservation, the authorization RPC response, and worker
receipt surround `AcceptStart` but do not define it. Lost responses and repeated reservations must
therefore stutter at L0 unless a successful History authorization transaction has committed.

The L0 interface must remain smaller than Matching. Workflow and Activity can refine it through the
durable History/Matching path; Nexus can refine it through Matching's synchronous request/response
path. Callback HTTP delivery is likely a sibling delivery implementation with different retry and
acknowledgement semantics. It should implement the common obligation/outcome contract where that is
true, not inherit Matching-specific backlog states merely to reuse a model.

The current Nexus/Activity relation is a good example of a property that belongs above an
individual feature but below the whole server. It should be checked in a small `Nexus + Activity`
composition co-owned by `nexus` and `activity`. If task delivery becomes relevant to the
counterexample, add the abstract delivery contract first and use a targeted
`Nexus + Activity + Delivery` target, adding the delivery provider as a co-owner; do not enable
every Workflow and Callback state in the same run.

### How the models integrate

The integration should be explicit in the source model:

```text
                         shared semantic vocabulary
                                   |
                    +--------------+--------------+
                    |                             |
          task-delivery foundation         feature lifecycles
                    |                    /     |      |      \
              exports contract       workflow activity nexus callback
                    |                    \     |      |      /
                    +----------- refinement adapters --------+
                                   |
                         selected compositions
```

The foundation exports actions such as `OfferWork`, `AcceptStart`, `RejectStart`, `Dispatch`, and
`ResolveAttempt`, plus guarantees about identity, routing, and cardinality. Feature models export
when an obligation may be created and when an attempt is still valid. A composition connects those
actions and discharges both sides of the contract.

The interface is also the organizational handoff. For example, `history` owns the guarantee that a
committed transition produces durable intent, `matching` owns the guarantee that accepted intent is
not lost or acknowledged prematurely, and `activity` owns the rule that a delivered attempt still
belongs to the current Activity state. No one owner can weaken the end-to-end claim unilaterally.

Cross-layer properties then become precise:

- a committed feature transition creates exactly the required work obligation;
- dispatched work corresponds to the current feature attempt;
- accepted/completed delivery enables only a legal feature transition;
- retry replaces the permitted attempt without duplicating the semantic obligation; and
- cancellation or terminal completion invalidates delivery according to an explicit race policy.

| Property scope | Default target |
| --- | --- |
| No task loss, duplicate authorization, stale-owner mutation, premature ack, or route crossing | Foundation target at the corresponding L1/L2/L3 depth |
| Legal Activity, Workflow, Nexus, or Callback lifecycle | Individual feature target over L0 assumptions |
| Scheduled work corresponds to the delivered/completed feature attempt | Feature + delivery composition |
| Nexus/Activity terminal and reciprocal-link consistency | Nexus + Activity composition |
| Callback belongs to the right operation/handler run and cannot outlive its policy | Callback + Nexus/Workflow composition |

The model family needs three kinds of checks:

1. **Module checks:** prove or explore a foundation or feature under the assumptions of its
   imported interface.
2. **Contract/refinement checks:** establish that an implementation module provides the guarantees
   required by the abstract interface.
3. **Composition checks:** explore a small concrete combination to catch bad adapters and emergent
   cross-module interleavings.

Assume-guarantee reasoning reduces the state space, but it can preserve a shared mistaken
assumption. The composed checks and Umpire's existing live refinement/reconciliation path are
therefore essential.

### Fit by verification backend

The three generators should consume the same selected target, but they need not generate the same
proof shape.

| Backend | Best Umpire role | Model shape | Main caution |
| --- | --- | --- | --- |
| Ivy | Foundation safety and contract proofs | Relational state, quantified identity/cardinality invariants, isolates with imported/exported actions and assume-guarantee contracts | Broad arithmetic-heavy queue algorithms may leave Ivy's supported decidable fragment; abstract order/prefixes or isolate them |
| TLA+ | Reference semantics, integration, refinement, fairness and liveness | Modules composed over an explicit action interface; small TLC configurations; refinement mappings between L1 and L0 | Shared-variable composition and unbounded liveness need care; every finite result must retain bounds and fairness assumptions |
| P | Cross-check the selected atomic target; later support actorized refinement | Initially retain one `UmpireWorld` per target; a follow-up may add History shard, Matching partition, poller, and relay machines | Mailboxes change the current atomic semantics, so actorization must be a separate project and refinement target rather than a generator accident |

#### Ivy

Ivy is the strongest immediate fit for the infrastructure safety kernel. Its isolate model is
designed to verify a component against the contracts of objects it calls, and its `require`/`ensure`
style supports explicit ownership of assumptions and guarantees, as described by Ivy's
[isolate and specification guide](https://microsoft.github.io/ivy/examples/specification.html) and
[language reference](https://microsoft.github.io/ivy/language.html). The task-delivery foundation is
mostly opaque identities, relations, cardinality, and action preservation: a natural shape for Ivy.
This advantage is conditional: Ivy rejects verification conditions outside its
[supported decidable fragments](https://microsoft.github.io/ivy/decidability.html). An Ivy result
can establish parameterized safety over uninterpreted identities when those proof obligations pass;
it is not merely another finite one-ID exploration.

The generated Ivy should therefore become a set of isolates rather than one flat file. A delivery
isolate can prove no phantom dispatch and single accepted start while assuming a feature-validity
interface; feature isolates can prove that they meet that interface without importing backlog
internals. Algorithm-specific integer or sequence reasoning should be abstracted to an order/prefix
contract or verified in a smaller isolate.

#### TLA+

TLA+ should remain the semantic reference and the place for small integrated behaviors. It makes
atomicity and fairness choices visible, supports specification composition, and can express that the
L1 dispatch protocol refines the L0 delivery contract through stuttering and auxiliary/history
variables. There are two distinct operations here. At one abstraction level, splitting a TLA+
specification into component modules and composing their behaviors is often an organizational
choice, with shared-state subtleties. Between L1 and L0, the split is semantic and requires an
actual refinement mapping. Lamport covers atomicity, fairness, composition, and interface
refinement in *[Specifying Systems](https://lamport.azurewebsites.net/tla/book-02-02-28.pdf)*
(Chapters 7, 8, and 10); the current
[safety-proof guide](https://lamport.azurewebsites.net/tla/proving-safety.pdf) gives the concrete
`Spec => HL!Spec` refinement form. TLC can explore finite instances; inductive safety requires a
separate proof obligation rather than a larger finite run.

This is the best backend for questions such as whether a retry/ownership-transfer interleaving can
strand an obligation, provided the target declares the poller, failure, and fairness assumptions.
It should not compensate for a missing source-level module system by accumulating one ever-growing
`Next` relation.

#### P

The initial split should keep P as the one-world semantic lowering for each selected target. This
preserves cross-backend comparison without introducing a second execution semantics.

After the split is independently useful, P's machines, asynchronous events,
[module system](https://p-org.github.io/P/manual/modulesystem/), and
[monitor specifications](https://p-org.github.io/P/manual/monitors/) align well with History,
Matching partitions, pollers, and responses. That actorized P model can find ordering bugs which the
current atomic world machine cannot express, but it is a follow-up refinement project rather than a
completion requirement for model splitting.

The ordinary P checker explores the requested finite test scenarios and schedule count; a run with
no bug is therefore bounded evidence, not an unqualified proof. P's compositional argument comes
from replacing dependencies with abstractions and checking trace-refinement obligations, as
formalized in the [ModP paper](https://ankushdesai.github.io/assets/papers/modp.pdf), not from merely
placing machines in separate source files.

Keep both meanings explicit:

- `p-semantic`: the current one-world lowering used to cross-check the shared atomic kernel; and
- `p-actors`: a future, separately selected execution refinement whose machines and message steps must
  refine that kernel or its delivery contract.

### Required source-model capabilities

The current verification IR cannot represent the recommended split without adding first-class
structure. The conceptual additions are:

- `ModelFamily`: the canonical compiled graph of modules, interfaces, compositions, refinements,
  and targets; projection lowers one target from this graph into the existing closed `Model` shape
  consumed by interpreters and exporters;
- `CapabilityOwner`: a stable semantic identifier whose current reviewers are resolved through
  repository ownership policy;
- `Module`: one capability owner plus its entities, relations, actions, properties, adapters, and
  internal state;
- `Interface`: one provider owner, declared consumer owners, imported/exported actions, assumptions,
  and guarantees;
- `RefinementMap`: concrete steps, stuttering steps, identity mapping, and abstract effects;
- `Composition`: modules and adapters instantiated together plus the capability owners responsible
  for their cross-boundary properties; and
- `VerificationTarget`: an owner or co-owners, selected roots, properties, bounds, fairness,
  failure policy, and backend requirements.

A target is a projection of the canonical protocol, not permission to omit dependencies silently.
Generation should reject a selected property when the target excludes an entity, action, relation,
assumption, or refinement obligation it needs.

#### Contract discharge

The first contract representation should prefer stable identity over a general-purpose theorem
language. An interface declares named obligations. A provider defines each obligation once as
exported actions plus safety or progress properties in the verification IR; consumers import those
same obligation IDs rather than restating similar expressions. Structural validation checks that:

- every imported obligation names exactly one provider guarantee;
- the provider and all declared consumers agree on the interface and identity sorts;
- every provider guarantee is included in a module check which verifies it under only that module's
  external assumptions; and
- every adapter has a refinement target for each imported or exported action, including explicit
  stuttering mappings.

Exact obligation identity is the initial discharge rule; the compiler must not guess logical
implication between separately written expressions. The Go validator checks ownership, references,
dependency closure, and exact discharge. The generated backend check establishes each provider
property and refinement obligation. A target is successful only if both stages pass.

Assume-guarantee dependencies form a directed graph from a provider guarantee to the external
obligations used to prove it. The compiler topologically orders an acyclic graph. It rejects a cycle
unless a declared composition closes that strongly connected component and checks its internal
guarantees without assuming any guarantee from within the same component. This prevents feature
validity and delivery guarantees from proving each other circularly.

#### Target projection and hiding

Projection is a deterministic compiler pass:

1. Start from a target's selected modules, compositions, properties, and refinement maps.
2. Compute transitive closure over referenced entities, relations, actions, interfaces, identity
   maps, assumptions, guarantees, and properties.
3. Retain every action owned by the closure. An omitted action which cannot affect retained state or
   an exported observation may disappear.
4. Permit an omitted internal action to stutter only when an explicit refinement map classifies it
   as stuttering and it has no unmodeled effect on retained state.
5. If an omitted module controls an action which can affect retained state, retain a constrained
   environment action derived from an imported interface guarantee. Reject the target when no such
   guarantee exists.
6. Reject a target whose selected property, progress premise, refinement obligation, or minimum
   non-vacuous bound is not closed by the result.

The projector emits a closure report listing retained, stuttering, environment, and rejected
actions. Exporters consume only this validated projected IR; they do not implement independent
hiding rules. This makes a projection error visible before it can be repeated by every backend.

Illustrative targets are:

```text
protocol-atomic
foundation-delivery-safety
foundation-backlog-ack
foundation-ownership-fencing
feature-workflow
feature-workflow-speculative-delivery
feature-activity
feature-nexus
feature-callback
integration-workflow-delivery
integration-activity-delivery
integration-nexus-activity
integration-nexus-delivery
integration-callback-nexus
integration-callback-workflow
```

Each target should have independent bounds. For example, an ownership proof may need two owners but
only one queue; a duplicate-start check needs two attempts; a routing isolation check needs two
namespaces or queues; a feature lifecycle check may need no physical partition at all. A single
global identity bound wastes states and can accidentally make an invariant vacuous.

For the first non-vacuous finite targets, use at least two obligations or attempts for duplicate
and aliasing properties, two owner generations for fencing, two queues or namespaces for isolation,
and competing pollers where reservation matters. These are test-design minima, not a cutoff theorem.
TLC exhaustiveness is only for the constants in a particular
[finite model](https://lamport.azurewebsites.net/tla/model-popup.html), and P's `-s` option controls
the number of schedules explored by the
[checker](https://p-org.github.io/P/getstarted/usingP/). Unless Umpire proves a target-specific
cutoff, results must not be extrapolated from those bounds to arbitrary cluster size. Report a claim
as parameterized only when an Ivy or inductive proof actually establishes it.

### Failure modes of the split

| Failure mode | Consequence | Required control |
| --- | --- | --- |
| Two modules assume the same unproved fact | Both pass while their composition is invalid | Give each assumption an owning guarantee and reject undischarged contracts |
| Feature and delivery guarantees depend on each other | Both module checks succeed through circular assumptions | Reject the contract cycle or prove its closed composition without internal assumptions |
| Adapter maps the wrong identity or terminal state | Module checks pass but the system-level claim is false | Generate explicit refinement obligations and run small concrete compositions |
| Projection silently deletes an interfering action | Safety passes vacuously or liveness changes | Validate dependency closure and make hidden/environment actions explicit |
| One-identity bounds remove contention | Cardinality, routing, and ownership properties pass trivially | Define per-property minimum non-vacuous bounds and mutation tests |
| Fairness is implicit | A liveness result claims more than the system guarantees | Record fairness and availability assumptions in the target and result |
| All exporters share a lowering bug | Cross-backend agreement repeats the same error | Retain the Go interpreter, tiny reachable-world equivalence, and seeded semantic mutations |
| Formal actions have no live realization/observation | The abstraction is sound but says little about Temporal | Preserve Umpire refinement, causal-footprint, and observation-gap reporting |
| A common interface is made too feature-specific | Nexus or Callback inherits false Matching semantics | Keep L0 minimal and allow sibling implementations behind it |
| One canonical source becomes centrally owned | Cross-team changes bottleneck on a modeling group and domain declarations drift | Federate declarations by stable capability owner and compile them into one graph |
| A provider changes a contract unilaterally | Consumer proofs still pass against an obsolete assumption | Require affected consumer review and contract/refinement CI for exported changes |
| An integration target has no durable owner | Cross-capability properties decay while every local target stays green | Record explicit co-owners and reject ownerless compositions |
| An organization rename changes model identity | Artifacts and history churn for a non-semantic change | Keep capability IDs stable and update only the `CODEOWNERS` mapping |

### What should remain outside these models

Unless a property explicitly depends on them, exclude:

- arbitrary payload bytes and most protobuf fields;
- exact goroutine, channel, cache, and RPC-handler structure;
- logging, metrics, tracing transport, and error text;
- precise timeout durations rather than timeout ordering;
- every task-queue kind, routing policy, or deployment-version combination in one target;
- performance, capacity, and 10x-load claims; and
- implementation-specific fairness heuristics in the default foundation target.

Some of these deserve specialized tests or dedicated models. They should not inflate the semantic
acceptance model merely because Umpire can observe them.

### Boundary decisions for the first target

The model family can be designed now, but the first L0/L1 target must make these choices explicit:

- **Acceptance linearization:** for Workflow and Activity Tasks, treat the successful History
  start-authorization transaction as `AcceptStart`; Matching reservation and worker receipt remain
  distinct surrounding steps. If Temporal intends a different product guarantee, change the L0
  vocabulary instead of hiding the difference in a generator.
- **Ownership domains:** model History shard and Matching task-queue generations separately, with a
  mapping only where an action carries both. A single global “owner epoch” would prove a system
  Temporal does not implement.
- **Speculative Workflow Tasks:** keep them explicitly out of the first durable L1 target, then add
  a sibling direct-dispatch-with-durable-fallback target. The current inventory already identifies
  `SpeculativeTaskCreation` as an uncovered rule; silently treating it as an ordinary persisted task
  would erase its defining race.
- **Terminal drops:** enumerate which stale, expired, canceled, or otherwise invalid tasks may
  advance acknowledgement. “No task loss” is meaningful only after those permitted drops are part
  of the contract.
- **Live observation:** identify which IDs and outcomes can be reconstructed from Umpire facts before
  claiming that a formal action refines production behavior. A proof-only ghost identity may be
  useful internally, but it cannot discharge live conformance by itself.

These are contained design decisions, not reasons to merge all features into the foundation model.

### Suggested order of work

1. Add the structural authoring and IR prerequisites. Extend
   `tests/umpire2/protocol/protocol.go`, `compile.go`,
   `common/testing/umpire/verify/model.go`, and `validate.go` with `ModelFamily`, capability owners,
   modules, interfaces, compositions, refinement maps, and verification targets. Keep the existing
   closed `verify.Model` as the exporter input. Validate exact obligation discharge, ownership,
   references, and contract cycles. Keep `protocol.Default()` compiling the same full runtime graph
   used by the monitor, planner, relations, and coverage.
2. Declare the initial stable capability owners, map their source paths through `CODEOWNERS`, and
   assign provider, consumer, relation-schema, and integration ownership for the current protocol.
   Generation must reject unowned declarations and ownerless compositions.
3. Implement target projection in a focused `common/testing/umpire/verify/project.go` deep module.
   Its interface should accept a `ModelFamily` plus a target name and return either one closed
   `verify.Model` with a closure report or a validation error. Test dependency closure,
   explicit stuttering, constrained environment actions, missing contracts, and non-vacuous bounds
   in isolation before changing any exporter.
4. Teach `tests/umpire2/protocol.VerificationModel`, manifests, each exporter, and
   `cmd/umpire-genmodels` to consume projected targets. Add `-target`, retain `-profile` for
   `smoke`/`nightly`, and emit the target-scoped artifact layout. First reproduce the current model
   as `protocol-atomic` and require reachable-world and generated-action/property equivalence.
5. Define L0 `WorkObligation` and `DeliveryAttempt`, the exact `AcceptStart` linearization, stable
   obligation IDs, terminal outcomes, and the feature adapter contract. Seed wrong identity,
   terminal-state, and stuttering mappings and require contract/refinement checks to reject them or
   expose their counterexamples.
6. Derive the first L1 foundation target for one logical queue, bounded obligations and attempts,
   sync/backlog choice, History start acceptance/rejection, coarse retirement, retry, and abstract
   crash/ownership outcomes. Keep owner generations, routing topology, and backlog positions behind
   contracts. Seed task-loss, duplicate-start, split-commit, retry-identity, and premature-retirement
   faults in the Go interpreter and generated Ivy/TLA+/P targets.
7. Connect Workflow and Activity adapters; check each feature alone and in small concrete delivery
   compositions. Then add the speculative Workflow Task sibling target with direct dispatch,
   durable fallback, cancellation, and duplicate-start races. Seed a broken fallback or acceptance
   mapping so `SpeculativeTaskCreation` no longer remains an untested inventory entry.
8. Connect Nexus dispatch while preserving the existing lowered `nexus.start_activity` action and
   its reciprocal-link and terminal-strengthening properties. Check both the Nexus-only target and
   the Nexus + Activity composition, including an intentionally wrong feature/foundation adapter.
9. Add Callback's L0 sibling delivery adapter and Callback + Nexus and Callback + Workflow targets.
   Preserve its transport-specific retry and acknowledgement policy, and seed wrong operation/run
   identity plus terminal-lifetime faults.
10. Add L2 owner-generation fencing, partition, forwarding, poller-compatibility, and routing
    targets one property at a time. Each addition must refine the corresponding L1 contract and
    detect a target-specific stale-owner or route-crossing mutation.
11. Add L3 read/ack advancement, garbage collection, and fairness algorithms only where a production
    invariant demands their concrete structure. Each target must detect a backlog-position or
    acknowledgement mutation not expressible at L1.

`p-actors` is a separate follow-up after these targets are independently useful. Its machines,
mailboxes, and message failures must refine `p-semantic` or L0/L1, but actorization is not a
completion condition for this split.

The success criterion is not that every backend accepts a larger model. Each layer should detect a
seeded fault unique to that layer, every projection/refinement boundary should reject or expose a
mutation introduced at that boundary, and the integration targets should detect a deliberately
wrong feature/foundation adapter. Without those tests, splitting may improve runtime while merely
moving blind spots between models.

### Verification matrix

Verification is a gate at each milestone, not a final cleanup pass:

| Surface | Test anchor | Required evidence |
| --- | --- | --- |
| Authoring and IR | `tests/umpire2/protocol/compile_test.go`, `common/testing/umpire/verify/model_test.go` | Unknown owners/consumers, duplicate guarantees, undischarged obligations, and unclosed contract cycles are rejected |
| Projection | new `common/testing/umpire/verify/project_test.go` | Dependency closure is complete; omitted interference becomes a constrained environment action or an error; only declared actions stutter; minimum bounds are enforced |
| Runtime preservation | `tests/umpire2/protocol/default_test.go`, planner, coverage, relation, and monitor tests | `protocol.Default()` retains the unified runtime entity/action/relation graph and existing consumers do not depend on a projected target |
| Multi-target generation | `cmd/umpire-genmodels/main_test.go` and exporter tests | `protocol-atomic` matches the current kernel; every target has deterministic IR, manifest, backend artifacts, bounds, owners, and closure report; `-target` and execution `-profile` remain independent |
| Contract and adapters | protocol verification/refinement tests | Wrong obligation identity, terminal mapping, acceptance point, stuttering classification, and cyclic discharge fail independently |
| Semantic layers | focused L0/L1/L2/L3 and feature/integration mutation tests | Each layer detects a seeded fault which a shallower contract intentionally abstracts |
| Cross-backend semantics | interpreter and tiny reachable-world equivalence tests | A projected fault cannot disappear between the source graph, projected IR, and generated Ivy/TLA+/P target |

Run the focused pure Go checks first, always with the required build tag:

```sh
go test -tags test_dep ./common/testing/umpire/verify/... ./tests/umpire2/protocol/... ./cmd/umpire-genmodels/...
```

Then verify deterministic artifacts and the bounded smoke execution profile:

```sh
make umpire-check-genmodels
make umpire-verify-smoke
make lint-code
```

Backend-dependent tests may skip when their pinned tool is unavailable locally, but CI must exercise
the configured target/backend matrix. A milestone is incomplete if its seeded mutation is not
observed failing before the correct model passes.

### Answers to the design questions

- **How deep should Umpire go?** Through the durable dispatch and start-handshake protocol (L1) by
  default. Add ownership/routing and queue algorithms as focused targets when a stated invariant
  requires them. Stop before implementation mechanics that do not change enabled actions or
  correctness outcomes.
- **Same model or separate models?** One source model and semantic vocabulary; multiple generated
  modules and bounded targets. Do not use one universal state space or independently maintained
  specifications.
- **How do they integrate?** Through an explicit work-delivery interface, feature adapters,
  refinement maps, and selected composed verification targets.
- **How are they owned?** By stable capability owners whose declarations compile into one model
  family. Provider and consumer owners share interface changes; integration targets are explicitly
  co-owned; current team mappings live in `CODEOWNERS` rather than semantic identifiers.
- **Where is Ivy/P/TLA+ generation most valuable?** First in infrastructure safety and concurrency.
  Ivy is especially promising for relational safety contracts, TLA+ for integrated refinement and
  fairness, and the existing one-world P lowering for cross-checking atomic targets. A deliberately
  actorized P refinement is valuable later. Feature lifecycle generation remains useful but should
  usually sit above the delivery contract.

### Primary sources

- Temporal's [History Service architecture](./docs/architecture/history-service.md)
  documents the atomic mutable-state/task transaction, transfer processing, acknowledgement,
  ownership generations, and transactional-outbox relationship with Matching.
- Temporal's [Workflow lifecycle](./docs/architecture/workflow-lifecycle.md)
  traces Workflow and Activity Tasks through History, Matching, polling, and the start handshake.
- Temporal's [Matching Service architecture](./docs/architecture/matching-service.md) describes
  logical queues, partitions, forwarding, and reassignment; the current
  [`AddTask`](./service/matching/task_queue_partition_manager_interface.go),
  [backlog reader](./service/matching/pri_task_reader.go), and
  [History ownership-error](./service/history/shard/context_impl.go) paths supply the deeper
  implementation evidence used above.
- Temporal's [Nexus architecture](./docs/architecture/nexus.md)
  describes Nexus task dispatch through Matching and its synchronous response path.
- Ivy's [specification and isolate guide](https://microsoft.github.io/ivy/examples/specification.html),
  [language reference](https://microsoft.github.io/ivy/language.html), and
  [decidability guide](https://microsoft.github.io/ivy/decidability.html) describe isolates, action
  contracts, assume-guarantee reasoning, invariant preservation, and the logic-fragment constraint.
- P's [module system](https://p-org.github.io/P/manual/modulesystem/),
  [monitor documentation](https://p-org.github.io/P/manual/monitors/), and
  [checker guide](https://p-org.github.io/P/getstarted/usingP/) describe machine composition,
  abstraction replacement, safety/liveness observers, and bounded schedule exploration. The
  [ModP paper](https://ankushdesai.github.io/assets/papers/modp.pdf) formalizes module composition,
  trace refinement, and assume-guarantee validation.
- Lamport's *[Specifying Systems](https://lamport.azurewebsites.net/tla/book-02-02-28.pdf)* covers
  abstraction level, action granularity, fairness, specification composition, and interface
  refinement. The [safety-proof guide](https://lamport.azurewebsites.net/tla/proving-safety.pdf)
  shows refinement mappings as proof obligations, and the
  [safety/liveness note](https://lamport.azurewebsites.net/tla/safety-liveness.pdf) defines weak and
  strong fairness rather than leaving progress assumptions implicit.

## Umpire and FizzBee

Review date: 2026-08-13.

### Recommendation

FizzBee is a good candidate for a fourth generated Umpire verification language/target. Its
Python-like specifications and interactive state exploration could make the bounded protocol easier
to review; communication and sequence diagrams become useful only in a later role-based target. It
should enter Umpire as a generated `fizz-semantic` target beside TLA+, P, and Ivy, not as a new
authoritative model and not initially through FizzBee's separate model-based testing product.

There is no FizzBee integration in this repository today: the canonical protocol is compiled into
Umpire's [verification IR](./common/testing/umpire/verify/model.go), and the checked-in generated
targets are only [TLA+](./tests/umpire2/genmodels/protocol-atomic/tla),
[P](./tests/umpire2/genmodels/protocol-atomic/p), and
[Ivy](./tests/umpire2/genmodels/protocol-atomic/ivy). The design described in
[UMPIRE.md](./UMPIRE.md), the
[generated-verification backlog](#umpire-generated-verification-models-remaining-work), and the
[model-boundary plan](#umpire-model-boundaries) already provide the correct integration seam:

```text
tests/umpire2/protocol
          |
          v
validated verification snapshot
    |          |          |          |
    v          v          v          v
  TLA+         P         Ivy      FizzBee
                                      |
                                      v
                         normalized verification result
```

The first target should preserve the IR's current atomic transition semantics. A later,
deliberately separate `fizz-actors` target could use FizzBee roles, non-atomic actions, message
loss, and crashes to model the task-delivery foundation proposed in
[model-boundary plan](#umpire-model-boundaries). That target would be an execution refinement, not a more
convenient spelling of the same atomic kernel.

### What FizzBee provides

FizzBee is an Apache-2.0-licensed formal specification language and model checker for distributed
systems. Its language is Python-like and its expressions are based on Starlark; the project
provides a browser playground, macOS/Linux binaries, Homebrew installation, and a local `fizz`
command. The latest published release at the time of this review is v0.5.2, dated May 22, 2026.
See the official [project README](https://github.com/fizzbee-io/fizzbee/tree/993a2caa70d25996717a1d99ef26e7e682320649#fizzbee),
[release](https://github.com/fizzbee-io/fizzbee/releases/tag/v0.5.2), and
[license](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/LICENSE).

The features relevant to Umpire are:

- Actions can be atomic, serial, parallel, or nondeterministic `oneof` blocks. A serial action has
  implicit yield points at which other operations can interleave and the current operation can
  stop, while an atomic action is one checker step. This makes atomicity visible in source, but it
  also makes an accidental missing `atomic` semantically significant. See FizzBee's
  [getting-started guide](https://fizzbee.io/design/tutorials/getting-started/) and
  [procedural two-phase-commit explanation](https://fizzbee.io/design/examples/two_phase_commit_procedural/).
- Roles group state, actions, and functions and may represent a service, process, thread, database,
  or abstract protocol participant. Roles can be created dynamically and communicate through
  function calls. See the official [roles guide](https://fizzbee.io/design/tutorials/roles/).
- `always` assertions express safety. FizzBee also supports a subset of LTL for liveness and
  distinguishes unfair, weakly fair, and strongly fair actions and choices. Fairness is not
  implicit. See the official [liveness and fairness guide](https://fizzbee.io/design/tutorials/liveness/).
- `require` is an enabling condition; `any` and `oneof` introduce nondeterministic choice. This is
  a close surface match for Umpire action guards, typed binding enumeration, and branches. See the
  official [guard-clause guide](https://fizzbee.io/design/tutorials/guard-clause/).
- Non-atomic execution can inject message loss, network partitions, thread/process crashes, and
  loss of state annotated as ephemeral. Message duplication, Byzantine behavior, disk corruption,
  and memory corruption still require explicit modeling. See the official
  [fault-injection guide](https://fizzbee.io/design/tutorials/fault-injection/).
- Symmetric values and symmetric roles can reduce equivalent states, and order-independent
  collections avoid artificial permutations. See the official
  [symmetry-reduction guide](https://fizzbee.io/design/tutorials/symmetry_reduction/).
- The explorer can render state graphs, role communication diagrams, and per-path sequence
  diagrams from the same specification. See the official
  [visualization guide](https://fizzbee.io/design/tutorials/visualizations/).
- State-space controls include maximum action occurrences, maximum concurrent actions,
  action-specific bounds, deadlock detection, crash-on-yield behavior, and whether checking
  continues after an invariant failure. The documented defaults include 100 actions and two
  concurrent actions. See the official
  [configuration guide](https://fizzbee.io/design/tutorials/frontmatter/) and the authoritative
  [`StateSpaceOptions` schema](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/proto/statespace_options.proto).

These features make FizzBee attractive for bounded design exploration and review. They do not
change Umpire's assurance boundary: a generated FizzBee run checks an abstraction, not the live
Temporal server or the correctness of Umpire's facts, realizers, observation windows, and
refinement mappings.

### Why FizzBee should not become the source model

Umpire's source is more than a transition system. The compiled protocol owns facts and entity
subscriptions, lifecycle declarations, typed relations, live actions and explicit action gaps,
sparse-regression capabilities, causal footprints, verification properties, and mappings between
abstract actions and observations. The verification snapshot is only one derived view.

A `.fizz` file can describe states, actions, assertions, and role interactions, but it does not by
itself declare Umpire's Go fact decoders, live `Realizer` implementations, observation schemas, or
regression vocabulary. Making it authoritative would therefore require a second metadata system or
leave part of the protocol authoritative in Go. Either result violates Umpire's one-semantic-source
principle.

FizzBee also permits general Starlark-like expressions and imperative bodies. Importing arbitrary
FizzBee source into the current typed verification IR would require a restricted front end and a
second semantic IR capable of representing local variables, program counters, role calls, yield
points, and fault behavior. It would not be a parser-only change. FizzBee's own
[current-limitations page](https://fizzbee.io/design/tutorials/limitations/) documents expression
and function-call restrictions and warns that extracting a local variable into its own statement
can change concurrency behavior.

Generation in the other direction is contained: Umpire already has a small, validated expression
algebra, finite identity pools, explicit effects, and deterministic normalization. A FizzBee
exporter can reject anything it cannot preserve, following the same rule as the existing exporters.

FizzBee could become an authoring front end later, but only as a deliberately restricted Umpire
dialect that compiles into the same validated IR and also supplies the runtime protocol metadata.
That route is credible only if it eliminates, rather than creates, companion Go declarations for
facts, realizers, relations, refinements, and regression capabilities. The canonical semantic
boundary would still be Umpire's validated compiler output; arbitrary `.fizz` programs should not
become inputs to the other exporters.

### Mapping the current IR

The initial generator should use top-level finite data and atomic actions rather than roles. That
shape most directly preserves the current Go interpreter and one-world P semantics.

| Umpire verification construct | Initial FizzBee lowering |
| --- | --- |
| Entity type and finite `IDs` | A fixed ID collection plus maps recording existence and lifecycle state |
| `InitiallyExists` and initial state | Assign the existence/state maps in `Init` |
| One/many relation | A set of source/target pairs; generated assertions enforce endpoint existence and cardinality |
| Input parameter | `oneof` value from the declared finite type, constrained by the generated action guard |
| Fresh parameter | `oneof` identity whose existence bit is false |
| Observed parameter | Treat as a typed abstract input in the formal target; retain its observation requirement in provenance |
| Guard expression | A generated predicate used by `require` |
| Deterministic effects | Compute the post-state from the pre-state, then commit it in one `atomic action` |
| Branches | `oneof` branches, each with its own precomputed post-state |
| Create effect | Set existence and the declared initial state for the selected fresh identity |
| State effect | Update the selected entity's state map |
| Relation add/remove | Add or remove the selected tuple |
| Safety property | `always assertion` plus implicit relation well-formedness/cardinality assertions |
| Quiescent property | `always assertion` guarded by a generated `CanStep` predicate; do not recast it as liveness |
| Progress property | Unsupported in the first slice; add it only when FizzBee can preserve the declared fairness explicitly |
| Unrealized action | A normal abstract environment action, retained rather than omitted |
| Refinement and provenance | Manifest/name maps and generated comments; checked by Umpire validation, not inferred by FizzBee |

Using post-state temporaries is important. Merely emitting a sequence of assignments inside an
atomic block prevents interleaving but still lets later expressions observe earlier writes. Umpire
effects are simultaneous and unspecified state is framed as unchanged, so the generator must
evaluate every right-hand side against the pre-state before committing any update.

The FizzBee model should not use serial or parallel actions in `fizz-semantic`. FizzBee's implicit
yield and crash behavior is valuable only when the Umpire profile intentionally represents those
steps. Enabling it in the semantic backend would add transitions absent from the shared IR and make
cross-backend comparison meaningless.

### Generated artifacts and runner

A natural checked-in layout is:

```text
tests/umpire2/genmodels/fizz/
  Umpire.fizz
  fizz.yaml
```

`common/testing/umpire/verify/fizz` should own deterministic generation, expression lowering, name
escaping, and unsupported-construct reporting. The existing generation command should add the two
files and include the FizzBee binary version and checksum in
[`manifest.json`](./tests/umpire2/genmodels/manifest.json). Suggested environment and command names
are consistent with the existing backends:

```text
UMPIRE_FIZZ_TOOL
make umpire-genmodels
make umpire-check-genmodels
make umpire-verify-smoke
make umpire-verify-nightly
```

The FizzBee shell command first parses `.fizz` to a JSON AST beside the source and then invokes the
Go checker. Its flags support a chosen output directory, BFS/DFS/random exploration, trace replay,
simulation, and symmetry controls. The wrapper source is authoritative for that behavior:
[`fizz`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/fizz).
Umpire should invoke it on a temporary copy of the checked-in model so that the parser's adjacent
JSON file cannot dirty `genmodels`; native output should go to the requested result-artifact
directory.

The semantic target's generated configuration should set `max_concurrent_actions: 1`,
`crash_on_yield: false`, and `deadlock_detection: false`. Umpire actions are atomic, and terminal
quiescence is a valid condition checked by generated `CanStep` implications rather than a FizzBee
deadlock failure. `max_actions` should come from the selected Umpire profile and remain visible in
the normalized result.

The released wrapper has a critical CI behavior: the underlying model-checker process can print a
semantic `FAILED` or `DEADLOCK` result without returning a nonzero exit status. The v0.5.2 wrapper
works around this for parallel simulation by inspecting output, while its ordinary final invocation
does not. An Umpire runner must positively recognize the expected `PASSED` completion marker and
reject `FAILED` or `DEADLOCK`; process exit status alone is unsafe. See the official v0.5.2
[`fizz` parallel-result handling](https://github.com/fizzbee-io/fizzbee/blob/v0.5.2/fizz#L527-L530)
and [ordinary invocation](https://github.com/fizzbee-io/fizzbee/blob/v0.5.2/fizz#L609-L610).

Useful native artifacts include the parsed `spec_ast.json`, effective `state_config.json`,
`graph.dot`, `communication.dot`, `trace.txt`, and the error graph/HTML files produced on failure.
The FizzBee checker source shows these artifact names and also shows that CLI-level composition and
refinement take separate paths; currently only one refinement block is accepted. See the official
[`main.go`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/main.go).

FizzBee also exposes a Go library that accepts its JSON AST and returns an in-memory state graph,
but the documented library performs seeded simulation, not the full CLI workflow, and explicitly
lacks composition/refinement and automatic artifact generation. Umpire should begin with a pinned
external CLI, as it does for the other foreign tools, rather than add a new third-party Go module.
See the official
[`pkg/modelchecker` documentation](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/pkg/modelchecker/README.md).

### Result normalization

FizzBee results must use Umpire's existing
[`Result`](./common/testing/umpire/verify/result.go) vocabulary rather than introduce a FizzBee
`passed` Boolean.

| FizzBee outcome | Umpire status |
| --- | --- |
| Parser/type-check only | `generated` |
| Invariant or deadlock violation with a recoverable path | `counterexample` |
| Clean run under explicit action/concurrency/identity bounds | `bounded-no-counterexample` |
| Timeout, memory exhaustion, interruption, malformed output, or checker crash | `inconclusive` |
| Source construct the exporter cannot preserve | `unsupported` |

Do not classify an ordinary clean FizzBee CLI run as `invariant-proved`. Also classify it as
`finite-exhaustive` only if a pinned checker version exposes a machine-readable completion signal
whose semantics Umpire tests. FizzBee's limits are part of the explored model, and its human output
uses `PASSED`/`FAILED`; conservatively reporting `bounded-no-counterexample` avoids claiming more
than Umpire can independently establish. The runner should always record identity bounds,
`max_actions`, `max_concurrent_actions`, crash-on-yield, deadlock policy, exploration strategy,
symmetry policy, fairness, tool version, native stdout/stderr, and the replay command.

Counterexample conversion needs a generated bidirectional name map. FizzBee action/role names and
IDs must map back to Umpire action names and typed bindings; state changes must map to
`StateDelta`s; and the native trace must remain attached when conversion is partial. FizzBee has
guided replay through `--trace-file`/`--trace`, implemented by its official
[`trace.go`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/modelchecker/trace.go),
so Umpire can preserve both the normalized trace and an exact native replay command.

### Fit with Umpire's model family

FizzBee does not replace the existing backends; it adds a different review and exploration surface.

| Backend | Best Umpire role | FizzBee relationship |
| --- | --- | --- |
| Ivy | Parameterized relational safety and contracts in supported fragments | FizzBee does not replace an inductive Ivy proof |
| TLA+ | Reference transition semantics, bounded integration, refinement, fairness, and liveness | `fizz-semantic` should agree with it on shared finite safety worlds |
| P | Current atomic semantic cross-check; later actorized scheduling refinement | FizzBee roles provide another possible actor view, but with different RPC/crash semantics |
| FizzBee | Accessible bounded exploration, diagrams, and developer-facing counterexamples | Adds reviewability and an independent implementation of finite checking |

The model-family structure proposed in the [model-boundary plan](#umpire-model-boundaries) is still required.
FizzBee's current CLI contains composition and refinement implementations, but using those directly
would not supply Umpire's missing source-level `Module`, `Interface`, `RefinementMap`,
`Composition`, or `VerificationProfile` constructs. Umpire must first select and close a profile in
its own IR; a backend feature cannot decide which omitted actions stutter, remain environmental, or
invalidate a property.

For the later `fizz-actors` target, roles could correspond to a History shard, Matching partition,
poller, and synchronous response relay. Serial calls and durability annotations could then make
message loss, ambiguous results, retry, ownership changes, and crashes explicit. The target should
prove or bounded-check a refinement to the L0/L1 delivery contract and should retain the same split
used for `p-actors`: actorization is selected semantics, never an exporter side effect.

### FizzBee model-based testing

FizzBee Model-Based Testing is a separate product and binary. Its Go workflow generates a test
skeleton, maps model roles/actions to implementation adapters, optionally snapshots implementation
state, and runs sequential and concurrent scenarios. See the official
[MBT overview](https://fizzbee.io/testing/),
[mapping guide](https://fizzbee.io/testing/tutorials/getting-started/), and
[Go quick start](https://fizzbee.io/testing/tutorials/quick-start/).

That overlaps heavily with Umpire's existing Planner, Driver, `Realizer`, Monitor, fact log,
relation store, sparse regression compiler, reconciliation, and replay artifacts. Adopting it first
would create a second action/state adapter beside the canonical protocol and would not reuse
Umpire's richer observation model. It should therefore remain a later experiment.

MBT also treats `mbt.ErrNotImplemented` as a way to disable an unfinished action. That supports
incremental adoption, but it can make a run green while relevant behavior is absent. Any experiment
must fail a completeness/coverage gate when a selected Umpire action has no implemented adapter.
Moreover, FizzBee's CLI requires `--no-symmetry-reduction` when generating state graphs for MBT
replay from specifications that use symmetric roles or values, trading replayability for a larger
state space. See the official [Go MBT quick start](https://fizzbee.io/testing/tutorials/quick-start/)
and the wrapper's
[`--no-symmetry-reduction` documentation](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/fizz#L173-L177).

A useful experiment would export one small profile and implement adapters by delegating to existing
Umpire realizers, then compare scenario coverage and failure minimization. It is worth retaining
only if it finds concurrent live-system behavior that Umpire's own driver does not and the adapter
can be generated or mechanically validated. It should not become a hand-maintained second test
model.

### Implementation outline

#### First deliverable

The first deliverable is one vertical `fizz-semantic` slice from the existing verification snapshot
to a normalized Umpire result. It includes deterministic generation, a pinned CLI runner, native
artifacts, counterexample normalization, tests, and an optional smoke job. It does not add roles,
non-atomic actions, implicit faults, symmetry, progress properties, FizzBee composition/refinement,
or MBT.

The slice is complete when:

- `make umpire-genmodels` creates a deterministic checked-in FizzBee target;
- `make umpire-check-genmodels` detects any drift in that target;
- a selected smoke run invokes the pinned FizzBee release and returns an honest Umpire `Result`;
- the FizzBee and Go interpreters agree on tiny reachable worlds;
- seeded semantic faults produce counterexamples; and
- every configured bound is recorded and cannot yield `finite-exhaustive`, while an ambiguous,
  timed-out, or malformed run cannot be reported as successful.

#### Module and interface

Create a deep `common/testing/umpire/verify/fizz` module. Callers should not know FizzBee syntax,
configuration defaults, identifier rules, or file layout. Its small interface should mirror the
existing exporters:

```go
func Generate(model verify.Model) (map[string][]byte, []Diagnostic, error)
func RenderConfig(bounds verify.Bounds) ([]byte, error)
func ActionIdentifier(name string) string
func PropertyIdentifier(name string) string
```

`Generate` returns `Umpire.fizz` and diagnostics for constructs that the first slice cannot
preserve. `RenderConfig` requires a positive `MaxDepth`, maps it to `max_actions`, and always emits
the semantic controls `max_concurrent_actions: 1`, `crash_on_yield: false`,
`deadlock_detection: false`, and disabled native liveness. Identifier functions provide the single
name mapping used by generation, result parsing, and tests. Internally, the module should:

1. validate and canonically order the `verify.Model` before writing anything;
2. allocate every generated identifier in one pass and reject reserved-word or normalization
   collisions;
3. emit finite existence/state maps and relation sets in `Init`;
4. lower each source action to one top-level `atomic action`, including typed `oneof` bindings,
   freshness checks, guards, branches, and simultaneous framed effects;
5. emit relation well-formedness/cardinality assertions, declared safety assertions, and
   quiescent assertions guarded by a generated `CanStep`; and
6. report progress properties and any future unsupported expression/effect as diagnostics rather
   than omit or weaken them.

Do not introduce another general exporter or runner interface for this work. The exporter packages
already share a stable convention, and the runner's backend switch is an existing seam with several
adapters. A new abstraction used only by FizzBee would be shallow.

#### Generated and runtime data flow

```text
verify.Model ---------------------> fizz.Generate ----------------> Umpire.fizz
verify.Bounds --------------------> fizz.RenderConfig ------------> fizz.yaml
Umpire.fizz + fizz.yaml ----------> pinned fizz CLI --------------> native output
native output + generated names --> runner normalization ---------> verify.Result
```

Check in the smoke configuration at `tests/umpire2/genmodels/fizz/fizz.yaml` so direct local runs
have explicit bounds. For every verification run, render a fresh profile-specific configuration
from `runner.Request.Bounds`; never rely on FizzBee defaults. The runner should assemble a
self-contained temporary directory containing the checked-in model and selected config because the
FizzBee wrapper writes a parsed JSON file beside its input. It should also retain a self-contained
copy in the result artifacts so the replay command does not point into a deleted temporary
directory.

#### Generator command wiring

Update `cmd/umpire-genmodels` as follows:

- add FizzBee v0.5.2 and per-platform release checksums to `pinnedTools`;
- merge `fizz.Generate` output under `tests/umpire2/genmodels/fizz`;
- convert generator diagnostics to manifest `Unsupported` entries with source provenance;
- render the checked-in smoke `fizz.yaml` from the existing smoke `verify.Bounds`;
- add `fizz` as an explicit smoke backend, but leave it out of `all` while the job is experimental;
- add `-fizz-tool` and `UMPIRE_FIZZ_TOOL`, failing before execution when no tool is configured;
- populate action/property maps with the FizzBee identifier functions; and
- set the requested tool version and model directory in `runnerRequest`.

Keep the existing Make targets for generation and drift checking. The experimental CI job should
invoke the existing command with `-backend fizz -profile smoke`; add FizzBee to `all` only when that
job becomes required, and do not add a parallel generator command.

#### Runner adapter

Add `runner.Fizz` and an internal `executeFizz` adapter in
`common/testing/umpire/verify/runner`. The adapter should:

1. create a temporary working directory and copy `Umpire.fizz` into it;
2. render the selected `fizz.yaml` from the request bounds;
3. invoke the pinned wrapper in BFS model-checking mode with an explicit native output directory;
4. capture stdout, stderr, effective config, parsed AST, graphs, failure pages, and native trace;
5. preserve the exact model/config pair in the artifact directory and build replay against that
   pair; and
6. clean up the temporary directory on success, failure, timeout, or interruption.

Classification order is part of the interface. Timeout, interruption, resource limits, and an
unknown tool version are `inconclusive`. A recognized `FAILED` invariant with a recoverable path is
a `counterexample`, even when the process exits zero. A run is `bounded-no-counterexample` only when
the expected `PASSED:` marker is present and no `FAILED` or `DEADLOCK` marker appears. Missing or
contradictory markers are `inconclusive`; the exit code alone never establishes success.

Normalize native action names and nondeterministic choices through the generated name maps. Convert
adjacent native states to Umpire `StateDelta`s and preserve the failed property name. If any action,
binding, property, or state cannot be mapped without guessing, retain the native evidence and return
`inconclusive` instead of emitting a partial counterexample as authoritative.

#### Tests

Add tests at the same seams callers use:

| Location | Cases |
| --- | --- |
| `verify/fizz/generate_test.go` | Atomicity, simultaneous effects and framing, input/fresh bindings, branches, relation cardinality, `CanStep`, quiescence, deterministic ordering, escaping/collisions, and unsupported progress |
| `verify/fizz/generate_test.go` with `UMPIRE_FIZZ_TOOL` | Generated source parses; passing and failing tiny models produce the expected checker markers |
| `verify/runner/runner_test.go` | Command/config construction, temporary cleanup, zero-exit `FAILED`, positive `PASSED`, conflicting/missing markers, timeout/limits, artifact retention, trace normalization, and replay |
| `cmd/umpire-genmodels/main_test.go` | Complete artifact set, manifest tool/unsupported entries, backend selection, missing-tool errors, checked-in drift, seeded mutations, and Go/FizzBee reachable-world agreement |

Use the smallest models that distinguish semantics. In particular, include an action with multiple
state and relation effects, two identities for freshness/cardinality, a terminal state with a
failing quiescent property, and action names that collide after naive normalization.
Tool-backed tests should skip only when `UMPIRE_FIZZ_TOOL` is unset; pure generator and classifier
tests must always run.

The focused verification commands are:

```sh
go test -tags test_dep ./common/testing/umpire/verify/fizz/...
go test -tags test_dep ./common/testing/umpire/verify/runner/... ./cmd/umpire-genmodels/...
make umpire-check-genmodels
make lint-code
```

#### Rollout and deferred work

Land generation and pure tests before enabling foreign-tool CI. Add a non-blocking smoke job with
the pinned binary next; promote it to a required check only after reachable-world equivalence,
mutation tests, result parsing, and at least one tool upgrade have remained stable. Enable a nightly
profile only after smoke resource use is measured and its additional bound exercises states that
smoke does not.

`fizz-actors`, role diagrams, native fault injection, progress/fairness, composition/refinement,
symmetry, and MBT remain separate follow-up decisions. None should enter the first slice merely
because FizzBee supports it.

### Validation plan

The integration is ready to trust only after all of the following pass:

1. Generate a tiny model containing creation, state changes, relations, input/fresh bindings,
   multiple branches, and framing; compare its reachable states and transitions with the pure Go
   interpreter.
2. Lower safety and quiescent properties and verify both passing and deliberately failing models.
   Confirm that quiescence means no source action is enabled, not merely that the FizzBee scheduler
   stops at a configured bound.
3. Seed the same semantic mutations used for the current exporters: missing guard, sequential
   instead of simultaneous effects, reversed cardinality, missing branch, reused identity, weakened
   property, and incorrect relation endpoint. FizzBee must expose each relevant mutation.
4. Test deterministic generation, identifier escaping, reserved words, stable ordering, and exact
   checked-in output.
5. Test runner classification for parse failure, invariant violation, deadlock, timeout, killed
   process, clean bounded completion, and output from an unknown tool version.
6. Normalize a counterexample and replay it with the recorded native command. Keep the native
   artifact if any action, binding, or delta cannot be translated.
7. Run the smallest non-vacuous Umpire profiles: two identities for duplicate/aliasing properties,
   two owner generations for fencing, and two queues or namespaces for isolation. A one-identity
   smoke model is useful for wiring but cannot establish these claims.
8. Pin the exact FizzBee binary and checksum in CI; regenerate the model and re-run mutation and
   reachable-world equivalence tests on every upgrade.

Documentation-only findings do not warrant running Temporal's Go unit or lint suite. Implementing
the backend would require targeted generator/runner tests with `-tags test_dep`,
`make umpire-check-genmodels`, the FizzBee verification profile, and `make lint-code`.

### Risks and controls

| Risk | Consequence | Required control |
| --- | --- | --- |
| A generated action is accidentally non-atomic | FizzBee explores crashes/interleavings absent from the shared IR | Emit `atomic action` mechanically and reject backend-native bodies in `fizz-semantic` |
| Sequential assignments weaken simultaneous effects | FizzBee and Umpire reach different states while action names agree | Compute a complete post-state from pre-state before committing |
| Default bounds are accepted silently | A green run appears broader than it is | Generate explicit per-profile configuration and record it in every result |
| FizzBee `PASSED` is overclassified | A bounded exploration becomes a proof claim | Default to `bounded-no-counterexample`; require tested evidence for `finite-exhaustive` |
| Symmetry identifies semantically distinct IDs | Routing, ownership, or relation bugs disappear | Enable symmetry only for source-declared interchangeable sorts and cross-check without it on tiny models |
| Implicit fault injection changes the kernel | Cross-backend disagreements reflect different semantics rather than bugs | Disable crash-on-yield for `fizz-semantic`; isolate faults in `fizz-actors` |
| General Starlark leaks into generation | Escaping bugs or unexpected evaluation alter the model | Generate from typed IR only, validate identifiers/literals, and never interpolate untrusted runtime data |
| State-space growth at 10x identities/actions | Memory and time rise combinatorially | Keep independent bounded profiles, use source-justified symmetry, and separate semantic from actor targets |
| Checker/tool behavior changes | Results or parsers silently drift | Pin release plus checksum, retain native output, and gate upgrades on equivalence/mutation tests |
| FizzBee remains a young, changing tool | Diagnostics, APIs, or unsupported language corners impede CI | Begin as optional experimental backend and treat malformed/unknown output as `inconclusive` |

FizzBee's own documentation calls the implementation a work in progress and currently notes
incorrect diagnostic line numbers plus restrictions on where Fizz functions may be called. The
official reference material is also evolving; generated code must target a pinned release rather
than assume that the website's current syntax matches every installed binary. See the official
[limitations](https://fizzbee.io/design/tutorials/limitations/) and
[language gotchas](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/examples/references/GOTCHAS.md).

Security exposure is limited if FizzBee remains a build-time tool: it need not run in Temporal
production or receive credentials. CI should execute the pinned binary with a timeout in a clean
working directory, without network access or secrets, and publish only bounded generated state and
semantic identifiers. Umpire's existing rule that causal artifacts contain no secret payloads
still applies.

The practical verdict is **adopt experimentally as a generated semantic backend**. FizzBee's
accessibility and visual counterexamples complement Umpire well, while Umpire's canonical Go
protocol, verification IR, live observation, and normalized assurance vocabulary should remain in
control of what is modeled and what a result is allowed to claim.
