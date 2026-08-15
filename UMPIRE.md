# Umpire

Umpire is model-based acceptance testing for Temporal. It drives a running server, converts
observations into a typed model, and evaluates that model against shared properties. Tests describe
semantic behavior instead of duplicating setup, polling, and assertions.

```text
scenario -> compile -> plan -> realize against Temporal
                                  |
                                  v
facts <- gRPC, OTEL, history, and in-process observation
  |
  v
model state + relations -> rules, reconciliation, and coverage
```

The canonical implementation is `tests/umpire2`. It is the default monitor used by `testcore`;
`tests/umpire1` remains a compatibility implementation.

## Design principles

- **One semantic source.** Facts, entities, lifecycle edges, relations, executable actions, and
  explicit action gaps are compiled from one protocol declaration.
- **Observe and act through separate seams.** Realizers change the system under test. The Monitor
  only records evidence and evaluates it.
- **Model intent, not mechanics.** Actions describe preconditions and effects. RPCs, workers,
  callbacks, timers, and faults are realization details.
- **Make unsupported behavior explicit.** Compilation rejects unknown vocabulary, impossible
  routes, missing capabilities, invalid bindings, and unbounded path enumeration.
- **Keep evidence replayable.** Completed plans, bindings, facts, action windows, and verdicts are
  recorded independently of the sparse source that produced them.
- **State claims precisely.** Coverage is evidence that an obligation was exercised, not proof that
  the model is correct. Finite execution can establish bounded progress, not unbounded liveness.

## Canonical protocol

`tests/umpire2/protocol` compiles the Temporal declaration into an immutable `Protocol`. The
declaration contains:

- normalized fact types and entity factories;
- each entity's fact subscriptions and executable lifecycle;
- typed relation schemas and fact-derived relation mutations;
- actions associated with exact lifecycle edges;
- explicit reasons for lifecycle edges without atomic realizers; and
- the sparse-regression domain used by the compiler.

Compilation validates names, subscriptions, lifecycle edges, hosting, action effects, relations,
and gaps before a test creates a cluster. Consumers use compiled catalogs rather than maintaining
parallel lists.

The protocol has several derived views:

- the runtime Monitor registration;
- route planning and action lookup;
- sparse regression compilation;
- semantic coverage catalogs and pairwise matrix inputs; and
- the bounded verification model-family projection.

## Monitor: observe and evaluate

`common/testing/umpire` is the Temporal-independent framework. `tests/umpire2` supplies the
Temporal facts, entities, relations, and rules.

Every recognized observation becomes a `Fact` addressed by an `EntityPath`. `ModelState` routes
facts to entity instances, `FactLog` retains the evidence, and `RelationStore` applies typed
cross-entity links. A `Lifecycle` is an executable transition function: for every observed event it
classifies the transition as advancing, tolerated, or illegal and records the resulting state.

The Monitor receives evidence from:

- frontend and internal gRPC requests, responses, and rejections;
- completed OTEL spans and span events;
- public history or response-derived facts; and
- explicitly normalized in-process observations.

Rules consume model state and relations, not wire types. Safety rules evaluate invariants at
runtime checkpoints; liveness rules maintain pending obligations that are resolved during final
namespace evaluation. Functional tests check and purge one namespace at teardown. Sparse
regressions add safety checkpoints after actions and observed milestones.

The Monitor deliberately ignores unregistered traffic. Missing evidence is therefore not, by
itself, evidence of correctness. A property must be paired with observation channels capable of
establishing its preconditions and outcome.

## Planner and Driver: act

The pure planner in `common/testing/umpire/planner.go` searches an entity lifecycle under explicit
constraints. It supports a shortest route, all routes, reproducible random selection, and bounded
exploration. Planning happens before execution and fails when the target is unreachable.

An `Action` declares semantic preconditions and effects over symbolic entity references. A
`Realizer` turns that action into Temporal traffic or controlled environment behavior. The generic
drive loop waits for observable preconditions, invokes proactive actions, installs reactive
actions, binds newly observed identities, and reconciles declared effects with the model.

Sparse regressions use the same concepts at a higher level. Their compiler fills the gaps between
author-supplied semantic key frames, synthesizes resources and data flow, and emits a completed
plan for the live harness. It rejects incomplete action, policy, and resource realizations before
environment allocation.

## Identity and relations

Identity follows the system that creates each identifier:

- Namespace name-to-ID mappings are seeded before traffic because the harness knows both values.
- Server-minted workflow Run IDs are learned from observations.
- Successor runs bind through observed lineage relations rather than guessed or precomputed IDs.
- Cross-entity structure such as callback, handler, activity, operation, and workflow links lives
  in typed relations instead of being inferred repeatedly by individual rules.

This separation lets a plan use stable symbolic names while the runtime grounds them to concrete
Temporal identities. It also makes inconsistent relation updates explicit violations instead of
silently choosing one interpretation.

## Divergence and faults

Input divergence and execution faults are modeled separately:

- A parameter `Domain` generates valid values and labeled neighboring variants. A variant may
  expect synchronous rejection, normalization, or alternate semantic effects. Rejections become
  ordinary facts and are reconciled against the driven variant.
- A fault policy perturbs an action's realization, such as an RPC occurrence. Fault campaigns use
  normal safety and progress properties plus explicit expected outcomes.

Invalid cases mutate one field of a valid base request. This keeps attribution and minimization
tractable. Reflection and registered validators supply reusable domains where available; semantic
overlays cover information such as entity references that descriptors cannot express.

## Observation and refinement

Semantic reconciliation asks whether an action produced its declared model effects. Causal
footprint reconciliation asks whether the concrete action window contained the expected normalized
observations. These are different contracts: a server may preserve the semantic outcome while its
implementation footprint changes.

Facts carry source event time or order where the source provides it. Trace records use normalized,
bounded action windows and verdicts; causal footprints contain non-secret semantic identifiers.
Ordering prefers a source EventID, then an internal monotonic counter, then an unordered clocked
window. Raw cross-process causality remains dependent on the available observation channel, so a
footprint must not be treated as an independent correctness oracle.

## Evidence profiles and portable environments

Every live claim is qualified by an `EnvironmentProfile`. The profile separates where behavior
runs (`local`, `ci`, `deployment`, or `canary`) from the evidence it can retain (public API,
history, telemetry, or in-process facts). A property declares its required sources and ordering.
Unavailable sources produce `unsupported`; evidence loss, ambiguous identity, conflicting lineage,
or incomparable clocks produce `inconclusive`. Neither outcome is a successful property claim.

Trace events retain causal references, source sequences, and clock domains. Cross-domain event
timestamps never establish causality by themselves. Ordered properties require a causal path or a
comparable source sequence. Sparse regression artifacts and formal results retain the selected
profile, observations, omissions, bounds, and qualified claims.

Environment mechanics remain behind capability-owned interfaces. The same completed behavioral
intent runs through local, CI, deployment, and approved canary profiles; the profile changes its
realization and justified evidence, not its sparse IR.

## Coverage and exploration

`Coverage` compares a declared catalog with observed facts, transitions, relations, and actions.
`tests/umpire2/protocol` derives these catalogs from the compiled protocol. `Coverage.Unmet()` is a
planning and reporting signal; it does not assert that an observed behavior was correct.

The framework also provides deterministic constrained pairwise generation. The Temporal matrix
adapter exposes declaration-ordered axes without creating a cluster. Scenario assembly and live
execution remain separate so combinatorial selection stays pure and bounded.

Exhaustive and sampled modes traverse the same constrained scenario space. Pinned regressions are
always included outside the sampling budget.

All bounds must be visible. Exhaustive modes fail when a caller-supplied limit would truncate the
space; exploratory modes record their seed and budget.

`common/testing/umpire/campaign` composes these pieces into a bounded discovery loop. One request
contains a completed behavioral template, declared risk, semantic coverage, matrix and lifecycle
exploration inputs, environment profile, seed, corpus, and hard budgets. The campaign records why
each scenario was selected or omitted, deduplicates semantic plans without runtime identities,
executes each scenario in an isolated environment, and retains qualified evidence.

A qualified violation is minimized monotonically across actions, policies, faults, resources, and
unused bindings. Each accepted reduction must reproduce the same property violation with complete
cleanup and evidence. The minimized experiment is replayed semantically; schedule drift is reported
separately from observation or violation drift. Only stable evidence produces a deterministic sparse
regression candidate for human review.

## Guarded canaries

`common/testing/umpire/canary` enforces an immutable safety envelope before allocating resources.
The envelope requires campaign, namespace, and tenant isolation; explicit action and fault
allowlists; traffic, fault, concurrency, duration, evidence, and cleanup budgets; and secret-safe
retention. Local fault capability never grants canary authority. Destructive actions or faults need
both envelope approval and a canary-specific drive capability.

Execution stops on invariant failure, observation loss, action failure, cancellation, timeout, or
budget exhaustion. Concurrent workers reserve hard count and evidence budgets before starting
actions. Execution and cleanup deadlines are supplied to the driver, whose transport or process
boundary must honor context cancellation. Cleanup uses an uncancelled deadline context on every
post-preparation path, and the result retains recovery-safe resource metadata plus a redacted audit
trail.

## Assurance boundaries

Umpire is strongest when the model, observer, and property are independently challenged. A green
run establishes only the claim supported by its model version, enabled observations, execution
profile, and explicit bounds.

In particular:

- partial observation can leave several real executions compatible with the same model state;
- quiescent or teardown progress checks are bounded claims, not proofs of eventual completion;
- a planner and oracle derived from the same declaration can share the same mistake;
- model and implementation mutation tests are needed to demonstrate fault-detection power;
- generated formal models verify the abstract protocol, not Temporal's refinement of it; and
- seeds alone do not reproduce uncontrolled distributed scheduling, so artifacts retain the
  completed semantic experiment and observations.

Tests for exact protobufs, authorization, performance, schemas, metrics, and low-level races should
remain specialized when their contract sits below the protocol abstraction.

## Code map

| Location | Responsibility |
| --- | --- |
| `common/testing/umpire` | Generic facts, entities, lifecycles, relations, rules, actions, planning, coverage, matrices, and tracing |
| `common/testing/umpire/regress` | Sparse-plan normalization, compilation, execution, and artifacts |
| `common/testing/umpire/campaign` | Bounded selection, corpus, execution, minimization, replay, and regression candidates |
| `common/testing/umpire/canary` | Guarded canary authority, budgets, audit, redaction, and cleanup |
| `common/testing/umpire/verify` | Verification snapshot, interpreter, exporters, runners, and normalized results |
| `tests/umpire2/protocol` | Canonical Temporal protocol and its derived views |
| `tests/umpire2/model`, `fact`, `rule` | Temporal observation and runtime evaluation |
| `tests/umpire2/action` | Temporal realizers, fault policies, and regression harness |
| `tests/umpire2/regress` | Typed Temporal authoring vocabulary |
| `tests/umpire2/genmodels` | Deterministically generated verification artifacts and manifest |

The implementation and tests are authoritative for current vocabulary. The generated
[`manifest.json`](./tests/umpire2/genmodels/manifest.json) inventories semantic targets and their
hashes; each target manifest, such as
[`protocol-atomic/manifest.json`](./tests/umpire2/genmodels/protocol-atomic/manifest.json), is
authoritative for that target's bounded formal verification scope. Planned work belongs in tracked
issues rather than status sections in this document.
