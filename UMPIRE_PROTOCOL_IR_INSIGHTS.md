# Umpire — Protocol IR Insights

> **Status: current architecture and forward design.** The v2 protocol, sparse regression domain,
> typed runtime relation store, focused semantic coverage/pairwise exploration, and normalized
> trace refinement are implemented. Richer constraints, causal refinement, and exporters remain
> subsequent phases.

Insights distilled from the shared ChatGPT discussion
[“Declarative vs Imperative Programming”](https://chatgpt.com/share/6a78d045-b678-83ea-85c2-09886bd17384),
interpreted in the context of Umpire's existing design documents and implementation.

## Thesis

Umpire is already converging on the useful core of a protocol intermediate representation:
entities with identity, executable lifecycles, declarative actions, relational references,
properties, failures, and observed refinements. The next step should not be a new Temporal-specific
language or a reimplementation of P, Ivy, or TLA+. It should be to make that existing semantic core
explicit and backend-neutral.

The target is a **typed relational transition system with refinement**:

```text
Protocol model
├── types
├── entities
├── relations and cardinality constraints
├── state
├── actions: preconditions + effects
├── nondeterministic choices
├── failures
├── logical time and progress
├── safety and liveness properties
└── refinement mappings
```

## Implementation status (2026-08-12)

The compatibility-backed Phase 1 slice now lives in `tests/umpire2/protocol`.
Its compiled declaration owns the current fact/entity catalog, lifecycle
planning, executable edge actions, and explicit action gaps. The Umpire 2
Monitor registers its model state from that declaration, and cross-version
tests compare its catalogs, plans, payloads, and fact routing with the renamed
`tests/umpirev1` baseline.

This completes catalog unification for Umpire 2 and establishes v2 as the
default `testcore` monitor while retaining v1 as an explicit compatibility
factory. A neutral testcore monitor boundary and an action-owned environment
interface remove Umpire 2's temporary transitive v1 coupling. Protocol-declared
relations now feed an indexed runtime store and the first lineage/link consumers;
focused coverage, pairwise generation, and trace refinement primitives are also
available. Generic constraints, relation-aware exploration, causal refinement,
and exporters remain future work.

This model can remain executable in Go while eventually supporting exporters to model checkers or
proof tools. The source of truth is the IR, not any one backend's syntax or worldview.

## The common semantic core

### Entities carry identity; relations carry structure

Nested object ownership is too rigid for distributed systems because relationships change over
time. Model identity and relationships separately:

```text
entity Workflow
entity WorkflowRun
entity Task
entity Worker

relation belongs_to(WorkflowRun, Workflow)
relation owns(HistoryShard, WorkflowRun)
relation queued_in(Task, TaskQueue)
relation leased_to(Task, Worker)
```

This generalizes Umpire's existing `EntityPath`, parent references, and run-lineage graph. It also
makes cross-entity invariants first-class rather than encoding them indirectly through object
fields or bespoke registry traversal.

### Ownership is a constrained relation, not a primitive

Ownership, containment, leasing, routing, visibility, and lineage should be library concepts built
from typed relations plus constraints. Cardinality is the reusable semantic primitive:

```text
relation owns(HistoryShard, WorkflowRun) {
    WorkflowRun -> exactly_one HistoryShard
}

relation leased_to(Task, Worker) {
    Task -> at_most_one Worker
}
```

From one declaration, backends can derive:

- a Go representation and runtime validation;
- planner constraints and relation-aware selectors;
- monitor invariants and coverage cells;
- P assertions, TLA+ invariants, or Ivy first-order properties.

The litmus test for a proposed primitive is: **can it be expressed as a relation plus constraints
without losing meaning?** If yes, it belongs in a library, not the IR core.

### Actions are the central transition primitive

An action states semantic intent without committing to its implementation mechanism:

```text
action CompleteActivity(activity) {
    requires activity.status == started
    effects  activity.status = completed
}
```

This matches Umpire's implemented actions model. The Planner sequences actions, the Driver realizes
them, and the Monitor observes their effects. Facts remain observations; they need not become the
IR's executable primitive.

### Failures are actions; retries and DLQs are protocol libraries

Crashes, drops, delays, duplicates, partitions, timeouts, and restarts should be modeled as allowed
actions or choices, not exceptional control flow outside the model. Retry and dead-letter behavior
then emerge as ordinary state transitions and policies:

```text
attempt_failed -> backing_off -> scheduled
attempt_failed at limit -> dead_lettered
```

Umpire already treats faults as action decorators and environment-controlled actions. The IR should
preserve this inversion: failure semantics are explicit, while their concrete injection mechanism
remains a realizer concern.

### Use logical order before wall-clock time

The shared discussion recommends logical time and happens-before constraints instead of embedding
milliseconds in the semantic model. That aligns with `UMPIRE_TRACING.md`:

- use history `EventID` where it exists;
- use internal transition/version counters where available;
- treat clockless operations as unordered sets inside clocked windows;
- map timers to virtual time in deterministic environments and wall time only in real backends.

Time belongs in the model only where it changes allowed behavior. Runtime duration and latency are
usually execution-policy concerns.

## Abstract meaning versus concrete execution

The most important insight is the **refinement boundary**.

Abstract action:

```text
CompleteActivity
```

Concrete realization:

```text
SendRequest
ReceiveRequest
ExecuteHandler
AppendHistoryEvent
PersistMutableState
SendResponse
```

A refinement mapping says which concrete observations implement the abstract action and which are
stuttering implementation steps that leave the abstract state unchanged.

This is the clean conceptual join between Umpire's existing layers:

| Umpire concept | IR role |
|---|---|
| lifecycle edge | abstract state transition |
| `Action` requires/effects | abstract operator |
| action `Kind` and realizer | implementation strategy |
| observed footprint | concrete execution trace |
| `Reconcile` | semantic conformance |
| `ReconcileFootprint` | concrete refinement/drift conformance |
| `EntityPath` / lineage | relation instances |
| rules | safety/liveness properties over state and relations |

The trace-derived footprint is therefore more than a fault-target list: it is an empirical first
version of a refinement mapping. A declared action says *what* happened; its footprint records
*how this implementation realized it*. Reconciliation checks both layers without conflating them.

## Do not make mechanisms core primitives

The IR should not have built-in concepts for RPC, queues, databases, history, goroutines, or
Temporal workflows. Those are reusable refinement libraries and execution policies.

For example, RPC is one implementation of an abstract action with a delivery contract:

```text
execution CompleteActivity {
    delivery = at_least_once
    may delay | duplicate | drop | timeout
}
```

P may lower this to messages between machines, TLA+ to alternative `Next` branches, Ivy to
relations and actions, and Go to an RPC adapter plus fault hooks. All must preserve the same set of
allowed behaviors.

The same applies to queues and persistence. Their correctness-relevant semantics—durability,
ordering, uniqueness, visibility, and recoverability—belong in relations and properties. Their
physical implementations do not.

## Backend roles

The discussed tools overlap, but each makes a different trade-off:

| Backend | Natural role |
|---|---|
| Go | canonical executable interpreter, runtime monitor, planners, drivers, and generated checks |
| P | bounded exploration of communicating machines, message order, crashes, and executable traces |
| TLA+ | architecture-level safety/liveness over global state and temporal behavior |
| Ivy | quantified invariants and refinement proofs for selected stable, critical protocols |
| Dafny | local contracts and verified generated components or data structures |
| Coq | formal semantics or exceptionally critical kernels, not routine feature development |

The IR must represent the **intersection** of backend semantics. Backend-only features should be
explicit extensions with capability checks, not silently approximated. An exporter must fail when
it cannot preserve meaning.

For normal Temporal development, bounded model checking and deterministic simulation are likely to
deliver more value per unit of complexity than theorem proving. Proof backends remain useful for
critical, stable sub-protocols and refinement boundaries.

## The specification gap remains

Generating Go from a model does not by itself prove that a production system implements the
model. The assurance chain is:

1. Define abstract behavior and properties.
2. Define or observe concrete implementation behavior.
3. Map concrete states and steps to abstract ones.
4. Generate the transition kernel where practical.
5. Check real executions against both semantic effects and concrete refinements.

Umpire already owns steps 2 and 5 through its Monitor, action reconciliation, lineage, and learned
footprints. That makes it a stronger foundation for closing the specification gap than a compiler
that only emits a state-machine skeleton.

Production code generation should therefore be optional and incremental. The immediate value is a
single executable model shared by planning, driving, observing, conformance, coverage, and
simulation.

## What this changes for Umpire

### Preserve

- `Lifecycle` as the executable transition function.
- `Action` as the declarative operator with preconditions and effects.
- `Ref`/`LinkedFrom` as the beginning of symbolic-to-concrete entity binding.
- Monitor facts as the normalized observation channel.
- safety/liveness rules for properties that do not collapse into model structure.
- action and footprint reconciliation as the two levels of conformance.
- environment capabilities as explicit declarations of which semantics can be observed or driven.

### Deepen

1. **Make relations explicit.** Generalize parent paths and lineage into typed relation declarations
   and runtime relation state.
2. **Add relation constraints.** Start with direction, cardinality, uniqueness, and optionality;
   derive generic conformance checks.
3. **Allow relational action effects.** Actions should add/remove relation tuples as well as fire
   lifecycle events.
4. **Name abstract/concrete refinement.** Treat action footprints as one concrete mapping and make
   stuttering operations explicit.
5. **Separate execution policy from semantics.** Delivery, retry, fault, and timing behavior should
   decorate actions without defining their abstract effect.
6. **Keep nondeterminism explicit.** Represent allowed outcomes as choices/policies rather than
   forcing every action into one unconditional effect when the environment cannot control it.

### Avoid

- inventing Temporal-specific core primitives for History, Matching, RPC, queues, or storage;
- making nested Go object structure the semantic relationship model;
- targeting the union of P, TLA+, Ivy, and Go features;
- starting with arbitrary quantifiers, SMT, or a theorem prover;
- claiming generated code eliminates model/implementation drift;
- silently weakening properties that a backend cannot represent.

## A practical incremental path

### Phase 1 — Canonical Go IR

Define a small, pure Go `Protocol` model containing types, entities, relations, state variables,
actions, properties, and refinement declarations. Adapt existing lifecycles/actions into it rather
than replacing them.

Success criterion: the Monitor model catalog and Planner action catalog derive from the same
declaration without losing current behavior.

### Phase 2 — Relational state and generic constraints

Represent relation tuples in `ModelState`; support relation preconditions/effects and generic
cardinality checking. Migrate workflow/run lineage and parent-child identity first because they are
already relational and well-tested.

Success criterion: lineage and ownership-style invariants require no bespoke traversal rule.

### Phase 3 — Refinement-backed footprints

Formalize the connection between an abstract action and its observed concrete operation set.
Distinguish required, optional, ambient, and stuttering operations; preserve capability provenance.

Success criterion: semantic reconciliation and footprint reconciliation explain one failure as two
clearly separated contracts.

### Phase 4 — Deterministic interpreter and explorer

Execute the canonical IR against immutable or cheaply cloned world state. Add bounded exploration,
failure insertion, replay, and counterexample minimization before adding a foreign backend.

Success criterion: the same declared action sequence runs against both the pure interpreter and a
Temporal realizer, with comparable observations.

### Phase 5 — One exporter as a semantic test

Choose P or TLA+ based on the first concrete property that exceeds the Go explorer. Treat exporter
construction as a test of IR clarity, not as the initial architecture driver.

Success criterion: differential checks show the Go interpreter and exported model agree over a
bounded corpus; unsupported semantics fail explicitly.

## Design risks

- **State explosion:** explicit relations multiply world states. Bound entities, canonicalize
  identities, exploit symmetry, and keep coverage budgets visible.
- **Semantic leakage:** backend or Temporal implementation concepts can creep into the core. Keep
  them in libraries and refinements.
- **False refinement confidence:** an observed footprint proves co-occurrence, not causality. Keep
  provenance and match policy explicit; add causal links only where necessary.
- **Generated-code lock-in:** production generation can make every optimization a language feature.
  Prefer a generated semantic kernel plus handwritten adapters.
- **Liveness ambiguity:** finite traces cannot prove unbounded liveness. Preserve Umpire's pending
  verdict and teardown/bounded-progress semantics.
- **Backend drift:** test exporters against the canonical interpreter with shared fixtures and
  counterexamples.

## Bottom line

The conversation's “LLVM for distributed protocols” analogy is useful only if interpreted
carefully. Umpire should not become a universal distributed-systems compiler. It can become a deep
module whose small semantic interface—typed entities and relations, actions, properties, choices,
and refinements—supports several consumers:

```text
                          ┌─ Monitor and conformance
                          ├─ Planner and coverage
Canonical protocol IR ────┼─ Driver and fault exploration
                          ├─ Deterministic interpreter
                          └─ Optional verification exporters
```

That direction strengthens what Umpire already does, keeps the implementation incremental, and
turns its most distinctive assets—runtime observation, cross-entity reasoning, action planning,
and learned operational footprints—into the bridge between abstract specifications and the real
Temporal server.
