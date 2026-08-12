# Umpire — Monitor (the passive half): architecture

> **Status: current architecture.** The protocol-backed v2 monitor is the suite-wide `testcore`
> default; v1 remains explicitly selectable for compatibility. V2 adds WorkflowRun, Activity,
> Nexus link/timeout/cancellation observations, typed runtime relations, and opt-in semantic
> coverage and normalized tracing. `UMPIRE_PLAN.md` tracks the remaining adoption work.

How the pieces fit together. For *why* it exists read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md);
for current status, gaps, and the rule inventory read [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).

Umpire watches a running Temporal server and rules on invariants. It never drives
behaviour — tests do that. Umpire only **observes** (gRPC traffic + OTEL spans),
**models** what it sees (entity state machines), and **judges** the model (rules that
emit violations).

```
   observe                     model                        judge
┌────────────┐   Facts   ┌──────────────────┐         ┌──────────────┐
│  Decoder   │ ────────▶ │     ModelState     │ ──────▶ │   RuleRegistry   │ ──▶ Violations
│ (wire+span)│           │  (entity FSMs +  │  dirty  │ safety +     │
└────────────┘           │   generations)   │  query  │ liveness     │
      ▲                  └──────────────────┘         └──────────────┘
      │                          │
 gRPC + OTEL                  FactLog (queryable record of every fact)
```

The central idea: **everything observed becomes a `Fact` addressed to one entity.**
Wire formats and change-tracking live in one place; rules just read entity state.

## Two layers

| Package | Role | Knows about |
|---|---|---|
| `common/testing/umpire/` | **Framework** — generic machinery | facts, entities, routing, rules — *not* Temporal |
| `tests/umpirev1/` | **V1 domain** — compatibility implementation | workflows, tasks, Nexus operations, gRPC/span shapes |
| `tests/umpire2/` | **V2 domain** — canonical direction | protocol catalog, WorkflowRun, Activity, richer Nexus rules, sparse regressions |

The framework never imports the domain. Adding a rule or entity is a domain change; the
framework stays put.

---

## Framework types (`common/testing/umpire/`)

### Addressing — who a fact is about

- **`EntityType`** — a string tag for a kind of entity (`"Workflow"`, `"WorkflowRun"`, …).
- **`EntityID`** = `{Type, ID}` — one entity of a type.
- **`EntityPath`** = `{EntityID, Ancestors []EntityID}` — an `EntityID` qualified by zero or more
  root-first ancestors. This is what a fact *targets*. `EntityPathKey(path)` serialises it to the
  canonical root-first registry key (`ancestorType:id@type:id`).

### `Fact` — the unit of observation (`entity.go`)

```go
type Fact interface {
    Name() string             // fact type; must equal the struct name
    TargetEntity() *EntityPath // which entity this fact is about
}

type BroadcastFact interface { // delivered to *every* entity of a type
    Fact
    BroadcastType() EntityType
}
```

A `Fact` is inert data. It carries no logic beyond saying what it is and who it concerns.

### `Entity` — the state machines (`entity.go`)

```go
type Entity interface {
    Type() EntityType                                            // must equal the struct name
    OnFact(ctx, path *EntityPath, facts iter.Seq[Fact]) error    // advance the FSM
}
type EntityFactory func() Entity
```

An entity interprets a stream of facts and holds the resulting state. Rules read that
state; they never see facts directly.

### `ModelState` — routing + dirty tracking (`model_state.go`)

The heart of the model layer — the runtime `*State` counterpart to the protocol's declarations.
It:

1. **Stores** entities keyed by `EntityPathKey`, each wrapped in an `entityRecord`
   `{entity, generation}`.
2. **Routes** — `RouteFacts([]Fact)` groups facts by target, lazily creates the target
   entity (and its parent chain) via the registered factory, and hands each entity its
   batch through `OnFact`. `BroadcastFact`s go to all entities of a type (nil path).
3. **Tracks change by generation** — a single atomic counter. Every time an entity
   receives facts, its record is stamped with the next generation value. This is how rules
   avoid re-examining everything: `QueryEntities(type, sinceGeneration)` returns only
   entities stamped *after* a watermark.

Registration (the `EntityRegistry` role) is validated at wire-up time:
- `RegisterEntity(factory, …)` panics unless `entity.Type() == structName`.
- `RegisterFact(probes…)` panics unless `fact.Name() == structName`.

### `RuleRegistry` — the judges (`rule_registry.go`)

Two rule kinds, mapping to strong vs. eventual consistency:

```go
type SafetyRule interface {   Name() string; CheckSafety(*SafetyContext)   }
type LivenessRule interface { Name() string; CheckLiveness(*LivenessContext) }
```

- **Safety** — must hold at *every* observation. A rule calls `c.Eval(key, ok, violation)`
  or `c.Pass(key)`; a false `ok` is an immediate `Violation`.
- **Liveness** — must hold *eventually*. A rule calls `c.Pending(key, violation)` while a
  condition is unmet and `c.Resolve(key)` once met. Anything still pending at the final
  check becomes a violation.

Both context types embed `ruleContext` (the registry, logger, `sinceGeneration` watermark,
per-rule `ruleState`) and expose entities through one generic query:

```go
for r := range umpire.ChangedEntities[model.NexusOperation](c) {
    op := r.Entity   // *model.NexusOperation, only if changed since last check
    ...
}
```

`RuleRegistry` responsibilities:
- **Register** rules by name (`RegisterSafety`/`RegisterLiveness`). A rule's `Name()` must
  equal `structName + "Rule"` — enforced at registration.
- **Init** a selected subset (or all) against a registry + logger (`InitRules`).
- **Check(ctx, final)** — runs every rule over only its dirty entities, advances each
  rule's generation watermark, and dedups repeat reports (`reportInterval`). When
  `final` is true, unresolved liveness `Pending`s are promoted to violations.

A **`Violation`** is `{Rule, Message, Tags}` — the framework's only output.

### Supporting pieces

- **`FactLog`** (`fact_log.go`) — an append-only, queryable record of every fact
  (`QueryByType`, `QueryByID`, `All`). Independent of the FSMs; useful for test assertions.
- **`interceptor.go`** — a gRPC unary interceptor built from optional hooks:
  `FactRecorder.RecordFact` (observe requests), `ResponseRecorder.RecordResponse` (observe
  responses), `RejectionRecorder.RecordRejection` (observe errors), and `FaultInjector.Inject`
  (apply configured transport faults).
- **`instrument.go`** — helpers for *producing* observations from inside the server:
  `Instrument`/`RecordFact` emit OTEL spans/events under `TracerName`, `EntityTag` stamps a
  span with the entity it concerns.
- **`Lifecycle`** (`lifecycle.go`) — the total transition oracle, transition history, traits,
  and stable declared/visited edge catalogs used by planning, conformance, and coverage.

---

## Domain layers (`tests/umpirev1/`, `tests/umpire2/`)

### `Monitor` — the orchestrator (`monitor.go`)

Wires the framework to Temporal and is the object tests hold. It owns a `ModelState`, a
`FactDecoder`, a `RuleRegistry` (with all default rules registered), and a `FactLog`.

It plugs into the server two ways:
- **OTEL** — implements `sdktrace.SpanProcessor`. `OnEnd(span)` decodes span events →
  `RouteFacts`. Synchronous (no batch delay), so per-PR cost stays low.
- **gRPC** — implements `FactRecorder`/`ResponseRecorder`. `RecordFact`/`RecordResponse`
  decode the request/response → append to `FactLog` → `RouteFacts`.

Tests call `CheckNamespace` at teardown to promote unresolved liveness conditions for one
namespace, then `PurgeNamespace` to remove that namespace's entities, facts, and rule state.

### `FactDecoder` — wire/span → `Fact` (`model/fact_decoder.go`)

The single place that understands Temporal wire formats. It holds registered importers and
tries each:
- **`RequestFact.ImportRequest(any)`** — type-asserts a gRPC request into a fact.
- **`ResponseRecorder` path** — turns a request+response pair into a fact (e.g. a poll that
  actually returned a task).
- **`SpanFact.ImportSpanEvent(attrs)`** — builds a fact from an OTEL span event whose name
  matches the fact's `Name()`.

### Entities — Temporal FSMs (`model/`)

V2 declares `Workflow`, `WorkflowRun`, `WorkflowTask`, `TaskQueue`, `NexusOperation`, and
`Activity` (with `Namespace` as a scoping identity). Each implements `Entity` and uses
`umpire.Lifecycle` for executable state. `model.DefaultEntities` and `model.DefaultFacts` feed the
compiled protocol, which declares subscriptions and executable actions together.

### Facts and rules

- **`fact/`** — one struct per observable thing, implementing `SpanFact` or `RequestFact`.
  Its `Name()` equals the struct name (and, for span facts, the OTEL event name).
- **`rule/`** — v1 registers two safety and two liveness rules; v2 registers four safety and two
  liveness rules. Each reads entity state via `ChangedEntities[T]` and emits `Violation`s.
- **`entity_key.go`** — a small fluent builder (`Workflow(id).Update(id)` / `.Task(…)`)
  that produces the same registry key strings the router uses, so tests can name the exact
  entity a rule should have passed (`RequireRulePassed`).

---

## End-to-end: the life of one observation

1. The server handles a gRPC call or emits an instrumented OTEL span.
2. The interceptor / `SpanProcessor` hands it to `Monitor`.
3. `FactDecoder` turns it into a `Fact` targeting an `EntityPath` (or nothing, if
   unrecognised — most traffic is ignored).
4. `ModelState.RouteFacts` finds/creates the target entity (and parents), delivers the fact
   via `OnFact`, and bumps that entity's **generation**.
5. On the next `RuleRegistry.Check`, each rule queries only entities changed since its last run
   (`ChangedEntities[T]`), then asserts (safety) or records `Pending`/`Resolve` (liveness).
6. At teardown, `CheckNamespace(ctx, namespaceID)` promotes unresolved liveness conditions to
   violations, and `PurgeNamespace` isolates the next test.

## Naming conventions (enforced at registration)

| Concept | `Name()` / `Type()` must equal | Validated by |
|---|---|---|
| Fact | struct name | `EntityRegistry.RegisterFact` |
| Entity | struct name (via `Type()`) | `EntityRegistry.RegisterEntity` |
| Rule | struct name **+ `"Rule"`** | `RuleRegistry.RegisterSafety`/`RegisterLiveness` |

These panics catch copy-paste drift at wire-up rather than letting a mislabeled rule or
fact fail silently at runtime.
