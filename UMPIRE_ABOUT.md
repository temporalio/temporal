# Umpire — Architecture

How the pieces fit together. For *why* it exists read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md);
for current status, gaps, and the rule inventory read [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).

Umpire watches a running Temporal server and rules on invariants. It never drives
behaviour — tests do that. Umpire only **observes** (gRPC traffic + OTEL spans),
**models** what it sees (entity state machines), and **judges** the model (rules that
emit violations).

```
   observe                     model                        judge
┌────────────┐   Facts   ┌──────────────────┐         ┌──────────────┐
│  Decoder   │ ────────▶ │     Registry     │ ──────▶ │   Rulebook   │ ──▶ Violations
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
| `tests/umpire/` | **Domain** — Temporal specifics | workflows, updates, task queues, gRPC/span shapes |

The framework never imports the domain. Adding a rule or entity is a domain change; the
framework stays put.

---

## Framework types (`common/testing/umpire/`)

### Addressing — who a fact is about

- **`EntityType`** — a string tag for a kind of entity (`"Workflow"`, `"WorkflowUpdate"`, …).
- **`EntityID`** = `{Type, ID}` — one entity of a type.
- **`EntityPath`** = `{EntityID, ParentID *EntityID}` — an `EntityID` optionally qualified
  by its parent. This is what a fact *targets*. `EntityPathKey(path)` serialises it to the
  canonical registry key (`type:id` or `type:id@parentType:parentID`).

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

### `Registry` — routing + dirty tracking (`registry.go`)

The heart of the model layer. It:

1. **Stores** entities keyed by `EntityPathKey`, each wrapped in an `entityRecord`
   `{entity, generation}`.
2. **Routes** — `RouteFacts([]Fact)` groups facts by target, lazily creates the target
   entity (and its parent chain) via the registered factory, and hands each entity its
   batch through `OnFact`. `BroadcastFact`s go to all entities of a type (nil path).
3. **Tracks change by generation** — a single atomic counter. Every time an entity
   receives facts, its record is stamped with the next generation value. This is how rules
   avoid re-examining everything: `QueryEntities(type, sinceGeneration)` returns only
   entities stamped *after* a watermark.

Registration is validated at wire-up time:
- `RegisterEntity(factory, …)` panics unless `entity.Type() == structName`.
- `RegisterFact(probes…)` panics unless `fact.Name() == structName`.

### `Rulebook` — the judges (`rulebook.go`)

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
for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
    wu := r.Entity   // *entity.WorkflowUpdate, only if changed since last check
    ...
}
```

`Rulebook` responsibilities:
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
- **`interceptor.go`** — a gRPC unary interceptor built from two optional hooks:
  `FactRecorder.RecordFact` (observe requests), `ResponseRecorder.RecordResponse` (observe
  responses), and `FaultInjector.Inject` (the dormant "active" hook — no logic yet).
- **`instrument.go`** — helpers for *producing* observations from inside the server:
  `Instrument`/`RecordFact` emit OTEL spans/events under `TracerName`, `EntityTag` stamps a
  span with the entity it concerns.
- **`Flag`** (`flag.go`) — a named boolean an entity FSM sets/clears on transitions
  (`Admitted`, `Accepted`, …); a small observable that rules and debugging can read.

---

## Domain layer (`tests/umpire/`)

### `Umpire` — the orchestrator (`umpire.go`)

Wires the framework to Temporal and is the object tests hold. It owns a `Registry`, a
`FactDecoder`, a `Rulebook` (with all default rules registered), and a `FactLog`.

It plugs into the server two ways:
- **OTEL** — implements `sdktrace.SpanProcessor`. `OnEnd(span)` decodes span events →
  `RouteFacts`. Synchronous (no batch delay), so per-PR cost stays low.
- **gRPC** — implements `FactRecorder`/`ResponseRecorder`. `RecordFact`/`RecordResponse`
  decode the request/response → append to `FactLog` → `RouteFacts`.

Tests call `Check(ctx, final…)` to collect violations, and at teardown `settleWorkflows`
broadcasts a `WorkflowTerminated` for every seen workflow so child FSMs reach terminal
states before the final liveness sweep.

### `FactDecoder` — wire/span → `Fact` (`entity/fact_decoder.go`)

The single place that understands Temporal wire formats. It holds registered importers and
tries each:
- **`RequestFact.ImportRequest(any)`** — type-asserts a gRPC request into a fact.
- **`ResponseRecorder` path** — turns a request+response pair into a fact (e.g. a poll that
  actually returned a task).
- **`SpanFact.ImportSpanEvent(attrs)`** — builds a fact from an OTEL span event whose name
  matches the fact's `Name()`.

### Entities — Temporal FSMs (`entity/`)

`Workflow`, `WorkflowTask`, `WorkflowUpdate`, `TaskQueue` (with `Namespace` also defined).
Each implements `Entity`, backs its state with a `looplab/fsm` machine, and exposes `Flag`s
and timestamps that rules read. Example: `WorkflowUpdate` transitions
`unspecified → admitted → accepted → completed` (or `rejected`/`aborted`), setting the
matching `Flag` and `…At` timestamp on each step. `register.go`'s `RegisterDefaultEntities`
wires the default entities and the fact types, declaring which facts each entity subscribes
to.

### Facts and rules

- **`fact/`** — one struct per observable thing, implementing `SpanFact` or `RequestFact`.
  Its `Name()` equals the struct name (and, for span facts, the OTEL event name).
- **`rule/`** — 14 rules (5 safety, 9 liveness). Each reads entity state via
  `ChangedEntities[T]` and emits `Violation`s. See `UMPIRE_PLAN.md` for the full inventory.
- **`entity_key.go`** — a small fluent builder (`Workflow(id).Update(id)` / `.Task(…)`)
  that produces the same registry key strings the router uses, so tests can name the exact
  entity a rule should have passed (`RequireRulePassed`).

---

## End-to-end: the life of one observation

1. The server handles a gRPC call or emits an instrumented OTEL span.
2. The interceptor / `SpanProcessor` hands it to `Umpire`.
3. `FactDecoder` turns it into a `Fact` targeting an `EntityPath` (or nothing, if
   unrecognised — most traffic is ignored).
4. `Registry.RouteFacts` finds/creates the target entity (and parents), delivers the fact
   via `OnFact`, and bumps that entity's **generation**.
5. On the next `Rulebook.Check`, each rule queries only entities changed since its last run
   (`ChangedEntities[T]`), then asserts (safety) or records `Pending`/`Resolve` (liveness).
6. At teardown, `Check(ctx, true)` promotes any unresolved liveness conditions to
   violations. The test fails on any violation.

## Naming conventions (enforced at registration)

| Concept | `Name()` / `Type()` must equal | Validated by |
|---|---|---|
| Fact | struct name | `Registry.RegisterFact` |
| Entity | struct name (via `Type()`) | `Registry.RegisterEntity` |
| Rule | struct name **+ `"Rule"`** | `Rulebook.RegisterSafety`/`RegisterLiveness` |

These panics catch copy-paste drift at wire-up rather than letting a mislabeled rule or
fact fail silently at runtime.
