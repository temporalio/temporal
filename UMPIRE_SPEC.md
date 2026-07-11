# Umpire — Spec

Property-based test monitoring for Temporal: observe a running server, model its
entities, and rule on invariants — without tests hand-writing assertions.

## Goals

- **Separate actions from assertions.** Tests drive behavior; Umpire judges it. The two
  are reusable independently (same rules across functional tests, nightly runs, canary).
- **Terse tests.** Replace per-test boilerplate assertions with reusable rules over a model.
- **Tests as living docs.** The model + rulebook describe how a feature behaves.
- **Find bugs earlier.** Cheap enough to run per-PR; a foundation for later fuzzing.

## Non-goals (for now)

- Driving actions / generating scenarios (the "active" side — Pitcher/Skipper). Tests drive.
- Fault injection, fuzzing, coverage-guided exploration. Hooks exist; logic does not.
- Persistence. State is in-memory and per-test.

## Constraints

- **Observe-only.** Umpire never changes SUT behavior; it reads gRPC traffic and OTEL spans.
- **Rules stay dumb.** A rule queries entity state; it knows nothing of wire formats,
  change tracking, or how facts arrive.
- **No SDK requirement** to describe behavior — facts come from the wire/spans, not test code.
- **Cheap.** Must run per-PR: synchronous span processing, no external services.

## Design decisions

- **Facts, not calls.** Everything observed (requests, responses, span events, history
  events) is normalized into a `Fact` targeting one entity. One decoder owns wire→fact.
- **Entities are FSMs.** A `Registry` routes facts to per-entity state machines
  (Workflow → Task/Update, TaskQueue, Namespace). Entities interpret; rules read.
- **Generation-based dirty tracking.** Each fact delivery bumps a counter; rules only
  re-examine entities changed since their last check. No per-tick history retained.
- **Safety vs. Liveness split** (maps strong vs. eventual consistency):
  - *Safety* — must hold at every observation; violated ⇒ immediate failure.
  - *Liveness* — must eventually hold; tracked as `Pending`/`Resolve`, unresolved items
    become violations at teardown.
- **Rulebook is pluggable.** Rules register by name (must match their type); a run may
  select a subset. Adding a rule ≠ touching the framework.
- **Framework / domain split.** `common/testing/umpire` is generic and reusable;
  `tests/umpire` holds all Temporal specifics (entities, facts, rules).

## Shape

```
gRPC + OTEL ──▶ Decoder ──▶ Facts ──▶ Registry(entity FSMs) ──▶ Rulebook ──▶ Violations
                                          (FactLog: queryable record of all facts)
```
