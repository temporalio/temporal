# Umpire — Spec

Property-based test monitoring for Temporal: observe a running server, build an
*executable model* of its entities, and rule on that model — without tests hand-writing
assertions.

## Goals

- **Separate actions from assertions.** Tests drive behavior; Umpire judges it. The two
  are reusable independently (same rules across functional tests, nightly runs, canary).
- **Terse tests.** Replace per-test boilerplate assertions with reusable rules over a model.
- **Tests as living docs.** The model + rulebook describe how a feature behaves.
- **Find bugs earlier.** Cheap enough to run per-PR; a foundation for later fuzzing.
- **Fault injection is first-class.** Faults (latency, drops, errors, early timers) are
  actions the framework must support natively, not a bolt-on — the `FaultInjector` hook is
  built into the interceptor for exactly this. Steering the SUT into rare states is where
  the interesting invariants get exercised.

## Non-goals (for now)

- Driving actions / generating scenarios (the "active" side — Player/Skipper). Tests
  drive. See `PLAYER.md`.
- Fuzzing, coverage-guided exploration. The Scenario/Coverage catalog is specced
  (`UMPIRE_PLAN.md`) but unbuilt.
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
- **Entities are executable models, not just FSMs.** Each entity is a total transition
  function `Classify(event) → Advance | NoOp | Illegal` (the oracle inversion in
  `SAAMODEL.md`). A total model has **no vacuous pass**: an unanticipated state or an
  illegal edge is a diff against the model, caught by one generic conformance rule — not
  something a human had to foresee and hand-write. Rules read; the model judges its own
  transitions.
- **Generation-based dirty tracking.** Each fact delivery bumps a counter; rules only
  re-examine entities changed since their last check. No per-tick history retained.
- **Safety vs. Liveness split** (maps strong vs. eventual consistency):
  - *Safety* — must hold at every observation; violated ⇒ immediate failure.
  - *Liveness* — must eventually hold; tracked as `Pending`/`Resolve`, unresolved items
    become violations at teardown. Both derive from model annotations (terminal states,
    `MustProgress`) where possible.
- **Models plus relational invariants.** A complete model is *per-entity*, but Umpire's
  most valuable invariants are *cross-entity* (speculative task ↔ update, update ↔ its
  workflow's close). So the rulebook splits: single-entity conformance and liveness
  collapse into generic, model-derived checks; genuinely relational invariants stay bespoke
  rules. That cross-entity reach is Umpire's differentiator — SAA's single-archetype model
  has no such story.
- **Observation tiers — black / grey / white box.** Facts carry a provenance *tier*
  (frontend gRPC = black; internal RPC + OTEL = grey; persistence = white). A run enables
  only importers ≤ its tier and skips higher-tier rules **explicitly** — "not observable
  here," never a silent pass. One model, tier-gated: the flagship lifecycle rules run in
  canary/Cloud while white-box rules stay functional-test-only. Portability is the axis
  SAA's white-box model doesn't address.
- **Rulebook is pluggable.** Rules register by name (must match their type); a run may
  select a subset. Adding a rule ≠ touching the framework.
- **Framework / domain split.** `common/testing/umpire` is generic and reusable;
  `tests/umpire` holds all Temporal specifics (entities, facts, rules).

## Shape

```
gRPC + OTEL + (persist)          Decoder      Facts      Registry            Rulebook
  tier-gated fact sources  ────▶  wire→fact ────────▶  entity models  ────▶  generic conformance
                                                       Classify:              + liveness (model-derived)
                                                       Advance/NoOp/Illegal   + cross-entity relational  ──▶ Violations
                                          (FactLog: queryable record of every fact)
```
