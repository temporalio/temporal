# About Umpire

Umpire is a model-based acceptance testing framework. It describes Temporal behavior in terms of
what should happen, drives a running system toward that behavior, observes what actually happens,
and judges the result against shared properties.

The central idea is to keep behavioral intent separate from execution mechanics. A test should say
that a Workflow reaches an expected outcome, not repeat every RPC, worker setup step, polling loop,
and assertion needed to get it there.

A **BOUND** is an explicit finite limit on time, actions, routes, schedules, search depth, retained
evidence, or another execution or search dimension. Reaching a bound is not success unless the
available evidence still justifies the resulting claim.

## The one-minute model

Authoring and driving follow this flow:

```text
TEST INTENT
  -> SPARSE REGRESSION PLAN
  -> COMPILED SUITE
  -> COMPLETED PATH
  -> ACTION
  -> ACTION REALIZER
  -> Temporal
```

A lifecycle route plan is another, narrower planning artifact: it contains one or more event routes
to a target state. It does not select a concrete runtime entity. A campaign scenario is assembled
later from a completed path plus selected matrix values, an exploration route, and faults; it is not
the universal source form for every Umpire test.

Observation and judgment are separate flows:

```text
RAW OBSERVATION -> FACT -> MODEL STATE
                       \-> RELATION STATE

MODEL STATE + RELATION STATE -> RULE -> PASS | PENDING OBLIGATION | VIOLATION

PROPERTY RESULT + OBSERVED EVIDENCE + ENVIRONMENT PROFILE -> QUALIFIED CLAIM
```

The code that changes Temporal does not decide whether Temporal behaved correctly. The Monitor
updates state from independently collected observations, rules evaluate that state at checkpoints,
and evidence qualification limits the conclusion to what the environment actually retained.

## Ubiquitous language

The terms are grouped in the order they become useful. The advanced assurance vocabulary is not
required to understand an ordinary Umpire test.

### Authoring

- **TEST INTENT** — the behavior an author wants to exercise and judge, independent of a particular
  environment or orchestration mechanism.
- **UMPIRE PROTOCOL** — the validated behavioral contract Umpire understands. It catalogs fact and
  entity types, subscriptions, lifecycles, action bindings and action gaps, relation schemas and
  derivation, sparse-regression vocabulary, and expected causal footprints. It is not a wire or RPC
  protocol. Rules and environment evidence remain separate from it.
- **PROPERTY** — a named proposition about behavior whose result can be evaluated and
  evidence-qualified. A property is the requirement; a rule is one way to evaluate it at runtime.

### Driving

- **ACTION** — a protocol-defined operation with observable preconditions and effects. It says what
  behavior should be caused, not which RPC, worker, participant, or fault implementation causes it.
- **ACTION REALIZER** — the environment-specific adapter that fires proactive operations or installs
  reactive behavior for an action.
- **LIFECYCLE ROUTE PLAN** — one or more lifecycle-event routes to a structural target state. It is
  inspectable before execution and does not identify a concrete runtime entity.
- **ACTION GAP** — a lifecycle transition Umpire understands but deliberately cannot drive as one
  atomic action. The Umpire protocol records the reason instead of silently treating the edge as
  executable. It does not mean the Temporal behavior or its properties are globally unsupported.

### Observation

- **ENTITY** — a thing whose behavior Umpire follows, such as a Workflow, Workflow Run, Activity,
  Nexus Operation, or Callback. A logical Workflow-ID chain and a RunID-specific Workflow Run are
  separate entities.
- **ENTITY PATH** — the containment ancestry used to address an entity in a scope. It is not a
  cross-entity relation.
- **RAW OBSERVATION** — a signal received from Temporal, such as a response, history event,
  telemetry event, or in-process notification.
- **FACT** — one raw observation translated into Umpire's shared vocabulary and addressed to an
  entity path or entity type. A fact records what Umpire observed; it does not prove a property by
  itself. Distinct sources may emit distinct facts about the same transition and remain visible.
- **MONITOR** — the runtime boundary that accepts facts, updates model and relation state, and
  evaluates rules without driving Temporal.
- **LIFECYCLE** — the optional state machine for an entity type. It classifies an observed event as
  an advance, including a reachable forward jump over unobserved states, a benign re-observation,
  or an illegal transition.
- **RELATION** — a typed, validated connection between entities, such as Workflow-to-Run membership,
  lineage, or ownership. It lets Umpire reason across components without guessing identity from
  timing or names.
- **MODEL STATE** — Umpire's current fact-derived state for entity instances in one scope.
- **RELATION STATE** — the separately validated cross-entity links in that scope. Rules may consult
  model and relation state together, but relations are not part of model state.

### Judgment

- **RULE** — a runtime evaluator for one or more properties using model state and relation state. A
  safety rule is checked at each applicable checkpoint; a liveness rule may retain an obligation
  until the final bounded check.
- **CHECKPOINT** — a named moment when the Monitor evaluates its safety rulebook, such as after an
  action, after an observation milestone, or at quiescence.
- **VIOLATION** — a judgment that an invariant failed, an obligation remained unresolved at the
  final bounded check, or observed behavior was illegal. It is derived from evidence; it is not the
  raw evidence itself.
- **EVIDENCE REQUIREMENT** — the sources, ordering guarantees, and identity lineage needed to
  establish or violate a property.
- **OBSERVED EVIDENCE** — the sources and guarantees actually retained for one evaluation, including
  any loss, ambiguity, conflict, or incomparable ordering.
- **ENVIRONMENT PROFILE** — the declared execution kind, drive capabilities, evidence sources,
  clock and ordering guarantees, identity lineage, supported properties, and retention policy. It
  says what an environment can provide, not what one run successfully retained.
- **QUALIFIED CLAIM** — the conclusion about a property, limited by the environment profile and
  observed evidence. Its statuses are:
  - **ESTABLISHED** — complete evidence supports the property within the declared bounds;
  - **VIOLATED** — complete evidence contradicts the property;
  - **UNSUPPORTED** — the declared environment cannot meet the property's evidence requirements;
  - **INCONCLUSIVE** — the environment could support the property, but evidence was lost, missing,
    ambiguous, conflicting, or incomparable during this evaluation.

### Advanced assurance

- **SPARSE REGRESSION PLAN** — author-written behavioral source containing selected instructions.
  It states important outcomes, actions, policies, and order without spelling out every valid step.
- **MILESTONE** — an author-selected fact, relation, binding, or state observation that must hold at
  a particular interval in a sparse regression.
- **COMPILED SUITE** — all validated completed paths produced from one sparse regression plan for a
  selected environment profile.
- **COMPLETED PATH** — one fully grounded sequence of actions, resources, policies, milestones, and
  bindings ready for realization.
- **SEMANTIC COVERAGE** — a comparison between declared behavior-level obligations and observed
  facts, transitions, relations, actions, rule evaluations, and violations. It shows what was
  exercised; it does not prove correctness.
- **CAMPAIGN SCENARIO** — one selected executable experiment containing a completed path, matrix
  choices, an optional exploration route, and faults. Expected behavior belongs to a property or
  selected milestone, not to the scenario value itself.
- **EXPLORATION CAMPAIGN** — a budgeted batch of isolated campaign scenarios used to discover,
  minimize, replay, and propose human-reviewed regression candidates.
- **VERIFICATION TARGET** — one selected formal-model slice with explicit properties, bounds,
  abstractions, backend requirements, and failure policy.
- **BOUNDED MODEL VERIFICATION** — generation and checking of finite abstract state spaces from
  protocol-derived data plus explicit verification modules. A backend that cannot express a
  feature reports **UNSUPPORTED BACKEND SEMANTICS**; that is distinct from an unsupported live
  evidence claim.
- **SAFETY ENVELOPE** — the isolation, authority, traffic, fault, time, retention, stop, and cleanup
  controls approved for a canary workload.
- **GUARDED CANARY** — an allowlisted workload run through a context-compliant driver inside a
  safety envelope. It does not directly execute a campaign scenario and does not necessarily
  produce an evidence-qualified claim.

Use bare **PLAN**, **SCENARIO**, **COVERAGE**, or **PROTOCOL** only when the surrounding context makes
the specific kind unambiguous.

## A concrete end-to-end story

Suppose a test intends to establish that Workflow `order-17` has a Run that completes after its
worker returns successfully.

1. The Umpire protocol models two entities: the logical Workflow `order-17` and the Run identified
   by its server-minted RunID. The property is that the selected Workflow Run reaches `completed`
   without a rule violation.
2. A sparse regression plan names the required completed state. Compilation produces a suite and a
   completed path. That path includes an action whose realizer starts the Workflow and arranges the
   successful worker result.
3. Temporal emits several raw observations. A start response and history or in-process notification
   become Workflow-started and Workflow-Run-started facts. A later completion observation becomes a
   Workflow-Run-completed fact.
4. The Monitor advances the Workflow entity and the Run entity independently. From the Run-started
   fact, relation derivation adds the `workflow-runs` relation from Workflow `order-17` to the
   concrete RunID.
5. At the quiescence checkpoint, rules inspect model state and relation state. The selected Run is
   `completed`, its Workflow membership is unambiguous, and no safety or liveness violation remains.
6. The in-process environment profile declares the needed evidence source and identity lineage. If
   those observations were retained, the property receives an established evidence-qualified
   claim. A missing source makes it unsupported; lost or ambiguous identity evidence makes it
   inconclusive; an illegal transition or unresolved obligation makes it violated.

The facts do not prove completion merely because they exist. The rule evaluates the property, and
the evidence requirement plus observed evidence determine how strong the final claim may be.

## Understanding an existing Umpire test

Read a test from intent toward evidence:

1. Identify its test intent and the property it wants to judge.
2. Determine whether its source is a sparse regression plan, lifecycle route plan, or explicit
   completed path.
3. Find the relevant entities, lifecycles, action bindings, and action gaps in the Umpire protocol.
4. Follow the actions and their realizers conceptually: what must Umpire cause, and what should each
   action change?
5. Find the raw observations and facts that make those changes visible, then the rules and
   checkpoints that judge them.
6. Read the environment profile, observed evidence, and qualified claim together. A public-API run
   and an in-process run may justify different conclusions from the same test intent.

This order keeps setup details from obscuring why the test exists.

## Authoring an Umpire test

Author from the shared language outward:

1. State a bounded test intent and property. Prefer the typed sparse regression vocabulary for an
   ordinary Temporal behavior test.
2. Reuse the canonical Umpire protocol whenever it already expresses the entities, states, actions,
   and relations involved.
3. Make every required outcome observable as facts or relation state before relying on a rule to
   judge it.
4. Keep actions behavioral and place environment mechanics in action realizers.
5. Select an environment profile whose drive capabilities and evidence requirements match the
   intent. Do not treat an action gap, incomplete evidence, or unsupported backend semantics as the
   same condition.
6. Record phase-specific bounds and omissions. Exercise the expected path and meaningful failure
   modes.
7. Interpret the qualified claim, retained evidence, and semantic coverage together.

Extend the Umpire protocol only when its current language cannot describe the behavior. A useful
new concept can be observed, driven where appropriate, and judged without embedding one test's
mechanics into the shared model.

## Conceptual structure

Umpire has six conceptual responsibilities:

- **LANGUAGE** — the Umpire protocol gives every other responsibility the same behavioral
  vocabulary.
- **PLANNING** — sparse source and structural targets become bounded, validated artifacts with
  distinct meanings.
- **DRIVING** — action realizers use the selected environment's declared authority to perform
  actions.
- **OBSERVATION** — raw signals become facts, model state, and relation state.
- **JUDGMENT** — rules evaluate properties at checkpoints; evidence qualification produces claims.
- **ASSURANCE** — replay, semantic coverage, exploration campaigns, bounded model verification,
  and guarded canaries broaden the search while preserving explicit bounds and authority.

These responsibilities share a language but remain separate seams. That separation lets a test
reuse behavioral intent across local tests, CI, deployment validation, and approved canaries
without pretending those environments offer identical control or evidence.

## What Umpire does not promise

- A bounded successful run is not exhaustive proof.
- Semantic coverage proves that an obligation was exercised, not that its model is correct.
- Replaying a campaign scenario preserves its behavioral experiment and evidence; it cannot
  reproduce an uncontrolled distributed schedule exactly.
- An evidence-qualified claim cannot be stronger than its environment profile and observed
  evidence.
- Production credentials, operator approval, and destructive authority remain outside the generic
  framework.
- Exact wire compatibility, authorization, performance, schema, metrics, and low-level concurrency
  contracts still need specialized tests when they sit below the Umpire protocol abstraction.

For detailed architecture and implementation boundaries, see
[`UMPIRE.md`](../../../UMPIRE.md).
