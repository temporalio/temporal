# Umpire ubiquitous language review

## Scope

This review covers the ubiquitous language and the surrounding conceptual flows in
[`ABOUT.md`](./common/testing/umpire/ABOUT.md). The terms were checked against the generic Umpire
framework, the Temporal protocol, sparse regressions, campaigns, generated verification, and
canary execution.

The recommendations below concern conceptual language. They do not imply that matching Go
identifiers should be renamed in the same change.

## Overall assessment

The foundation is understandable. **ENTITY**, **FACT**, **LIFECYCLE**, **RELATION**, **ACTION**,
**ENVIRONMENT PROFILE**, and **CAMPAIGN** are useful domain terms. The separation between intent
and mechanics, and between evidence and justified conclusions, is also the right teaching model.

The main problem is that the glossary presents one flat vocabulary while several terms mean
different things at different stages. In particular, **PLAN**, **SCENARIO**, **RULE**,
**MODEL STATE**, and unsupported behavior currently collapse distinctions a novice needs. The
glossary also introduces advanced assurance modes at the same level as the core execution loop.

## Corrected conceptual flows

The current one-minute flow implies that every **SCENARIO** produces one **PLAN**, every **PLAN** is
an ordered list of **ACTIONS**, and every **RULE** directly produces a **QUALIFIED CLAIM**. Those
relationships are not universally true.

A more accurate authoring and execution flow is:

```text
TEST INTENT
  -> SPARSE REGRESSION PLAN
  -> COMPILED SUITE
  -> COMPLETED PATH
  -> ACTION
  -> ACTION REALIZER
  -> Temporal
```

A **CAMPAIGN SCENARIO** is assembled later from a **COMPLETED PATH** plus selected matrix values,
an exploration route, and faults. It is not the universal source form for every Umpire test.

Observation and judgment should be shown as separate flows:

```text
RAW OBSERVATION -> FACT -> MODEL STATE
                       \-> RELATION STATE

MODEL STATE + RELATION STATE -> RULE -> PASS | PENDING OBLIGATION | VIOLATION

PROPERTY RESULT + OBSERVED EVIDENCE + ENVIRONMENT PROFILE -> QUALIFIED CLAIM
```

This preserves the load-bearing distinction between what Umpire observed, how it interpreted those
observations, what failed, and how strong a conclusion the evidence permits.

## Priority findings

### 1. Split **PLAN** into the artifacts it currently conflates

**PLAN** is the most misleading definition. The framework has at least three distinct concepts:

- a lifecycle planner produces one or more event routes to a target state;
- a sparse regression plan is author-written source containing selected instructions; and
- regression compilation produces suites and completed paths containing validated actions,
  resources, policies, milestones, and bindings.

The current definition—an ordered set of actions through lifecycles—does not accurately describe
all three.

Recommended terms:

- **LIFECYCLE ROUTE PLAN** — one or more lifecycle-event routes to a target state;
- **SPARSE REGRESSION PLAN** — the author-written behavioral source;
- **COMPILED SUITE** — all validated paths produced from a sparse regression plan; and
- **COMPLETED PATH** — one fully grounded path ready for realization.

Use bare **PLAN** only when the specific plan kind is already unambiguous.

### 2. Separate **PROPERTY**, **RULE**, **VIOLATION**, and **QUALIFIED CLAIM**

The current glossary says a **RULE** is the expectation, a **VIOLATION** is evidence, and a rule
produces a **QUALIFIED CLAIM**. This collapses the behavioral requirement, its runtime evaluator,
the resulting judgment, and the evidence boundary.

Recommended definitions:

- **PROPERTY** — a named proposition about behavior whose result can be evaluated and
  evidence-qualified;
- **RULE** — a runtime evaluator for one or more properties using **MODEL STATE** and
  **RELATION STATE**;
- **VIOLATION** — a judgment that an invariant failed, an obligation remained unresolved at the
  final bounded check, or observed behavior was illegal; and
- **QUALIFIED CLAIM** — the conclusion about a **PROPERTY**, limited by the declared environment
  and evidence actually retained.

A **VIOLATION** is derived from evidence; it is not itself the raw evidence. Formal verification
can also produce a claim about a **PROPERTY** without running a live **RULE**.

The four claim statuses need explicit distinctions:

- **ESTABLISHED** — complete evidence supports the property within the declared bounds;
- **VIOLATED** — complete evidence contradicts the property;
- **UNSUPPORTED** — the declared environment cannot meet the property's evidence requirements;
  and
- **INCONCLUSIVE** — the environment could support the property, but evidence was lost, missing,
  ambiguous, conflicting, or incomparable during this evaluation.

Keep **QUALIFIED CLAIM** because it is established architecture and code vocabulary. In
explanatory prose, “evidence-qualified claim” is more immediately understandable than bare
“qualified claim.”

### 3. Narrow **SCENARIO** to its actual role

The glossary presents **SCENARIO** as the universal authoring input and assigns it an expected
outcome. The campaign concept is narrower: it is an environment-independent executable experiment
assembled from a completed path and selected variations. Expected behavior belongs to a
**PROPERTY** or author-selected milestone, not to the campaign scenario itself.

Recommended terms:

- **TEST INTENT** — the generic behavior an author wants to exercise and judge; and
- **CAMPAIGN SCENARIO** — one selected executable experiment containing a completed path, matrix
  choices, an optional exploration route, and faults.

Use **SCENARIO** alone only when the campaign context is already clear.

### 4. Keep **MODEL STATE** separate from relations and retained facts

The current definition says **MODEL STATE** contains facts, entities, and relations. Conceptually
and structurally, these are separate:

- **MODEL STATE** tracks entity instances and their fact-derived state;
- **RELATION STATE** tracks validated cross-entity links; and
- **OBSERVED EVIDENCE** retains the facts and other evidence used during evaluation.

Rules may consult model and relation state together, but that does not make relations part of
**MODEL STATE**. Keeping the distinction visible makes cross-entity rules and relation failures
easier to understand.

### 5. Stop using “unsupported” for unrelated conditions

“Unsupported” currently risks meaning all of the following:

- a lifecycle edge has no atomic action realization;
- an environment cannot supply evidence required by a property;
- evidence was expected but incomplete; or
- a formal backend cannot express a semantic feature.

These conditions require different remediation. Add **ACTION GAP** for a known lifecycle edge that
deliberately has no atomic realization, and reserve **INCONCLUSIVE** for evidence loss or ambiguity.
When discussing formal verification, say **UNSUPPORTED BACKEND SEMANTICS** rather than bare
“unsupported behavior.”

Recommended definition:

> **ACTION GAP** — a lifecycle transition Umpire understands but deliberately cannot drive as one
> atomic action. The protocol records the reason instead of silently treating the edge as
> executable.

### 6. Add the missing observation and evidence terms

The distinction between observation and proof is central to Umpire, but **EVIDENCE** is not defined
and **FACT** can sound like established truth.

Add:

- **RAW OBSERVATION** — a signal received from Temporal, such as a response, history event,
  telemetry event, or in-process notification;
- **FACT** — one raw observation translated into Umpire's shared vocabulary and addressed to an
  entity path or entity type; a fact records what Umpire observed and does not prove a property by
  itself;
- **MONITOR** — the runtime boundary that accepts facts, updates model and relation state, and
  evaluates rules without driving Temporal;
- **EVIDENCE REQUIREMENT** — the sources, ordering, and identity guarantees needed to establish or
  violate a property; and
- **OBSERVED EVIDENCE** — the sources and guarantees actually retained for one evaluation,
  including loss, ambiguity, conflict, and incomparable ordering.

Also define **CHECKPOINT** as a named moment when the Monitor evaluates its safety rulebook, such as
after an action, after an observation milestone, or at quiescence.

Do not imply that observations from different sources are deduplicated into the same **FACT**.
Distinct facts may describe the same lifecycle transition and must remain individually visible.

### 7. Clarify **PROTOCOL** and add **ACTION GAP**

“Protocol” is easily mistaken for an RPC or wire protocol, while “compiled behavioral
vocabulary” is implementation-oriented and circularly references several undefined terms.

Keep the established term but introduce it as **UMPIRE PROTOCOL** on first use:

> **UMPIRE PROTOCOL** — the validated behavioral contract Umpire understands. It catalogs fact and
> entity types, entity subscriptions, lifecycles, action bindings and action gaps, relation schemas
> and derivation, sparse-regression vocabulary, and expected causal footprints.

Rules and environment evidence are separate from the protocol. An **ACTION GAP** also does not mean
that the corresponding Temporal behavior or property is globally unsupported.

### 8. Split the flat glossary by learning stage

Nineteen equally weighted terms are too many for a novice's first pass. Group the glossary in the
order concepts become necessary:

1. Authoring: **TEST INTENT**, **UMPIRE PROTOCOL**, **PROPERTY**.
2. Driving: **ACTION**, **ACTION REALIZER**, **LIFECYCLE ROUTE PLAN**.
3. Observation: **ENTITY**, **ENTITY PATH**, **RAW OBSERVATION**, **FACT**, **MONITOR**,
   **LIFECYCLE**, **RELATION**, **MODEL STATE**, **RELATION STATE**.
4. Judgment: **PROPERTY**, **RULE**, **CHECKPOINT**, **VIOLATION**, **EVIDENCE REQUIREMENT**,
   **OBSERVED EVIDENCE**, **ENVIRONMENT PROFILE**, **QUALIFIED CLAIM**.
5. Advanced assurance: **SPARSE REGRESSION PLAN**, **MILESTONE**, **COMPLETED PATH**,
   **SEMANTIC COVERAGE**, **CAMPAIGN SCENARIO**, **EXPLORATION CAMPAIGN**,
   **BOUNDED MODEL VERIFICATION**, **VERIFICATION TARGET**, **SAFETY ENVELOPE**,
   **GUARDED CANARY**.

The first three groups explain how an ordinary test works. The final group should be clearly
optional on a first read.

### 9. Replace or define repeated jargon

“Semantic” appears repeatedly as “semantic type,” “semantic operation,”
“semantic diagnosis,” and “semantic key frame.” Replace it with a concrete phrase such as
“behavior-level,”
“protocol-defined,” or “behavioral milestone.” Define the intent/mechanics distinction once
rather than making readers infer a different meaning in every entry.

“Bounded” is load-bearing but undefined. State once that a **BOUND** is an explicit finite limit
on time, actions, routes, schedules, depth, retained evidence, or another search dimension.
Reaching a bound must not be described as success unless the resulting claim remains justified.

### 10. Tighten specific established terms

These terms should remain, with narrower definitions:

- **ENTITY** — use **WORKFLOW** and **WORKFLOW RUN** as separate examples. Umpire models the
  logical Workflow-ID chain separately from a RunID-specific execution. Add **ENTITY PATH** for
  containment ancestry; do not confuse it with a cross-entity **RELATION**.
- **LIFECYCLE** — say that it is optional for an entity type. It classifies an event as an
  advance, including a reachable forward jump over unobserved states, a benign re-observation, or
  an illegal transition.
- **ACTION** — keep the “what, not how” definition, but use **ACTION REALIZER** in prose. A
  realizer may install reactive behavior as well as fire a proactive operation.
- **ENVIRONMENT PROFILE** — define the execution kind, drive capabilities, available evidence
  sources, clock and ordering guarantees, identity lineage, supported properties, and retention.
  Keep it distinct from **OBSERVED EVIDENCE**.
- **COVERAGE** — prefer **SEMANTIC COVERAGE** in novice-facing prose so it cannot be mistaken for
  line coverage. It records observed facts, transitions, relations, actions, rule evaluations, and
  violations; it does not prove correctness.
- **SPARSE REGRESSION** — keep the established name, but define **MILESTONE** instead of using the
  unexplained “semantic key frame” metaphor. Compilation may produce one or many completed
  paths, not one intermediate plan.
- **CAMPAIGN** — prefer **EXPLORATION CAMPAIGN** on first use. It is a budgeted batch of isolated
  scenarios used to discover, minimize, replay, and propose regression candidates.
- **GENERATED VERIFICATION** — prefer **BOUNDED MODEL VERIFICATION**. Add **VERIFICATION TARGET**
  for one selected slice with explicit properties, bounds, abstractions, backend requirements,
  and failure policy. The model combines protocol-derived data with explicit verification
  modules; it is not a direct projection of the protocol alone.
- **GUARDED CANARY** — keep the name, but define **SAFETY ENVELOPE**. A guarded canary runs an
  approved allowlisted workload under isolation, authority, budget, retention, stop, and cleanup
  controls. It does not directly execute a campaign scenario or necessarily produce a qualified
  claim.

## Terms that are already strong

The following ideas should be preserved through any rewrite:

- **ENTITY** as the thing Umpire follows;
- **FACT** as the normalized observation boundary;
- **LIFECYCLE** as executable behavioral state;
- **RELATION** as an explicit alternative to identity inference;
- **ACTION** as intent separated from environment mechanics;
- **ENVIRONMENT PROFILE** as the declared capability and evidence contract;
- **QUALIFIED CLAIM** as a conclusion limited by evidence;
- **COVERAGE** as an exercised-behavior signal rather than proof; and
- **CAMPAIGN** as the bounded discovery-to-regression loop.

The running example should then demonstrate these terms with concrete names rather than repeat the
glossary abstractly: one Workflow and Workflow Run, one action, the facts observed, one lifecycle
transition, one relation, one property, and the resulting evidence-qualified claim.

## Recommended change order

1. Correct the **PLAN**, **SCENARIO**, **MODEL STATE**, **RULE**, **VIOLATION**, and
   **GUARDED CANARY** definitions and the one-minute flow.
2. Add **PROPERTY**, **ACTION GAP**, **RAW OBSERVATION**, **MONITOR**, **CHECKPOINT**,
   **OBSERVED EVIDENCE**, **MILESTONE**, **COMPLETED PATH**, and **SAFETY ENVELOPE**.
3. Group the glossary by learning stage and move advanced assurance terms out of the core path.
4. Replace repeated “semantic” jargon and define “bounded.”
5. Rewrite the conceptual example as one concrete end-to-end story.
