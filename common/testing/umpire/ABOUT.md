# About Umpire

Umpire is a model-based acceptance testing framework. It describes Temporal behavior in terms of
what should happen, drives a running system toward that behavior, observes what actually happens,
and judges the result against shared expectations.

The central idea is to keep behavioral intent separate from mechanics. A test should say that a
Workflow reaches an expected outcome, not repeat every RPC, worker setup step, polling loop, and
assertion needed to get it there.

## The one-minute model

Umpire has three cooperating paths:

1. The driving path turns a **SCENARIO** into a **PLAN**, then uses **ACTIONS** and **REALIZERS** to
   interact with Temporal.
2. The observation path turns responses, history, telemetry, and in-process signals into **FACTS**
   about **ENTITIES**. Those facts update **LIFECYCLES**, **RELATIONS**, and the **MODEL STATE**.
3. The judgment path applies **RULES** to that state and produces a **QUALIFIED CLAIM**.

In short:

**SCENARIO** → **PLAN** → **ACTION** → **REALIZER** → Temporal

Temporal → observations → **FACT** → **MODEL STATE** → **RULE** → **QUALIFIED CLAIM**

Driving and observation are deliberately separate. The code that changes Temporal does not get to
decide whether Temporal behaved correctly; judgment comes from independently collected evidence.

## Ubiquitous language

These terms form Umpire's shared conceptual language. Together, they connect authoring, execution,
regression, exploration, and verification.

- **PROTOCOL** — the compiled behavioral vocabulary shared by the rest of Umpire. It declares the
  known **ENTITIES**, **FACTS**, **LIFECYCLES**, **RELATIONS**, **ACTIONS**, and explicitly
  unsupported behavior.
- **ENTITY** — a thing whose behavior Umpire tracks, such as a Workflow Execution, Activity, Nexus
  Operation, or Callback. An **ENTITY** has a stable semantic type and a concrete identity learned
  before or during execution.
- **FACT** — a normalized observation about an **ENTITY**. Different evidence sources can describe
  the same behavior as the same **FACT**, so **RULES** do not need to understand RPC or telemetry
  formats.
- **LIFECYCLE** — the allowed states of an **ENTITY** and the events that move it between them. An
  observed event may advance the **LIFECYCLE**, be an allowed no-op, or be illegal.
- **RELATION** — a typed connection between **ENTITIES**, such as lineage or ownership. It lets
  Umpire reason across components without guessing identities from timing or names.
- **MODEL STATE** — Umpire's current understanding of an execution, built only from observed
  **FACTS**, tracked **ENTITIES**, and their **RELATIONS**.
- **SCENARIO** — one bounded semantic experiment: the behavior to exercise, its inputs, and its
  expected outcome. It describes intent rather than environment-specific setup.
- **ACTION** — a semantic operation with observable preconditions and effects. An **ACTION** says
  what behavior should be caused, not which RPC or worker implementation causes it.
- **PLAN** — an inspectable, bounded route through one or more **LIFECYCLES**. It orders the
  **ACTIONS** needed to reach the **SCENARIO**'s goal before execution begins.
- **REALIZER** — the environment-specific adapter that turns an **ACTION** into actual Temporal
  traffic, worker behavior, or controlled faults.
- **RULE** — a behavioral expectation evaluated against the **MODEL STATE**. A **SAFETY RULE**
  must hold at each checkpoint; a **LIVENESS RULE** records an obligation that must be resolved by
  the final bounded check.
- **VIOLATION** — evidence that a **RULE** did not hold. It identifies the failed behavioral
  expectation and retains a semantic, non-secret diagnosis.
- **ENVIRONMENT PROFILE** — the contract for where a **SCENARIO** runs, what it may drive, what it
  can observe, and which ordering guarantees its evidence supports.
- **QUALIFIED CLAIM** — the strongest conclusion justified by the available evidence. A claim is
  established, violated, unsupported, or inconclusive; missing evidence is never treated as
  success.
- **COVERAGE** — a comparison between declared behavioral obligations and what execution observed.
  It shows what was exercised, not whether the behavior is universally correct.
- **SPARSE REGRESSION** — a durable test written as a few semantic key frames. Umpire fills in the
  valid intermediate plan and rejects the regression before execution if required behavior cannot
  be realized.
- **CAMPAIGN** — a bounded discovery loop that selects **SCENARIOS**, executes them in isolation,
  minimizes qualified failures, replays the reduced experiment, and proposes stable candidates for
  human-reviewed **SPARSE REGRESSIONS**.
- **GENERATED VERIFICATION** — a bounded formal projection of the **PROTOCOL** used to check its
  abstract state space independently of a running Temporal system.
- **GUARDED CANARY** — an approved **SCENARIO** executed under explicit isolation, authority,
  traffic, fault, evidence, and cleanup limits.

## A conceptual example

Suppose a test needs to establish that a Workflow completes after its worker returns successfully.

The **SCENARIO** states that goal using vocabulary from the **PROTOCOL**. Umpire follows the
Workflow's **LIFECYCLE** to produce a **PLAN**. Each **ACTION** in that plan is handed to a
**REALIZER**, which performs the appropriate client or worker behavior in the selected
**ENVIRONMENT PROFILE**.

At the same time, Umpire observes Temporal. It normalizes relevant responses, history events,
telemetry, and in-process signals into **FACTS**. Those **FACTS** identify the Workflow **ENTITY**,
advance its **LIFECYCLE**, and update any **RELATIONS** to other **ENTITIES**. **RULES** then
inspect the resulting **MODEL STATE**.

If the required evidence is complete, the result can become an established or violated
**QUALIFIED CLAIM**. If the environment could not observe something the **RULE** requires, the
claim is unsupported or inconclusive instead of silently passing.

## Understanding an existing Umpire test

Read an existing test from intent toward evidence:

1. Start with its **SCENARIO** or **SPARSE REGRESSION**. What behavioral outcome is it asking for?
2. Identify the relevant **ENTITIES**, **LIFECYCLES**, and **RELATIONS** in the **PROTOCOL**.
3. Follow the **PLAN** and **ACTIONS** conceptually. What must Umpire cause, and what should each
   **ACTION** change?
4. Find the **FACTS** that make those changes observable and the **RULES** that judge them.
5. Check the **ENVIRONMENT PROFILE** before interpreting the **QUALIFIED CLAIM**. A black-box run
   and an in-process run may justify different conclusions from the same behavioral intent.

This reading order keeps setup details from obscuring the reason the test exists.

## Authoring an Umpire test

Author from the shared language outward:

1. State the desired behavior as a bounded **SCENARIO** or **SPARSE REGRESSION**.
2. Reuse the existing **PROTOCOL** vocabulary whenever it already expresses the behavior.
3. Make every required outcome observable as a **FACT** before relying on it in a **RULE**.
4. Keep the **ACTION** semantic and put environment mechanics in its **REALIZER**.
5. Declare explicit bounds, capabilities, and evidence needs through the **PLAN** and
   **ENVIRONMENT PROFILE**.
6. Exercise the expected path and meaningful failure modes. Treat unsupported behavior and
   incomplete evidence as explicit outcomes.
7. Read the resulting **QUALIFIED CLAIM**, retained evidence, and **COVERAGE** together.

Extend the **PROTOCOL** only when its current language cannot describe the behavior. A new concept
is useful when it can be observed, driven where appropriate, and judged without embedding one
test's mechanics into the shared model.

## Conceptual structure

Umpire is roughly organized into six conceptual responsibilities:

- **LANGUAGE** — the **PROTOCOL** gives every other responsibility the same behavioral vocabulary.
- **PLANNING** — **SCENARIOS** and **SPARSE REGRESSIONS** become bounded, validated **PLANS**.
- **DRIVING** — using the selected **ENVIRONMENT PROFILE**, **REALIZERS** perform **ACTIONS**.
- **OBSERVATION** — raw signals become **FACTS**, which build the **MODEL STATE**.
- **JUDGMENT** — **RULES** turn evidence into **VIOLATIONS** and **QUALIFIED CLAIMS**.
- **ASSURANCE** — replay, **COVERAGE**, **CAMPAIGNS**, **GENERATED VERIFICATION**, and
  **GUARDED CANARIES** broaden the search while preserving explicit bounds and authority.

These responsibilities share a language but remain separate seams. That separation lets a test
reuse its behavioral intent across local tests, CI, deployment validation, and approved canaries
without pretending those environments offer identical control or evidence.

## What Umpire does not promise

- A bounded successful run is not exhaustive proof.
- **COVERAGE** proves that an obligation was exercised, not that its model is correct.
- Replaying a **SCENARIO** preserves its semantic experiment and evidence; it cannot reproduce an
  uncontrolled distributed schedule exactly.
- A **QUALIFIED CLAIM** cannot be stronger than its **ENVIRONMENT PROFILE** and observed evidence.
- Production credentials, operator approval, and destructive authority remain outside the generic
  framework.
- Exact wire compatibility, authorization, performance, schema, metrics, and low-level concurrency
  contracts still need specialized tests when they sit below the **PROTOCOL** abstraction.

For detailed architecture and implementation boundaries, see
[`UMPIRE.md`](../../../UMPIRE.md).
