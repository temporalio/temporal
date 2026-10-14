---
name: review
description: Review code for common issues. Use when reviewing PRs, suggesting code changes, or when user asks to review code.
allowed-tools: Read, Grep, Glob
---

# Code Review Skill

The main question of a review is whether the PR changes product behavior in the way it intends, and
whether it changes behavior in any way it does not intend. Code quality comes second. Where the code
guidelines at the end of this file disagree with this procedure about priorities, this procedure
takes precedence.

## 1. State the intended behavior change

Read the PR title and description. From them, state the intended behavior change: who sees what
difference, and under which conditions. If the description says that the change mirrors, extends or
replaces an existing feature, that feature is a reference implementation for step 3. If the
description does not state the intended behavior, infer it from the diff, and say in the summary
that the description should state it.

## 2. Read the change in full

Read the diff. Then read every changed file completely, together with the code that calls the
changed code and the code it calls.

## 3. Find the reference implementations

Find the existing code that implements the most similar behavior: the feature that the description
names, the same feature for another execution type, and the code path that the change adds to or
bypasses. Read it.

## 4. Compare behavior along each dimension

For each dimension below, determine how the reference behaves and how the PR behaves. Each
difference is a finding unless the PR explains why it is intended. An intended difference is still a
finding if its consequence is harmful.

- Gating: the dynamic configs and request fields that enable the behavior, their defaults, and the
  behavior under the defaults. Whether the switches that operators use to turn off the reference
  feature also turn off the new behavior.
- Admission and fallback: when the server declines the new behavior, and what it does instead.
- Retries and idempotency: what the client receives when it retries a request with the same request
  ID, when the request conflicts with an existing execution, and when the response is lost. Whether
  the client can still make progress, and how long it waits.
- Failure recovery: what recovers the execution after a worker crash or a lost response, and which
  timeout and retry configurations prevent that recovery.
- Interaction with other server features: rate limits, backlog, priority and fairness in Matching,
  worker versioning, pause, cancellation, callbacks, and visibility. Whether the PR respects each of
  them in the same way as the reference.
- Resource accounting: anything acquired or consumed, such as a rate-limit token, a slot or a lock,
  before a step that may not take place.
- Observability: the metrics, logs and persisted fields that the reference emits, including for
  denials and failures.
- API surface: fields that the reference API has and the new API lacks, and whether adding them later
  would require a breaking change or SDK changes.

## 5. Check consequences outside the diff

Behavior can be wrong because of code that the PR does not change, for example a dynamic config
default, a check that exists only on another code path, or a caller that relies on a field being
set. Follow each new code path back to the source of its inputs and forward to the consumers of its
outputs.

## 6. Do not treat a TODO or a test as justification

A `TODO` records a gap; it does not make the gap acceptable. Report the gap if its consequence
matters once the change is released. A test that asserts a behavior shows that the author intended
the behavior, not that the behavior is correct.

## 7. Review tests and code

Identify intended behavior that no test covers, and tests that would still pass if the corresponding
implementation were reverted. Then review the code against the guidelines below.

## Reporting

Report behavior findings before code findings. For each behavior finding, state the condition, what
happens, what the reference does, and why it matters. A behavior difference that can strand an
execution, lose work, or bypass a server protection such as a rate limit is at least `med`.

@.github/copilot-instructions.md
