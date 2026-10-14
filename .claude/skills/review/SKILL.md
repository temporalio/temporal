---
name: review
description: Review code for common issues. Use when reviewing PRs, suggesting code changes, or when user asks to review code.
allowed-tools: Read, Grep, Glob
---

# Code Review Skill

The main question of a review is whether the PR changes Temporal's behavior in the way it intends,
and whether it changes Temporal's behavior in any way it does not intend. Behavior includes internal
behavior that users do not observe directly, such as persisted state, tasks, and RPCs between
services. Code quality comes second. Where the code guidelines at the end of this file disagree with
this procedure about priorities, this procedure takes precedence.

## 1. State the intended change

Read the PR title and description, and state the change to Temporal's behavior that they intend. If
they do not say what the PR is intended to change and why, report that as the only finding: ask the
author to describe the intended change in the PR description and then request another review. Do
not review further.

## 2. Read the change in full

Read the diff. Then read every changed file completely, together with the code that calls the
changed code and the code it calls.

## 3. Find the reference behavior

Find the existing code that does the most similar thing: code that the description says the change
mirrors or replaces, code that does the same thing for another execution type or service, and code
paths that the change adds to or bypasses. Read it. The change should either behave like this code
or differ from it for a reason.

## 4. Compare the change with the reference

For each condition the reference checks, each configuration it reads, each side effect it performs
and each metric it emits, determine whether the change does the same. If the change bypasses a code
path, determine what that code path guaranteed and whether the change still guarantees it. Each
difference is a finding unless the PR explains why it is intended. An intended difference is still a
finding if its consequence is harmful.

## 5. Determine the behavior of each new code path

For each new code path, determine what happens under the default configuration, when a request is
retried or delivered twice, when it runs concurrently with other operations on the same state, and
when it fails or its response is lost after each step. Determine whether the server and its callers
can still make progress in each case.

## 6. Check consequences outside the diff

Behavior can be wrong because of code that the PR does not change, for example a configuration
default, a check that exists only on another code path, or a caller that relies on a field being
set. Follow each new code path back to the source of its inputs and forward to the consumers of its
outputs.

## 7. Do not treat a TODO or a test as justification

A `TODO` records a gap; it does not make the gap acceptable. Report the gap if its consequence
matters once the change is released. A test that asserts a behavior shows that the author intended
the behavior, not that the behavior is correct.

## 8. Review tests and code

Identify intended behavior that no test covers, and tests that would still pass if the corresponding
implementation were reverted. Then review the code against the guidelines below.

## Reporting

Report behavior findings before code findings. For each behavior finding, state the condition, what
happens, what the reference does, and why it matters. A behavior difference that can lose data,
leave an execution unable to make progress, or remove a guarantee that the server provides elsewhere
is at least `med`.

@.github/copilot-instructions.md
