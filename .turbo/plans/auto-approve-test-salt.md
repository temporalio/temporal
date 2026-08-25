---
status: done
---

# Plan: Auto-approve test shard salt updates

## Context

The scheduled test-sharding optimizer already creates a single-file pull request and enables squash auto-merge. The pull request remains blocked because the generated salt file inherits CODEOWNERS and Temporal has no guarded auto-approval workflow for this CICD change on `main`.

## Pattern Survey

### Analogous Features

- `.github/workflows/auto-approve-cicd-release-pr.yml:1` — Existing Temporal auto-approval uses a `pull_request` trigger, write access to pull requests, an actor gate, and `gh pr review --approve`; it is limited to `cloud/**`.
- `.github/workflows/optimize-test-sharding.yml:13` — The salt producer centralizes the exact salt path and branch name, commits only that path, creates the exact-title PR, and enables squash auto-merge.
- `../saas-temporal/.github/workflows/auto-approve-cicd-bot-pr.yml:1` — The working SaaS analogue approves CICD-bot PRs to `main` by recognized title, including its test-salt update.
- `../saas-temporal/.github/workflows/dependabot-go-mod-tidy.yml:21` — A privileged bot workflow validates actor, PR author, same-repository head, expected branch family, and changed paths.
- `../saas-temporal/.github/CODEOWNERS:36` — Generated CI-managed files use exact ownerless entries placed after broader ownership rules.

### Reusable Utilities

- `.github/workflows/optimize-test-sharding.yml:12` — `SALT_FILE` and `BRANCH` are the canonical values for the generated file and automation branch.
- `.github/workflows/auto-approve-cicd-release-pr.yml:15` — The repository already uses the runner-provided `gh` CLI and standard Actions token for approval.
- No project-local helper encapsulates changed-file enumeration or the complete approval predicate.

### Convention Anchors

- PR automation lives under `.github/workflows/` and declares explicit permissions.
- Privileged workflows read immutable pull-request metadata from the event payload and reject unexpected changed paths.
- More-specific ownerless CODEOWNERS entries follow broad ownership rules so they override them.

### Proposed Alignment

Combine Temporal's existing approval mechanism with the stronger same-repository and exact-file validation used by SaaS bot workflows, and follow SaaS's late ownerless CODEOWNERS override.

## Implementation Steps

1. **Exempt the generated salt from CODEOWNERS**
   - Add an explanatory comment and exact ownerless `/tests/testcore/shard_salt.txt` entry to `.github/CODEOWNERS` after the broad ownership rules.
2. **Add guarded approval automation**
   - Add `.github/workflows/auto-approve-test-shard-salt.yml` for pull requests to `main` that touch the salt path.
   - Gate approval on the CICD author, same-repository source, canonical branch, and exact title from `.github/workflows/optimize-test-sharding.yml`.
   - Query pull-request files through `gh`, fail closed unless the salt is the sole changed file, then approve with the standard Actions token.

## Verification

- Run `make lint-actions`; expect actionlint to accept all workflows.
- Run `git diff --check`; expect no whitespace errors.
- Statically inspect the workflow predicate and file query; expect every approved design guard and only `tests/testcore/shard_salt.txt` in the allowlist.
- Compare the ownerless rule with the SaaS generated-file convention; expect it to appear after Temporal's wildcard rule.

## Context Files

- `.github/workflows/optimize-test-sharding.yml` — canonical branch, title, file, and existing auto-merge behavior.
- `.github/workflows/auto-approve-cicd-release-pr.yml` — Temporal approval token and command convention.
- `.github/CODEOWNERS` — ownership ordering and exact Nexus mappings.
- `../saas-temporal/.github/workflows/dependabot-go-mod-tidy.yml` — fail-closed privileged bot validation.
