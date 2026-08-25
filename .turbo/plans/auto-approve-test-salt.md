---
status: done
---

# Plan: Auto-approve test shard salt updates

## Context

The scheduled test-sharding optimizer already creates a single-file pull request and enables squash auto-merge. The pull request remains blocked because the generated salt file inherits CODEOWNERS and the optimizer does not approve the CICD change.

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

Use Temporal's existing approval mechanism directly in the trusted scheduled optimizer, and follow SaaS's late ownerless CODEOWNERS override.

## Implementation Steps

1. **Exempt the generated salt from CODEOWNERS**
   - Add an explanatory comment and exact ownerless `/tests/testcore/shard_salt.txt` entry to `.github/CODEOWNERS` after the broad ownership rules.
2. **Approve the generated pull request**
   - Capture the pull-request URL in `.github/workflows/optimize-test-sharding.yml` after the trusted scheduled job creates it.
   - Approve with the standard Actions token in a following step while retaining the CICD App token for pull-request creation and auto-merge.
   - Skip approval when the optimizer detects no salt change and therefore creates no pull request.

## Verification

- Run `make lint-actions`; expect actionlint to accept all workflows.
- Run `git diff --check`; expect no whitespace errors.
- Statically inspect the workflow token boundaries; expect the CICD App token to create the pull request and enable auto-merge, and the standard Actions token to approve it.
- Compare the ownerless rule with the SaaS generated-file convention; expect it to appear after Temporal's wildcard rule.

## Context Files

- `.github/workflows/optimize-test-sharding.yml` — canonical branch, title, file, and existing auto-merge behavior.
- `.github/workflows/auto-approve-cicd-release-pr.yml` — Temporal approval token and command convention.
- `.github/CODEOWNERS` — ownership ordering and exact Nexus mappings.
- `../saas-temporal/.github/workflows/dependabot-go-mod-tidy.yml` — fail-closed privileged bot validation.
