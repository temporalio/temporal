# Auto-approve test shard salt updates

## Context

The scheduled `Optimize Test Sharding` workflow already opens a pull request and enables auto-merge. Unlike the equivalent SaaS workflow, pull requests to update `tests/testcore/shard_salt.txt` still need a manual CODEOWNER approval because Temporal has no `main`-branch auto-approval workflow for this generated change.

## Design

Add an ownerless `tests/testcore/shard_salt.txt` entry to `.github/CODEOWNERS`, matching the established SaaS treatment for generated salt files.

Add a dedicated pull-request workflow for `main`. It may approve a pull request only when all of these conditions hold:

- The pull request author is `temporal-cicd[bot]`.
- The head repository is `temporalio/temporal`.
- The head branch is `auto/optimize-test-sharding`.
- The title is exactly `Update test shard salt`.
- The only changed file is `tests/testcore/shard_salt.txt`.

The workflow uses the standard Actions token with read-only contents access and pull-request write access. It does not check out or execute pull-request code.

The existing optimizer remains responsible for enabling squash auto-merge after creating the pull request.

## Verification

Validate the workflow syntax and statically verify every approval guard, the exact changed-file check, and the ownerless CODEOWNERS rule. No permanent test is added for these configuration-only changes.

## Out of scope

Deployment Owners rules are managed in `temporalio/cicd-terraform` and are handled separately.
