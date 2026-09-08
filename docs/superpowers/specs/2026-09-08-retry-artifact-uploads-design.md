# Retry Post-Test Artifact Uploads

## Problem

The test matrix uploads several artifacts per job. GitHub's artifact service can intermittently return `403 Forbidden` while finalizing an upload, and `actions/upload-artifact` does not retry that response. A single failed upload currently fails an otherwise successful test job.

## Design

Add a composite action that wraps `actions/upload-artifact` with three total attempts and short backoff delays. Retries use the same artifact name with `overwrite: true`, preserving the artifact naming contract and preventing duplicate reports.

Use the wrapper for artifacts produced by post-test reporting. JUnit XML and test-summary JSON remain required after retries. Debug logs, cluster events, and memory diagnostics become best-effort after retries because losing optional diagnostics must not fail a successful test job.

## Verification

Run the repository's GitHub Actions and YAML linters. Review the expanded workflow conditions to confirm that retries run only after a failed attempt and that the final required attempt propagates failure.
