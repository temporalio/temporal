# Fx Lifecycle Logging Design

## Goal

Reduce server and functional-test startup cost by removing high-volume,
success-path Fx lifecycle debug logs while preserving actionable lifecycle
failure diagnostics.

## Behavior

The Temporal Fx event adapter will ignore successful construction, invocation,
start, stop, and logger-initialization events. It will continue to log:

- all Fx event errors with their existing function, module, caller, and stack
  context;
- rollback and rollback failures;
- process shutdown signals; and
- unknown Fx event types.

This changes only diagnostic output. Fx graph construction and lifecycle hook
execution are unchanged.

## Verification

Unit tests will establish that a representative success-path lifecycle event
does not emit a debug log while its failure event still emits an error. The
existing Temporal package tests and repository formatting and lint checks will
also run.

The performance comparison will use identical repeated cluster-boot workloads
with debug logging enabled on the parent commit and the changed commit. The PR
will report wall time and allocation results and will not include a benchmark
harness unless that harness is independently useful.

## Scope

The masked server-configuration debug log is independent and remains unchanged.
No logger interfaces, test log defaults, Fx graph topology, or server lifecycle
behavior will change.
