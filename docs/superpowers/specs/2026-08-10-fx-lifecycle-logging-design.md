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

The performance comparison used the same core-cluster boot benchmark before and
after the change, with debug logging enabled, 20 samples, and five boots per
sample. Median boot time fell from 25.02 ms to 19.50 ms (22.1%), allocated bytes
fell 8.8%, and allocated objects fell 6.2%. The existing benchmark harness is
part of the isolated-cluster work and is not included in this change. Its
deliberately immediate teardown was stabilized outside the timed region so the
same workload could run reliably at both speeds.

## Scope

The masked server-configuration debug log is independent and remains unchanged.
No logger interfaces, test log defaults, Fx graph topology, or server lifecycle
behavior will change.
