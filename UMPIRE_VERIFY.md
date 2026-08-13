# Umpire generated verification models: remaining work

- [ ] Complete verification coverage of the canonical protocol.
  - Migrate `SpeculativeTaskCreation`, `NexusOperationClosure`,
    `NexusOperationTimeoutSemantics`, and `WorkflowTaskStarvation` into the shared property algebra.
  - Add refinements for regression semantics still marked as outside the initial bounded slice, or
    classify them as permanent exclusions.

- [ ] Enforce unsupported semantics consistently.
  - Preserve progress and fairness semantics in each backend or emit an explicit unsupported result.
  - Return `unsupported` when a backend cannot provide the requested guarantee.
  - Prevent smoke and nightly runs from reporting success for an unsupported semantic subset.

- [ ] Complete normalized counterexample evidence.
  - Recover action bindings from every backend trace.
  - Populate state and relation deltas for every normalized step.
  - Validate normalized traces by replaying them through the Go interpreter when supported.

- [ ] Complete limit classification.
  - Detect depth, state, step, memory, schedule, timeout, interruption, and tool limits.
  - Ensure every reached limit produces `inconclusive`, never a success status.

- [ ] Expand backend equivalence and mutation testing.
  - Compare tiny reachable worlds with TLA+, P, Ivy, and Apalache results.
  - Seed missing guards, missing frame clauses, reversed cardinality, omitted choices, identity reuse,
    weakened properties, and incorrect refinements.
  - Require every supporting backend to expose each mutation.

- [ ] Make tool pins a single source of truth.
  - Derive CI installation and reported runner versions from the manifest's versions and checksums.
  - Fail verification when workflow, runner, and manifest tool metadata drift.
