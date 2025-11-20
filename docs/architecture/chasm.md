# Terminology

- The Temporal server implements abstractions such as `Workflow`, `Activity`, and `Scheduler`.

- Each of these abstractions is an `Archetype`.

- An instance of an `Archetype` is an `Execution` (e.g. a workflow execution, an activity execution).

- An execution possesses a `BusinessID` that should typically be meaningful in the user’s own systems. It is guaranteed to be unique among executions within a namespace that are in a non-terminal state.

- A single `Execution` may consist of multiple non-overlapping runs (e.g. successive non-overlapping invocations of an `Activity` or `Workflow` that share the same `businessID`, successive non-overlapping invocations of a `Workflow` by a `Scheduler`).

- An `Execution` has a tree structure in which each subtree is a `Component`. A component, and thus an execution, may be a tree comprising one node only.

- A `ComponentRef` contains an `ExecutionKey`, a `ComponentPath`, and additional information identifying (uniquely across all clusters when there is a multi-cluster configuration with failovers) a specific transition in execution history. Together these identify a component within an execution.