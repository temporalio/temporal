# Always Start the Worker Service in Functional Tests

## Goal

Remove the `WithWorkerService` opt-in and start the system worker service for every functional-test cluster. The draft PR's CI run will provide the runtime and memory data needed to evaluate the impact.

## Design

Make worker startup an invariant of `testcore` clusters instead of a per-test option:

- Remove `WithWorkerService` and all call sites.
- Remove the worker-specific fields and option plumbing from test environments, cluster requests, and cluster configuration.
- Construct every shared, dedicated, and suite-scoped cluster with the worker service enabled.
- Stop treating worker usage alone as a reason to create a fresh dedicated cluster. Tests without other cluster-global requirements will use the normal shared or pooled cluster path.
- Keep cluster-creation telemetry accurate by reporting that the worker is enabled for every created cluster.

This intentionally measures the complete steady-state behavior, including any benefit from reusing worker-enabled clusters instead of creating clusters solely for worker-dependent tests.

## Failure Handling

Worker startup continues through the existing test-cluster startup path, so startup failures fail cluster setup as they do for currently opted-in tests. No new runtime error path is introduced.

## Testing

- Update the default functional-test health-check coverage so it proves the worker service starts without an opt-in.
- Add or update focused cluster-routing tests to prove worker-specific configuration no longer forces a fresh cluster.
- Run the relevant `testcore` tests with `-tags test_dep`.
- Run formatting and `make lint-code` before creating the draft PR.

## Trade-offs

- Runtime and memory may increase because every functional-test cluster hosts the worker service; measuring that impact is the purpose of the draft PR.
- Worker-dependent tests can reuse shared or pooled clusters when they have no other isolation requirement, reducing cluster churn but changing their prior isolation behavior.
- Removing the option and its plumbing is simpler than retaining a compatibility no-op and prevents future tests from assuming worker startup is conditional.
- The change does not alter production service configuration or introduce new security behavior.

At substantially higher test concurrency, each concurrently active cluster carries worker-service overhead, while cluster reuse limits the number of clusters created. Existing cluster-pool limits continue to bound concurrency.
