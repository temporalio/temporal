# Namespace-scoped RPC fault hooks

## Goal

Allow functional tests to inject unary gRPC request and response faults while tests run concurrently on a shared cluster. A fault registered by one `TestEnv` must be invisible to every other namespace.

## Design

Define four testhook keys, covering the cross-product of fault stage and namespace representation:

- request faults scoped by `namespace.ID`
- request faults scoped by `namespace.Name`
- response faults scoped by `namespace.ID`
- response faults scoped by `namespace.Name`

The interceptor extracts a non-empty namespace ID from the request when available; otherwise it extracts a non-empty namespace name. It looks up only the corresponding stage-and-scope hook. Requests without either namespace representation are not eligible for these faults.

`RPCFaultGenerator` owns the callback lists and the lifecycle of the four scoped hooks. Registering the first callback for a stage and namespace alias installs the corresponding testhook; unregistering the last callback removes it. This preserves first-match-wins behavior and allows multiple independently removable faults in one namespace.

`TestEnv.InjectRPCRequestFault` and `TestEnv.InjectRPCResponseFault` register each fault under both the environment's namespace ID and name. They do not accept namespace overrides. Raw `testcore` helpers require at least one explicit namespace option, so they cannot silently create a cluster-global fault.

Production builds continue selecting the `!test_dep` no-op interceptor, leaving the production unary interceptor chain unchanged.

## Failure handling

- A raw registration without a namespace fails immediately.
- Empty namespace values are rejected rather than treated as wildcards.
- A request exposing both representations uses namespace ID precedence, so one callback is not invoked twice.
- Cleanup is idempotent and cannot remove a later or still-active registration for the same scope.

## Tests

- Prove ID-scoped and name-scoped request hooks cannot observe another namespace.
- Prove the same isolation for response hooks, including a nil response with a handler error.
- Prove ID precedence for a request exposing both representations.
- Prove multiple callbacks in one scope retain first-match and independent cleanup behavior.
- Prove raw helpers reject missing namespace scope.
- Retain the shared-cluster functional test that replaces the sticky-worker sleep with a scoped request fault.
- Verify the production no-op build and run focused tests with `-tags test_dep` and `-race`.

## Trade-offs

Four hooks duplicate a small amount of request/response and ID/name wiring, but make namespace isolation part of the registry lookup instead of a callback convention. Namespace-less fault injection is intentionally excluded because it cannot be isolated on a shared cluster; a future global API would require a dedicated cluster and a separate explicit contract.
