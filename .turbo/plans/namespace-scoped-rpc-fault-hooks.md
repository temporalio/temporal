---
status: draft
---

# Plan: Namespace-scoped RPC fault hooks

## Context

The RPC fault helpers must be safe when functional tests run concurrently on a shared cluster. The current global testhook dispatchers invoke a shared callback registry and rely on callback-level namespace checks; caller-supplied options can override those checks and expose another test to the fault.

Replace the global dispatchers with four exact-scope hooks covering request/response faults and namespace ID/name lookup. Keep multi-callback ordering and cleanup inside `RPCFaultGenerator`, because the testhook registry intentionally stores only one value for each key and scope.

## Pattern Survey

### Analogous Features
- `common/testing/testhooks/hooks.go:18` — Namespace-ID-scoped hook keys use `newKey[T, namespace.ID]`; the file explicitly discourages global scope because it requires a dedicated cluster.
- `common/testing/testhooks/hooks.go:32` — `NamespaceReplicationTaskInterceptor` demonstrates a namespace-name-scoped callback key using `newKey[T, namespace.Name]`.
- `service/matching/physical_task_queue_manager.go:724` — Consumers retrieve ID-scoped hooks by converting request or partition namespace data to `namespace.ID` at the use site.
- `common/namespace/nsreplication/replication_task_executor.go:114` — Consumers retrieve name-scoped hooks by extracting the request namespace and converting it to `namespace.Name`.
- `common/testing/testhooks/test_impl_test.go:58` — Existing tests prove that values registered under the same key for different namespace IDs remain isolated and that cleanup affects only its exact scope.
- `tests/testcore/test_env.go:379` — `TestEnv.InjectHook` treats namespace-scoped hooks as safe for shared clusters and automatically binds them to the environment’s namespace ID.
- `tests/xdc/history_replication_signals_and_updates_test.go:130` — Namespace-name-scoped hooks use `TestCluster.InjectHook` with an explicit `namespace.Name` scope.
- `tests/testcore/test_env.go:552` — Namespace-scoped dynamic configuration is another shared-cluster isolation pattern: TestEnv applies its namespace constraint before registering the override.

### Reusable Utilities
- `common/testing/testhooks/test_impl.go:39` — `Get` — retrieves a typed hook by the composite identity of key ID and exact typed scope.
- `common/testing/testhooks/test_impl.go:69` — `NewHook` — packages a typed key/value for type-erased injection while validating the runtime scope type.
- `common/testing/testhooks/test_impl.go:82` — `Set` — stores a hook in the concurrent registry and returns cleanup that deletes the exact key/scope pair; registration for the same key/scope overwrites the previous value.
- `tests/testcore/onebox.go:470` — `injectHook` — applies a hook, registers its cleanup with `testing.T.Cleanup`, and returns the same cleanup function.
- `common/rpc/interceptor/namespace.go:12` — `NamespaceNameGetter` — established request interface for extracting `GetNamespace()` without depending on concrete RPC message types.
- `common/rpc/interceptor/namespace.go:16` — `NamespaceIDGetter` — established request interface for extracting `GetNamespaceId()` without depending on concrete RPC message types.
- `common/rpc/faultinjection/grpc.go:31` — `RPCFaultGenerator` — thread-safe multi-callback registry with request/response separation, first-match behavior, snapshot iteration, and idempotent per-callback cleanup.
- `tests/testcore/test_env.go:366` — `Namespace` and `NamespaceID` — expose both namespace representations owned by each TestEnv.

### Convention Anchors
- Typed scope is part of hook identity: `Key[T, S]` carries `namespace.ID`, `namespace.Name`, or global scope at compile time, while the registry keys values by both key ID and scope (`common/testing/testhooks/hooks.go:52`, `common/testing/testhooks/test_impl.go:17`).
- Namespace extraction occurs at the consumer boundary: hook callers convert request-derived strings to `namespace.ID` or `namespace.Name` immediately before `testhooks.Get` (`service/matching/pri_forwarder.go:63`, `common/namespace/nsreplication/replication_task_executor.go:114`).
- Shared-cluster effects are namespace constrained: TestEnv permits namespace-scoped hooks and overrides while rejecting global effects unless the test owns a dedicated cluster (`tests/testcore/test_env.go:379`, `tests/testcore/test_env.go:552`).
- Name-scoped injection is explicit: the generic TestEnv hook path supplies `namespace.ID`; existing `namespace.Name` hooks use `TestCluster.InjectHook` with an explicit name scope (`tests/testcore/test_env.go:383`, `tests/xdc/history_replication_signals_and_updates_test.go:143`).
- Hook lifecycle is test-owned: registration returns cleanup and testcore attaches it to `testing.T.Cleanup` (`common/testing/testhooks/test_impl.go:81`, `tests/testcore/onebox.go:470`).
- Scoped registry entries are single-valued: registering the same key and scope overwrites the prior value, as explicitly tested (`common/testing/testhooks/test_impl_test.go:94`); multi-callback behavior currently lives in `RPCFaultGenerator`.
- Interceptor tests live beside the interceptor, use the `test_dep` build tag, construct `TestHooks` directly, register exact scoped values, run in parallel, and assert with `require` (`common/rpc/faultinjection/interceptor_testdep_test.go:1`).
- Production and test interceptor behavior are split by complementary build-tagged files (`common/rpc/faultinjection/interceptor_testdep.go:1`, `common/rpc/faultinjection/interceptor_noop.go:1`).

### Proposed Alignment
Follow the existing typed `namespace.ID`/`namespace.Name` hook-key and request-getter patterns, with callback lookup remaining at the interceptor boundary. Preserve the existing cleanup ownership and account for the registry’s single-value-per-key/scope semantics where multiple fault callbacks for one namespace remain supported.

## Implementation Steps

1. **Pin namespace routing at the interceptor boundary**
   - Update `common/rpc/faultinjection/interceptor_testdep_test.go` first with failing cases for request and response hooks scoped by namespace ID and name, mismatched namespaces, handler errors with nil responses, requests without namespaces, and ID precedence when both representations exist.
   - Replace the two global keys in `common/testing/testhooks/hooks.go` with symmetric request/response keys scoped by `namespace.ID` and `namespace.Name`.
   - Update `GRPCUnaryServerInterceptor` in `common/rpc/faultinjection/interceptor_testdep.go` to reuse `interceptor.NamespaceIDGetter` and `interceptor.NamespaceNameGetter`, choose one non-empty namespace representation, and retrieve only the matching stage-specific hook.

2. **Make the generator own scoped registration lifecycle**
   - Update `common/rpc/faultinjection/grpc_test.go` first with failing tests for isolated ID/name registrations, multiple callbacks in one scope, first-match ordering, idempotent cleanup, last-registration hook removal, and concurrent generate/unregister behavior.
   - Change `RPCFaultGenerator` in `common/rpc/faultinjection/grpc.go` to receive `testhooks.TestHooks`, maintain callback buckets by stage and exact typed namespace scope, install a testhook when a bucket gains its first callback, and remove it when the last callback unregisters.
   - Keep snapshot iteration so unregistering cannot deadlock an in-flight callback, and keep request and response callback signatures distinct.

3. **Enforce scoped helper APIs**
   - Update `tests/testcore/fault_injection_test.go` first with failing cases showing empty namespace options are rejected and ID/name options are translated into scoped generator registrations without OR-based callback filtering.
   - Remove `matchesNamespace` from `tests/testcore/fault_injection.go`; make raw request/response helpers require at least one non-empty namespace option and pass the selected scopes to `RPCFaultGenerator`.
   - Remove namespace-option parameters from `TestEnv.InjectRPCRequestFault` and `TestEnv.InjectRPCResponseFault` in `tests/testcore/test_env.go`; always register both `e.nsID` and `e.nsName`.
   - Construct `RPCFaultGenerator` with the cluster's `TestHooks` in `tests/testcore/onebox.go` and remove the two global hook installations.

4. **Align callers and documentation**
   - Keep the existing shared-cluster functional test in `tests/update_workflow_test.go` on `env.InjectRPCRequestFault`, and make the touched suite error assertions fatal per project rules.
   - Update `docs/development/testing.md` and the raw helper comments to describe exact namespace scoping, the required raw namespace option, unary-only interception, and the lack of namespace-less/global injection.
   - Preserve existing comments outside the changed behavior and keep request/response naming symmetric.

## Verification

- Run focused red/green tests with `go test -tags test_dep` for `./common/rpc/faultinjection` and `./tests/testcore`; the new routing tests must fail before implementation and pass afterward.
- Run `go test -race -tags test_dep ./common/rpc/faultinjection ./tests/testcore -count=1`; all scoped registration and concurrent cleanup cases must pass without races.
- Run the named sticky-worker-unavailable functional test with `-tags test_dep`; it must pass without the previous sleep and without requiring a dedicated cluster.
- Run production-tag tests for `./common/rpc/faultinjection`; the no-op interceptor must remain nil.
- Run `make fmt-imports`, `git diff --check`, focused changed-line lint, and `make lint-code`; changed code must introduce no formatting or lint findings.

## Context Files

- `common/testing/testhooks/hooks.go` — defines typed hook keys and their scope.
- `common/testing/testhooks/test_impl.go` — defines exact-scope storage and cleanup semantics.
- `common/rpc/interceptor/namespace.go` — provides the existing request namespace getter interfaces.
- `common/rpc/faultinjection/grpc.go` — owns callback ordering, concurrency, and cleanup.
- `common/rpc/faultinjection/interceptor_testdep.go` — performs request-boundary hook lookup and fault execution.
- `tests/testcore/fault_injection.go` — defines the public raw helper contract and cleanup behavior.
- `tests/testcore/test_env.go` — owns each functional test's namespace identity and safe shared-cluster API.
