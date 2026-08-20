# Test Environment Log Capture Design

## Context

The Nexus observability functional test needs to inspect a server log after the operation has completed. `testlogger.Expect` is registered before a log is emitted and records only match counts, so the current test pauses the Nexus handler until it learns the request ID and can register an expectation. Metrics already have an opt-in capture model that supports post-hoc inspection.

A Nexus test environment can own two namespaces: its primary `Namespace()` and the inherited `ExternalNamespace()`. Namespace-scoped log capture must account for both.

## Scope

Add opt-in log recording to `common/testing/testlogger`, expose an environment-scoped wrapper from `tests/testcore`, and migrate the Nexus observability test to inspect the captured log after the failed operation. Preserve existing `testlogger` expectation and failure behavior.

The first milestone records only logs owned by the environment. A namespace mentioned solely as a Nexus endpoint target does not establish ownership.

## API and Behavior

`TestLogger` will support starting an independent capture and taking a snapshot of its records. A record contains its level, message, and a defensive copy of its tags. Recording has no cost beyond checking for active captures when no capture is running. Loggers returned by `With` share capture state with their parent logger.

`TestEnv.StartLogCapture()` will start a capture on the environment's cluster logger and stop it automatically during test cleanup. It will retain a log when an ownership tag identifies either:

- `env.Namespace()` by name or `env.NamespaceID()` by ID; or
- `env.ExternalNamespace()` by name.

The test infrastructure does not currently retain the external namespace ID, so external-namespace ownership is matched by name in this milestone. The `nexus-endpoint-target-namespace-id` tag is deliberately not an ownership tag and will not make a record eligible by itself.

The returned capture exposes a snapshot rather than Nexus-specific query methods. Individual tests remain responsible for selecting the expected message and checking the exact tags relevant to their contract. This keeps the recorder generic and avoids recreating `newNexusLogRecorder` in the Nexus test.

The inherited external namespace can be shared by multiple test environments using the same underlying cluster. The ownership filter therefore reduces noise but does not claim complete per-test isolation. Correlation tags such as the Nexus request ID provide the final selection criterion.

## Nexus Test Migration

The Nexus observability test will start environment log capture before executing the workflow. The handler will report the request it received and immediately return the intentional failure; it will no longer pause while the test registers a logger expectation.

After the workflow fails, the test will find the outbound failure record in the capture and verify:

- the log carries the primary environment namespace; and
- its Nexus request ID equals the request ID observed by the handler.

The existing namespace metric capture and outbound request/latency assertions remain unchanged.

## Concurrency and Lifecycle

Active captures are registered in `TestLogger`'s shared state. Each capture owns synchronization for its records so logging can append concurrently while the test takes snapshots. Stopping a capture removes it from the active set; snapshots taken afterward remain valid. Snapshot results and tag slices are copied so callers cannot mutate recorder state.

## Tests

Unit tests for the recorder and environment wrapper will cover:

- no records are retained unless capture is explicitly started;
- `With` loggers write to the same active capture;
- primary namespace name and ID ownership are included;
- external namespace name ownership is included;
- target-only namespace tags and unrelated namespaces are excluded;
- snapshots and their tags are defensive copies; and
- stopped captures receive no additional records.

The Nexus observability suite will continue running for both HSM and CHASM operation implementations and will verify the log/handler request-ID correlation plus the namespace-scoped outbound metrics.

## Out of Scope

- A Nexus-specific log recorder or query language.
- Always-on recording for all functional tests.
- Treating target-only namespace tags as ownership.
- Adding storage for the external namespace ID solely for this milestone.
- Broad log-fidelity assertions beyond the Nexus outbound failure contract.
