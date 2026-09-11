# Local-first test plan

Tests use stable identifiers so implementation changes can cite the invariant they cover. Add new
entries rather than overloading an existing test ID.

## Steel-thread feasibility

| ID | Scenario | Required result | Primary repository | Status |
| --- | --- | --- | --- | --- |
| T000 | An upstream-started workflow is copied locally, loops through three full Activities, and reaches a periodic sync | Upstream stays at its 2-event baseline while local reaches 23 events; after the 100 ms tick, upstream has protobuf-identical history, completed status, and the same result; a repeat from that cursor transfers no batches | `temporal-server` | Implemented by `TestLocalFirstSteelThread` |

## Phase 1: acquisition and synchronization

| ID | Scenario | Required result | Primary repository | Status |
| --- | --- | --- | --- | --- |
| T001 | Worker has no `LocalFirstOptions` | No bridge starts; existing polling and history are unchanged | `temporal-sdk-rust` | Implemented by the unchanged default Worker path and existing Worker integration coverage |
| T002 | System or namespace capability is absent | Worker uses the direct upstream path without startup failure | `temporal-sdk-rust` | Implemented by `local_first_capability_fallback` and capability unit coverage |
| T003 | Local poll acquires a pending WFT | Ownership is written to mutable state; no ownership or upstream WFT-started history event appears | `temporal-server` | Implemented by `TestLocalExecutionAcquisition` |
| T004 | Two bridges poll for the same execution | Exactly one receives an ownership token/epoch | `temporal-server` | Ownership CAS and matching consumption implemented; concurrent-poll test planned |
| T005 | Bridge crashes before baseline SQLite commit | Core never receives a task; upstream lease eventually expires | `temporal-server` | Lease expiry implemented; crash-before-handoff test planned |
| T006 | Bridge crashes after baseline commit and before Core poll return | Restart reconstructs and serves the local task without a second acquisition | `temporal-server` | Planned |
| T007 | Workflow schedules several local Activities/WFTs | No upstream execution RPC occurs between acquisition and synchronization | cross-repository | Demonstrated with ownership; complete execution-RPC trace assertion planned |
| T008 | First non-empty `SyncLocalExecution` | Upstream history equals the local delta and can replay normally | `temporal-server` | Implemented for protocol v1 |
| T009 | Sync request is repeated | Same cursor is acknowledged without duplicate events or tasks | `temporal-server` | Implemented for exact-cursor repeat |
| T010 | Sync fails after any history-node append but before the final mutable-state CAS | Supported upstream history reads and the acknowledged cursor remain exactly at the previous completed sync point; appended nodes are unreachable, and retry can commit the complete delta through a fresh copy-on-write branch | `temporal-server` | Implemented by `TestLocalFirstSteelThread` failure injection |
| T011 | Sync contains a cursor gap, regression, or conflicting hash | Request is rejected without mutation | `temporal-server` | Implemented for gap/regression and deterministic sync-ID fingerprint conflicts |
| T012 | A standalone server binary is driven by a raw Core Worker | Core connects only to the file-backed local frontend, completes three normal Activity lifecycles and multiple WFTs while upstream remains at its 2-event baseline, then the timed sync produces the same upstream result and protobuf-identical history | cross-repository | Implemented |
| T013 | History requests an ordered replication resend | The current batch retries for typed `RetryReplication` in both in-process and gRPC forms; unrelated `ABORTED` errors fail instead of looping | `temporal-server` | Implemented |
| T014 | Standalone bridge synchronizes through upstream frontend | Upstream and bridge are separate processes; the bridge has no upstream History listener address, and `SyncLocalExecution` produces the same result and protobuf-identical history | cross-repository | Implemented |
| T015 | Sync request cursor is stale, regressive, gapped, malformed, or repeated | Invalid requests mutate nothing; an exact repeat acknowledges the existing new cursor without duplicating history | `temporal-server` | Implemented for protocol v1 |
| T016 | Enable local-first for an existing local namespace | Acquisition and active-History synchronization work without promoting, recreating, or migrating the upstream namespace; the same path also works for a global namespace and emits its normal replication tasks after commit | `temporal-server` | Implemented by local/global `TestLocalFirstSteelThread` subtests; bridge cluster import support remains an internal prerequisite |
| T017 | Lease expires, bridge crashes, or force revoke races a synchronization | The workflow lock serializes them with sync; the next owner observes either the complete prior sync point or the complete committed delta, never an intermediate cursor | `temporal-server` | Planned |
| T018 | Persistence fails after one or more history-node appends | Mutable-state CAS does not occur, supported normal/Admin reads remain bounded at the old cursor, context is evicted, and retry commits a fresh fork; best-effort trim can remove abandoned branches | `temporal-server` | Implemented by `TestLocalFirstSteelThread` failure injection |
| T019 | A multi-WFT delta contains intermediate Activities/timers that are complete by its final event | Sync discards intermediate generated tasks and persists only tasks required by the final state and retain/release intent | `temporal-server` | Implemented by the three-Activity steel threads, final completion, and focused task-filter unit coverage |

## Phase 2: lease, pause, and takeover

| ID | Scenario | Required result | Primary repository | Status |
| --- | --- | --- | --- | --- |
| T020 | Idle owned execution reaches `sync_interval` | Empty sync renews expiration to upstream time plus `3 * sync_interval` | `temporal-server` | Implemented in acquisition test and standalone demo |
| T021 | Interval sync cannot connect | Bridge stops new task dispatch and timer advancement at the safe boundary | `temporal-server` | Implemented by controlled-loop unit coverage, timer functional coverage, and the Core bridge with injected sync failures |
| T022 | Dynamically configured event, batch, or byte limit is reached offline | Bridge attempts sync early and pauses at a safe boundary if unavailable, without admitting a delta the atomic sync mechanism cannot commit | `temporal-server` | Planned |
| T023 | Connectivity returns before expiration | Current epoch syncs successfully and local execution resumes | cross-repository | Implemented by controlled-loop unit coverage and the Core bridge with injected sync failures |
| T024 | Connectivity returns after expiration | Upstream rejects the stale epoch; bridge invalidates tokens and abandons its tail | cross-repository | Lease-deadline invalidation and typed stale-owner rejection implemented; combined process takeover scenario remains planned |
| T025 | Expiration timer from an older renewal fires | Timer is a no-op because epoch/expiration no longer match | `temporal-server` | Implemented by `TestLocalExecutionAcquisition` |
| T026 | Current expiration timer fires | Epoch increments, ownership clears, retained inputs reapply, and upstream tasks regenerate | `temporal-server` | Ownership/task regeneration implemented; inbox reapplication planned |
| T027 | New Worker resumes after expiry | Workflow replays only synchronized history and makes progress | cross-repository | Reacquisition and baseline verified; post-takeover workflow progress planned |
| T028 | Stale local task completes after takeover | Completion cannot mutate either local authoritative state or upstream history | cross-repository | Local Workflow and Activity token invalidation is functionally covered; combined upstream takeover scenario remains planned |
| T029 | Operator force-revokes expected epoch | Same state transition as expiry occurs immediately | `temporal-server` | Planned |
| T030 | Operator supplies a stale expected epoch | Newer owner is not revoked | `temporal-server` | Planned |

## Phase 3: bridge lifecycle and routing

| ID | Scenario | Required result | Primary repository | Status |
| --- | --- | --- | --- | --- |
| T040 | Valid local-first configuration | Core starts `server start-bridge`, bootstraps it, and sends Worker polls locally | `temporal-sdk-rust` | Implemented by `local_first_core_activation` |
| T041 | In-memory persistence requested | Bridge refuses startup before acquiring work | `temporal-cli` | Implemented by `TestStartRequiresDurableState` and `TestServer_StartBridgeRequiresPersistentStateAndBootstrapToken` |
| T042 | State directory is already in use | Second bridge fails safely without corrupting SQLite | `temporal-sdk-rust` | Implemented by the second-Worker assertion in `local_first_core_activation` and state-store lock tests |
| T043 | Bootstrap token is reused | Request is rejected after the successful one-time bootstrap | `temporal-server` | Implemented by `TestBridgeBootstrapServerBootstrapsOnce` |
| T044 | Process arguments and normal logs are captured | Upstream API keys and TLS private material are absent | cross-repository | Implemented structurally by exact Core command-argument coverage and by CLI lifecycle log capture with a secret bootstrap header; all connection secrets travel only in the authenticated request body |
| T045 | Worker registration changes during startup | Bridge has the final workflow/Activity manifest before acquisition | `temporal-sdk-rust` | Implemented by `TestBridgeRuntimePollsWithFinalConfiguration`, `local_first_core_activation`, and the unregistered-Activity test |
| T046 | Workflow requests an unregistered Activity | Local history syncs and releases; upstream dispatches the Activity | cross-repository | Implemented by `local_first_unregistered_activity_hands_back_upstream` plus eager-dispatch and command-boundary unit coverage |
| T047 | Workflow requests Nexus or a remote external operation | Local history syncs and releases before remote execution | cross-repository | Implemented for external Signal by `local_first_external_signal_hands_back_upstream`; focused command tests cover Nexus and external cancellation, and the boundary observer has direct unit coverage |
| T048 | Interactive agent runs multiple ordinary Activities per user turn | Three canned turns execute nine configurable-duration Activities locally; each operation label is stored in Activity summary metadata; after every turn, input remains disabled until the waiting Workflow Task completes and local/upstream histories converge; two official Temporal UI instances show the same run through the local and upstream frontends; all summaries survive synchronization | cross-repository | Implemented by `local-first-agent-demo` and `./docs/local-first/run-agent-demo.sh --smoke`; both UI pages and raw-history endpoints verified through their same-origin proxy paths |
| T049 | A caller explicitly requests synchronization before the configured interval | Repeated requests coalesce and wake the managed execution's controlled loop; the bridge still pauses at a safe boundary and uses the ordinary atomic `SyncLocalExecution` path; with a one-minute fallback interval, the demo synchronizes its initial wait and all three completed turns promptly | cross-repository | Implemented by `TestBridgeRuntimeRequestSynchronization`, `TestWaitForSynchronizationBoundaryObservesExplicitTrigger`, and the agent smoke path |
| T050 | A legacy Query reaches the acquisition poll before Phase 4 Query relay exists | Bridge completes the Query as unsupported without treating it as an ordinary Workflow Task or stopping local execution; a response RPC failure also leaves the bridge running; the agent smoke path issues Temporal UI's metadata Query before executing and synchronizing all nine Activities | cross-repository | Implemented by `TestBridgeRuntimeRejectsLegacyQueryWithoutStopping`, `TestBridgeRuntimeQueryResponseFailureDoesNotStop`, `TestBridgeRuntimeStillRejectsOrdinaryWorkflowTask`, and the agent smoke path |

## Phase 4: external messages and Queries

| ID | Scenario | Required result | Primary repository |
| --- | --- | --- | --- |
| T060 | Signal targets an owned execution | Upstream inbox persists it; local commit acknowledges it; later sync removes it | `temporal-server` |
| T061 | Signal caller times out after upstream persistence | Signal remains deliverable and retry does not duplicate it | `temporal-server` |
| T062 | Delivered mutation is not yet synchronized and lease expires | Upstream reapplies it exactly once to resumed execution | `temporal-server` |
| T063 | Update waits for accepted | Response occurs only after local acceptance is synchronized upstream | cross-repository |
| T064 | Update waits for completed | Result occurs only after completion is synchronized upstream | cross-repository |
| T065 | Bridge message poll reconnects | Pending messages may redeliver without duplicate history | `temporal-server` |
| T066 | Query targets an owned connected execution | Only the local Worker evaluates it and the result returns upstream | cross-repository |
| T067 | Query targets a disconnected owner | It waits until caller deadline and leaves no durable relay state | `temporal-server` |
| T068 | Lease expires while Query is in flight | Old result is discarded; caller may retry against the new owner | `temporal-server` |

## Phase 5: children and run transitions

| ID | Scenario | Required result | Primary repository |
| --- | --- | --- | --- |
| T080 | First local WFT starts a child | Completion metadata is found through lookahead before child command matching | `temporal-sdk-rust` |
| T081 | Local execution starts multiple children with the same logical ID on different bridges | Physical IDs do not collide | `temporal-sdk-rust` |
| T082 | Physical ID would exceed server limits | Child-ID encoding version 1's deterministic truncation/hash remains stable on replay | `temporal-sdk-rust` |
| T083 | Workflow returns upstream and later starts another child | Recorded bridge namespace continues to apply deterministically | `temporal-sdk-rust` |
| T084 | Parent and child histories sync | Child exists before parent events that consume its result | `temporal-server` |
| T085 | Parent/child sync partially fails | Retry resumes per execution; none is released with incomplete history | `temporal-server` |
| T086 | Active child remains after parent sync | Child has its own ownership token and epoch | `temporal-server` |
| T087 | Continue-as-new occurs locally | Old/new runs synchronize without duplicate run creation and new run reacquires normally | cross-repository |
| T088 | Protocol-v1 local execution requests child start or a close that would create a successor run through continue-as-new, retry, or cron | The unsupported boundary hands execution back without sending a multi-run or multi-execution v1 sync; upstream validation rejects any such delta, and a later protocol version is required before T084-T087 can be enabled | cross-repository |

## Compatibility and stress

| ID | Scenario | Required result | Primary repository |
| --- | --- | --- | --- |
| T100 | Feature flags are disabled | Existing functional and SDK integration suites remain unchanged | all |
| T101 | Unknown bridge protocol version | Upstream consumes no matching task and returns a clear incompatibility | `temporal-server` |
| T102 | Inbox reaches configured count/byte limit | New mutation is rejected with a stable resource-exhausted result; existing entries remain intact | `temporal-server` |
| T103 | Sync approaches a dynamically configured request/backlog limit | Bridge triggers sync before the hard limit and the server atomically commits the bounded complete delta with bounded memory | cross-repository |
| T104 | 10x expected locally owned executions | Polling, expiration tasks, and mutable-state size remain bounded; no shard-wide scan is required | `temporal-server` |
| T105 | Bridge clock is skewed | Upstream lease time remains authoritative; local timer execution pauses outside allowed skew | `temporal-server` |

## Verification strategy

- Prefer server unit tests for mutable-state transitions, token/epoch validation, task suppression,
  inbox behavior, and timer-task staleness.
- Add server functional tests for matching acquisition, raw history import, task regeneration, and
  takeover.
- Add Core unit/history tests for child-ID transformation and replay lookahead.
- Add Core integration tests through `cargo integ-test` for bridge lifecycle and end-to-end Worker
  behavior.
- Add CLI command tests only for parsing, validation, and server option construction.
- Capture RPC traces or instrument a test client for T007 so the round-trip claim is asserted rather
  than inferred from latency.
- Run generation only from source proto changes and apply the standard repository formatting,
  linting, and test commands recorded in each repository's `AGENTS.md`.

## Prototype exit criteria

The prototype is successful when:

1. T001-T030 pass, excluding unused identifier gaps.
2. One Core Worker completes a multi-WFT, multi-Activity execution locally and produces replayable
   upstream history.
3. A forced partition pauses at the first synchronization boundary.
4. Both reconnection-before-expiry and takeover-after-expiry behave deterministically.
5. Normal Workers remain compatible when local execution is unsupported or disabled.
