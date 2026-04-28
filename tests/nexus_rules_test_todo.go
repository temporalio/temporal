package tests

// Additional rules to consider. Grouped by what they need from the model.
//
// === Rules using only facts already recorded ===
//
// TODO(nexusTerminalIsAbsorbingRule, safety):
//   Once an operation transitions to a terminal status, no further
//   Transition[nexusOperationStatus] should be recorded for that op. Iterate
//   the history; for each operation, after the first terminal transition,
//   any subsequent transition is a violation. Both `From` and `To` are
//   available on Transition, so this can also detect illegal cross-terminal
//   moves (e.g. Completed -> Canceled).
//
// TODO(nexusTerminalFromRunningRule, safety):
//   Every transition into a terminal status must have
//   From == nexusOperationStatusRunning. Guards against illegal jumps such
//   as Completed -> Terminated. Implementable in a single pass over
//   Transition[nexusOperationStatus] records.
//
// TODO(nexusNoTaskOutcomeAfterTerminalRule, safety):
//   Symmetric to nexusNoPostTerminalDispatchRule, but for `completed` and
//   `failed` task outcomes. Use umpire.LastTransitionSeqTo(history, isTerminalStatus)
//   exactly like nexusNoPostTerminalDispatchRule, then flag any nexusTaskEvent
//   with Outcome in {completed, failed} whose Seq > terminalSeq. Catches a
//   worker racing the terminator and resolving a task on a closed op.
//
// TODO(nexusAtMostOneInflightTaskPerKindRule, safety):
//   For each (operationID, kind), maintain a counter: +1 on `dispatched`,
//   -1 on `completed` or `failed`. The counter must never exceed 1 and never
//   go below 0. Catches matching double-dispatch and stray completions for
//   tasks that were never dispatched (overlaps with causality but with a
//   stronger bound).
//
// TODO(nexusFailedTaskEventuallyResolvesRule, liveness):
//   Every `failed` outcome for a given (operationID, kind) must be followed
//   by another `dispatched` for the same key, a `completed` for the same key,
//   or a terminal Transition for that operationID. On non-final Check, track
//   unresolved failed events as pending; on final Check, any still-pending
//   becomes a violation. This is the highest-value liveness rule given that
//   DoPollAndFail is part of the model.
//
// TODO(nexusStartEventuallyDispatchedRule, liveness):
//   Every operation that appears in the history (i.e. any nexusTaskEvent or
//   Transition referencing the op) must eventually have at least one
//   nexusTaskEvent with Kind=start, Outcome=dispatched — unless the op
//   reaches a terminal Transition with no prior start dispatch (legitimate
//   if terminated before any matching attempt). Resolve pending entries when
//   either condition is met; flag remaining on final.
//
// === Rules that need a small model addition ===
//
// TODO(nexusCancelTaskRequiresCancelRequestRule, safety):
//   Add a new fact type, e.g. `nexusCancelRequestedFact{OperationID, RequestID}`,
//   recorded inside DoCancelOperation right after a successful
//   RequestCancelNexusOperationExecution. Rule: no nexusTaskEvent with
//   Kind=cancel and Outcome=dispatched may appear for an operation before
//   the first nexusCancelRequestedFact for that operation. Today nothing
//   prevents a spurious cancel-task dispatch from passing all rules.
//
// TODO(nexusCancelRequestDispatchesCancelTaskRule, liveness):
//   Builds on nexusCancelRequestedFact above. Once a cancel is requested for
//   an op, a nexusTaskEvent{Kind=cancel, Outcome=dispatched} must eventually
//   follow — unless the op reaches a terminal Transition first (terminate or
//   start-completion can pre-empt the cancel task). Mirror of
//   nexusStartEventuallyDispatchedRule for cancellation paths. Codifies the
//   behavior exercised by TestStandaloneNexusOperationCancel in
//   nexus_standalone_test.go.
//
// TODO(nexusCancelRequestIdempotenceRule, safety):
//   Extend the model with a DoCancelOperationAgain step that re-issues
//   RequestCancelNexusOperationExecution. Record both the request ID used
//   and whether the call returned an error. Rule:
//   - Repeat with the same RequestID as a prior accepted cancel must not error.
//   - Repeat with a different RequestID must error with "cancellation already requested".
//   Direct port of nexus_standalone_test.go:873 (s.Run("AlreadyCanceled", ...))
//   into the property test.
//
// TODO(nexusTerminateOverridesCancelRequestRule, safety):
//   Builds on nexusCancelRequestedFact. If a nexusCancelRequestedFact is
//   followed by a Transition into nexusOperationStatusTerminated for the
//   same op, no later Transition into nexusOperationStatusCanceled for that
//   op is allowed, and the final terminal status must be Terminated.
//   Codifies the override behavior asserted at
//   nexus_standalone_test.go:1098-1134.
//
// TODO(nexusRunIDStabilityRule, safety):
//   Record a fact, e.g. `nexusRunIDFact{OperationID, RunID}`, the first time
//   a run ID is observed for an operation (initially from the
//   StartNexusOperationExecution response in DoStartOperation, and from
//   Describe/Poll responses thereafter). Rule: all nexusRunIDFact entries
//   for the same OperationID must share the same RunID. Lifts the inline
//   require.Equal in DoDescribeOperation into a global invariant that fires
//   on every step that observes a run ID.
//
// === Liveness rules requiring traffic observation ===
//
// The umpire exposes a TrafficObserver hook (see umpire.New / umpire.Umpire
// in tests/testcore/umpire) that this model does not currently use. Wiring
// it up — by passing a server-side gRPC interceptor when constructing the
// test env and implementing ObserveTraffic on *nexusPropModel — would
// record observed request/response pairs as facts and unlock the following
// rules without scattering record* calls through every action.
//
// TODO(nexusTerminalReflectedByDescribeRule, liveness):
//   Every Transition into a terminal status must eventually be reflected by
//   a DescribeNexusOperationExecution response whose
//   Info.Status == toProtoStatus(terminalStatus). Pending until observed;
//   flagged on final Check if not. Catches staleness windows that
//   CheckInvariant only samples at step boundaries.
//
// TODO(nexusPollWaitStageCorrectnessRule, safety):
//   Observed PollNexusOperationExecution responses with
//   WaitStage == NEXUS_OPERATION_WAIT_STAGE_CLOSED must coincide with the op
//   being in a terminal status (per the most recent Transition before the
//   response was observed). Mirrors the assertions at
//   nexus_standalone_test.go:1958-1965.
//
// ============================================================================
// Properties that push past the current umpire framework.
//
// The categories below describe invariants we'd want to assert but can't
// today without extending tests/testcore/umpire. Each TODO calls out the
// specific framework gap so the framework work and the rule work can be
// scoped together.
// ============================================================================
//
// === Time-bounded properties (need wall-clock timestamps on Records) ===
//
// Today Record only carries Seq. Adding `At time.Time` (set at Record() time)
// would let rules express "X happens within Y of trigger". This needs:
//   - umpire.Record gains an At field.
//   - LivenessRule gets a way to fail if pending entries exceed a per-rule
//     deadline (rather than only at final). One option: extend the
//     LivenessRule contract with a `Deadline(pending)` hook the umpire calls
//     against the latest record's clock between actions.
//
// TODO(nexusScheduleToStartTimeoutRule, safety+liveness):
//   When StartNexusOperationExecution is called with ScheduleToStartTimeout=D
//   and no successful start dispatch occurs within D after the schedule
//   record, the op must transition to a terminal failure with
//   TimeoutType=SCHEDULE_TO_START. Conversely, if a start was dispatched
//   before D elapsed, no SCHEDULE_TO_START timeout failure is permitted.
//   Requires recording the timeout values as facts at start time.
//
// TODO(nexusStartToCloseTimeoutRule, safety+liveness):
//   After a start task is dispatched and StartToCloseTimeout=D is set, the
//   task's `completed`/`failed` must arrive within D, or the op must become
//   terminal with TimeoutType=START_TO_CLOSE. No START_TO_CLOSE timeout
//   should fire before any start dispatch.
//
// TODO(nexusScheduleToCloseTimeoutBoundsRule, safety):
//   Total time from the first schedule fact to the first terminal Transition
//   must be <= ScheduleToCloseTimeout + tolerance. Catches timer-leak bugs
//   where ops outlive their close deadline.
//
// TODO(nexusRetryBackoffMonotonicRule, safety):
//   For each (op, kind), the gap between consecutive `dispatched` events
//   separated by a `failed` event should be non-decreasing (retryable
//   backoff). Strict bound is hard, but a weaker form — "second retry gap
//   >= first retry gap, modulo jitter window" — catches retry config
//   regressions. Needs timestamps + a tunable jitter epsilon.
//
// TODO(nexusEventualReflectionDeadlineRule, liveness):
//   Stronger version of nexusTerminalReflectedByDescribeRule: every terminal
//   Transition must be reflected by Describe within a bounded duration
//   (e.g., 1s). Today liveness rules only fire on `final`; with deadlines we
//   can fail mid-test on staleness instead of waiting for cleanup.
//
// === Quantitative invariants (need numeric assertions over facts) ===
//
// Rules currently express ordering only. A small helper for "max/min/sum
// over facts grouped by Key" would let us encode counter properties cleanly.
//
// TODO(nexusAttemptCountMatchesRetriesRule, safety):
//   Describe.Info.Attempt should equal 1 + (count of `failed` start outcomes
//   for that op observed before the response). Requires correlating Describe
//   responses (TrafficObserver) with task history. Catches off-by-one
//   regressions in attempt accounting.
//
// TODO(nexusStateTransitionCountMonotonicRule, safety):
//   Across all observed Describe responses for the same op, Info.StateTransitionCount
//   must be non-decreasing in observation order. And the count at any point
//   must be >= the number of Transition records for that op recorded so far.
//
// TODO(nexusListVsDescribeAgreementRule, safety):
//   For each op present in a ListNexusOperations response, the per-op fields
//   (status, endpoint, service, operation, runID) must agree with the most
//   recent Describe response for that op. Catches visibility-vs-primary
//   divergence. Requires TrafficObserver.
//
// TODO(nexusCountEqualsListSizeRule, safety):
//   For matching query strings, CountNexusOperations.Count must equal the
//   total Operations across paginated ListNexusOperations responses. Catches
//   index drift bugs.
//
// === Concurrency / linearizability (need a reference state machine) ===
//
// The current model tracks per-op status as a single int. A linearizability
// checker would maintain a parallel reference state machine and assert that
// the observed sequence of API responses is consistent with *some* sequential
// execution of those calls in real-time order. This is a separate framework
// effort (similar to porcupine or knossos).
//
// TODO(nexusLinearizabilityRule, safety):
//   For each operationID, the sequence of {Start, RequestCancel, Terminate,
//   Describe, Poll, Delete} calls + their responses must be linearizable with
//   respect to a reference Nexus operation state machine. This subsumes
//   several of the simpler rules above and catches cross-API races that
//   per-rule checks miss (e.g., a Describe sneaking between a Terminate
//   request and its persistence and observing Running, then a later Describe
//   observing Running again — non-monotonic).
//
// TODO(nexusReadYourWritesPerSessionRule, safety):
//   Within a single client session (same gRPC connection or same Identity),
//   any successful mutating call must be observable on the immediate
//   following read. Stronger than eventual consistency. Requires the
//   TrafficObserver to tag records with a session identifier.
//
// === Fault injection / chaos (need controllable nondeterminism) ===
//
// These properties are about resilience. They need a runtime that can pause,
// restart, or inject faults into specific server components mid-test. Today
// the test env runs a single in-process server with no chaos hooks. Possible
// extension: a "fault injector" interface the umpire can drive from rapid
// (so failures become part of the property search space).
//
// TODO(nexusCrashRecoveryRule, safety):
//   Restart the history/matching service mid-test. After restart, all
//   pre-restart Transitions must still be observable, no Transitions are
//   replayed (no double-fire of terminal effects), and any in-flight tasks
//   are eventually re-dispatched. Requires a restartable test env.
//
// TODO(nexusPersistenceCrashConsistencyRule, safety):
//   Inject a crash between an API call returning success and the next
//   observation. The post-crash state must reflect every successful call
//   that returned before the crash. (Equivalent to assert "no lost writes".)
//
// TODO(nexusWorkerCrashRedispatchRule, liveness):
//   If the worker crashes after polling a task but before responding, the
//   task must be re-dispatched after the visibility timeout. Liveness:
//   eventually re-dispatched; safety: at most one concurrent dispatch.
//
// === Negative / quiescence properties (need explicit "no progress" checks) ===
//
// Most rules check "X happens"; a few should check "nothing happens".
// Implementable today but needs a "quiet window" primitive in the model:
// after stopping all action drawing, assert the umpire history doesn't grow
// for some duration (modulo expected background polls).
//
// TODO(nexusNoSpuriousDispatchRule, safety):
//   With no operations started, no nexusTaskEvent should be recorded. With
//   all operations terminal, no further nexusTaskEvent (any outcome) should
//   be recorded. Stronger version of nexusNoPostTerminalDispatchRule that
//   also rules out cross-op contamination.
//
// TODO(nexusTerminalQuiescenceRule, liveness):
//   After every op reaches a terminal status, the umpire history stops
//   growing within a bounded window. Catches "background goroutine keeps
//   firing tasks" regressions.
//
// === External-handler observation (need a recording handler harness) ===
//
// nexus_standalone_test.go uses nexustest.Handler closures to observe what
// the external handler sees (headers, payload, cancel token). Lifting these
// observations into umpire facts would let us write rules over them. This
// needs a small library: a handler that records every invocation as a fact.
//
// TODO(nexusHeaderForwardingRule, safety):
//   Headers passed to StartNexusOperationExecution must be visible verbatim
//   in the OnStartOperation handler call, and the same headers must be
//   visible in the OnCancelOperation handler call when cancellation is
//   requested. Codifies TestStandaloneNexusOperationCancel/RequestCancel_ForwardsOriginalNexusHeaders.
//
// TODO(nexusPayloadIntegrityRule, safety):
//   The payload bytes given to StartNexusOperationExecution must match what
//   the OnStartOperation handler receives, with the same content type
//   metadata. Catches encoding regressions.
//
// TODO(nexusCancelTokenMatchesStartRule, safety):
//   The OperationToken delivered by an async OnStartOperation must equal the
//   token passed to OnCancelOperation for the same op. Catches token
//   threading bugs across the start/cancel boundary.
//
// === Symbolic / generative scope (need richer rapid generators) ===
//
// Today actions use fixed inputs (one timeout, one endpoint). Drawing
// timeouts, headers, payload sizes, and ID-conflict policies from rapid
// generators would let rules find boundary bugs. The framework already
// exposes umpire.Draw; we mostly need named generators for these domains.
//
// TODO(nexusIDConflictPolicyRule, safety):
//   Property: for any two StartNexusOperationExecution calls with the same
//   OperationId, the response must follow the matrix in
//   nexus_standalone_test.go:163-217 — same RequestId returns existing run
//   with Started=false; different RequestId with FAIL policy errors with
//   NexusOperationExecutionAlreadyStarted; different RequestId with
//   USE_EXISTING returns existing run. Requires drawing RequestIds and
//   policies from rapid.
//
// TODO(nexusTimeoutCompositionRule, safety):
//   Property: for any drawn (ScheduleToStart, StartToClose, ScheduleToClose)
//   triple, the operation must close within
//   min(ScheduleToClose, ScheduleToStart + StartToClose) + tolerance, with
//   the timeout type matching whichever bound fired. Composes the three
//   timeout rules above into one search space. Requires timestamps and
//   timeout-value facts.
//
// === Refinement testing (need an in-memory reference) ===
//
// A reference Nexus state machine, written as straightforward Go code that
// mirrors the spec, could run alongside each action. The property is then
// "the observable result of every API call equals the reference's output".
// This is the strongest single property we could add but requires a maintained
// reference implementation as a new package.
//
// TODO(nexusReferenceRefinementRule, safety):
//   For every Do* action, after invoking the real API and the reference
//   model with the same inputs, assert that response fields visible to a
//   client (status, runID, error type, presence/absence of fields) agree.
//   The reference doesn't need to be fast or persistent — it just needs to
//   encode the spec. This catches every behavioral divergence at once and
//   lets future rules be derived by inspection of reference behavior.
