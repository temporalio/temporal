package namespacereplication

import (
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventScheduleLocal is emitted at component creation. Schedules ApplyLocalTask.
type EventScheduleLocal struct{}

// TransitionScheduleLocal is the initial transition. Stays in RUNNING (component
// just created) and schedules the local apply task.
var TransitionScheduleLocal = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, _ EventScheduleLocal) error {
		ctx.AddTask(c, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{Attempt: 0})
		return nil
	},
)

// localRetryBackoff returns the delay after the given number of unresolved
// attempts. It uses the same capped doubling policy as peer retries without
// relying on queue-local, in-memory attempt counters.
func localRetryBackoff(completedAttempts int32) time.Duration {
	delay := defaultLocalRetryInitialInterval
	for attempt := int32(1); attempt < completedAttempts && delay < defaultLocalRetryMaximumInterval; attempt++ {
		if delay > defaultLocalRetryMaximumInterval/2 {
			return defaultLocalRetryMaximumInterval
		}
		delay *= 2
	}
	return min(delay, defaultLocalRetryMaximumInterval)
}

// EventLocalRetry records an unresolved local apply and schedules the next
// durable attempt. Attempts is both the number of unresolved results recorded
// and the generation carried by the next task.
type EventLocalRetry struct {
	Time     time.Time
	Attempts int32
	Delay    time.Duration
	Err      error
}

var TransitionLocalRetry = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalRetry) error {
		status := c.GetLocalApply()
		if status == nil {
			status = &namespacereplicationpb.LocalApplyStatus{}
		}
		if status.GetFirstAttemptAt() == nil {
			status.FirstAttemptAt = timestamppb.New(event.Time)
		}
		status.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING
		status.AttemptCount = event.Attempts
		status.LastAttemptAt = timestamppb.New(event.Time)
		if event.Err != nil {
			status.LastFailure = &failurepb.Failure{Message: event.Err.Error()}
		}
		c.LocalApply = status

		ctx.AddTask(c, chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(event.Delay),
		}, &namespacereplicationpb.ApplyLocalTask{Attempt: event.Attempts})
		return nil
	},
)

// EventLocalCommitted is emitted by ApplyLocalTask on successful CAS commit.
// Records the commit and schedules ApplyPeerTask{cell} for each peer.
type EventLocalCommitted struct {
	Time time.Time
}

var TransitionLocalCommitted = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalCommitted) error {
		status := c.GetLocalApply()
		if status == nil {
			status = &namespacereplicationpb.LocalApplyStatus{}
		}
		status.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
		status.ResolvedAt = timestamppb.New(event.Time)
		status.Failure = nil
		c.LocalApply = status
		// Fan out: one ApplyPeerTask per peer cell.
		for _, cell := range c.GetMutation().GetPeerCells() {
			ctx.AddTask(c, chasm.TaskAttributes{Destination: cell}, &namespacereplicationpb.ApplyPeerTask{
				TargetCell: cell,
				Attempt:    0,
			})
		}
		// NOTE: completion — including the zero-peer case (single-cluster global
		// namespace, nothing to fan out to) — is applied by the caller via
		// TransitionAllPeersTerminal, not here. Setting COMPLETED inside this
		// transition would be overwritten: the framework rewrites the component
		// status to this transition's destination (RUNNING) after Apply returns
		// (see Transition.Apply in statemachine.go), the same reason peer
		// completion needs its own transition.
		return nil
	},
)

// EventLocalFailed records a terminal local phase failure when the mutation is
// known not to have committed. Peers are never contacted.
type EventLocalFailed struct {
	Time    time.Time
	Err     error
	ErrType string // caller-facing gRPC error class; see classifyLocalErr
}

var TransitionLocalFailed = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_FAILED,
	func(c *NamespaceMutationComponent, _ chasm.MutableContext, event EventLocalFailed) error {
		status := c.GetLocalApply()
		if status == nil {
			status = &namespacereplicationpb.LocalApplyStatus{}
		}
		status.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED
		status.ResolvedAt = timestamppb.New(event.Time)
		status.Failure = localApplyFailure(event.Err, event.ErrType)
		c.LocalApply = status
		return nil
	},
)

// EventLocalIndeterminate records a terminal local outcome when the retry
// budget expires or a newer mutation erased the evidence needed to prove the
// result. The component fails, but the outcome remains distinct from FAILED
// because the metadata write may have committed. Peers are never contacted.
type EventLocalIndeterminate struct {
	Time     time.Time
	Attempts int32
	Err      error
}

var TransitionLocalIndeterminate = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_FAILED,
	func(c *NamespaceMutationComponent, _ chasm.MutableContext, event EventLocalIndeterminate) error {
		status := c.GetLocalApply()
		if status == nil {
			status = &namespacereplicationpb.LocalApplyStatus{}
		}
		if event.Attempts > status.GetAttemptCount() {
			if status.GetFirstAttemptAt() == nil {
				status.FirstAttemptAt = timestamppb.New(event.Time)
			}
			status.AttemptCount = event.Attempts
			status.LastAttemptAt = timestamppb.New(event.Time)
			if event.Err != nil {
				status.LastFailure = &failurepb.Failure{Message: event.Err.Error()}
			}
		}
		status.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_INDETERMINATE
		status.ResolvedAt = timestamppb.New(event.Time)
		status.Failure = localApplyFailure(event.Err, localFailureUnavailable)
		c.LocalApply = status
		return nil
	},
)

func localApplyFailure(err error, errType string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				// Type carries the caller-facing error class so the handler's poll
				// predicate can reconstruct the appropriate gRPC error.
				Type:         errType,
				NonRetryable: errType != localFailureUnavailable,
			},
		},
	}
}

// EventPeerCompleted is emitted by ApplyPeerTask after a peer reaches a terminal
// outcome (Applied, NoOpStale, NotAdmitted, or FailedTerminal). It updates the
// peer's status; the caller then transitions the component to COMPLETED once all
// peers are terminal.
//
// For per-peer success/no-op cases, see Outcome on PeerApplyStatus. Failure detail
// is in LastFailure when Outcome is FAILED_TERMINAL.
type EventPeerCompleted struct {
	Time       time.Time
	TargetCell string
	Outcome    namespacereplicationpb.PeerApplyOutcome
	Attempts   int32
	Err        error // may be nil for non-failure outcomes
}

// TransitionPeerCompleted updates one peer's outcome. Stays in RUNNING — callers
// should apply [TransitionAllPeersTerminal] afterward when c.allPeersTerminal()
// reports true, to move the component to COMPLETED. (The framework rewrites the
// component status to t.Destination after the apply function returns, so we
// can't directly set COMPLETED inside this transition's apply function.)
var TransitionPeerCompleted = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventPeerCompleted) error {
		status := c.GetPeerApply()[event.TargetCell]
		if status == nil {
			status = &namespacereplicationpb.PeerApplyStatus{}
		}
		if status.GetFirstAttemptAt() == nil {
			status.FirstAttemptAt = timestamppb.New(event.Time)
		}
		status.Outcome = event.Outcome
		status.AttemptCount = event.Attempts
		status.LastAttemptAt = timestamppb.New(event.Time)
		if event.Err != nil {
			status.LastFailure = &failurepb.Failure{
				Message: event.Err.Error(),
			}
		}
		c.PeerApply[event.TargetCell] = status
		return nil
	},
)

// EventAllPeersTerminal is emitted by the peer task handler after applying
// TransitionPeerCompleted, when c.allPeersTerminal() reports true. Moves the
// component from RUNNING to COMPLETED so retention can clean it up.
type EventAllPeersTerminal struct{}

var TransitionAllPeersTerminal = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
	func(_ *NamespaceMutationComponent, _ chasm.MutableContext, _ EventAllPeersTerminal) error {
		return nil
	},
)

// -----------------------------------------------------------------------------
// Peer retry: capped exponential backoff with a total budget.
// -----------------------------------------------------------------------------

// peerRetryBackoff returns the delay after the given number of completed
// attempts: exponential (initial * 2^(attempts-1)) capped at the configured
// maximum interval, overflow-safe.
func peerRetryBackoff(completedAttempts int32, policy peerRetryPolicy) time.Duration {
	delay := min(policy.initialInterval, policy.maximumInterval)
	for attempt := int32(1); attempt < completedAttempts && delay < policy.maximumInterval; attempt++ {
		// Saturate before doubling would exceed the configured maximum or
		// overflow time.Duration.
		if delay > policy.maximumInterval/2 {
			return policy.maximumInterval
		}
		delay *= 2
	}
	return delay
}

// EventPeerRetry records a retriable peer failure without marking the peer
// terminal: it updates the attempt count and last failure, marks the peer
// FAILED_RETRIABLE while waiting for a pure backoff task, and schedules that
// task. On peer recovery a later attempt
// succeeds; if the peer never recovers, recordPeerOutcome flips it to
// FAILED_TERMINAL once the retry budget is exhausted.
type EventPeerRetry struct {
	Time       time.Time
	TargetCell string
	Attempts   int32
	Delay      time.Duration
	Err        error
}

var TransitionPeerRetry = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventPeerRetry) error {
		status := c.GetPeerApply()[event.TargetCell]
		if status == nil {
			status = &namespacereplicationpb.PeerApplyStatus{}
		}
		if status.GetFirstAttemptAt() == nil {
			status.FirstAttemptAt = timestamppb.New(event.Time)
		}
		status.Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
		status.AttemptCount = event.Attempts
		status.LastAttemptAt = timestamppb.New(event.Time)
		if event.Err != nil {
			status.LastFailure = &failurepb.Failure{Message: event.Err.Error()}
		}
		c.PeerApply[event.TargetCell] = status

		// Destination tasks must be immediate, so the retry delay is represented by
		// a pure timer task. Its attempt matches the peer's AttemptCount, allowing
		// Validate to drop stale or duplicate timers from an earlier attempt.
		ctx.AddTask(c, chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(event.Delay),
		}, &namespacereplicationpb.ApplyPeerBackoffTask{
			TargetCell: event.TargetCell,
			Attempt:    event.Attempts,
		})
		return nil
	},
)

type EventPeerRetryReady struct {
	TargetCell string
	Attempt    int32
}

var TransitionPeerRetryReady = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventPeerRetryReady) error {
		peer := c.GetPeerApply()[event.TargetCell]
		if peer.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE ||
			peer.GetAttemptCount() != event.Attempt {
			return nil
		}
		// Invalidate the scheduled timer in the same transaction that dispatches
		// the next delivery. Only PENDING peers can execute outbound tasks.
		peer.Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING
		ctx.AddTask(c, chasm.TaskAttributes{Destination: event.TargetCell}, &namespacereplicationpb.ApplyPeerTask{
			TargetCell: event.TargetCell,
			Attempt:    event.Attempt,
		})
		return nil
	},
)
