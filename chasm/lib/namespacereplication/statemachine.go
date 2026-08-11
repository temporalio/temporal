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
		ctx.AddTask(c, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{})
		return nil
	},
)

// EventLocalCommitted is emitted by ApplyLocalTask on successful CAS commit.
// Records the new version and schedules ApplyPeerTask{cell} for each peer.
type EventLocalCommitted struct {
	Time       time.Time
	NewVersion int64
}

var TransitionLocalCommitted = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalCommitted) error {
		c.LocalApply = &namespacereplicationpb.LocalApplyStatus{
			Outcome:    namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
			NewVersion: event.NewVersion,
			AppliedAt:  timestamppb.New(event.Time),
		}
		// Fan out: one ApplyPeerTask per peer cell.
		for _, cell := range c.GetMutation().GetPeerCells() {
			ctx.AddTask(c, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{
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

// EventLocalFailed is emitted by ApplyLocalTask on terminal failure (CAS conflict,
// validation error, store unavailable past retry budget). Transitions to FAILED;
// peers are never contacted. This is the "no divergence on caller-visible failure"
// guarantee.
type EventLocalFailed struct {
	Time    time.Time
	Err     error
	ErrType string // caller-facing gRPC error class; see classifyLocalErr
}

var TransitionLocalFailed = chasm.NewTransition(
	[]namespacereplicationpb.ComponentStatus{namespacereplicationpb.COMPONENT_STATUS_RUNNING},
	namespacereplicationpb.COMPONENT_STATUS_FAILED,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalFailed) error {
		c.LocalApply = &namespacereplicationpb.LocalApplyStatus{
			Outcome:   namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED,
			AppliedAt: timestamppb.New(event.Time),
			Failure: &failurepb.Failure{
				Message: event.Err.Error(),
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						// Type carries the caller-facing error class so the handler's
						// poll predicate can reconstruct the right gRPC error; only a
						// CAS conflict / transient store failure (Unavailable) is retriable.
						Type:         event.ErrType,
						NonRetryable: event.ErrType != localFailureUnavailable,
					},
				},
			},
		}
		return nil
	},
)

// EventPeerCompleted is emitted by ApplyPeerTask after a peer reaches a terminal outcome
// (Applied, NoOpStale, or FailedTerminal). Updates the peer's status; if all peers are
// now terminal, the component transitions to COMPLETED.
//
// For per-peer success/no-op cases, see Outcome on PeerApplyStatus. Failure detail is
// in LastFailure when Outcome is FAILED_*.
type EventPeerCompleted struct {
	Time       time.Time
	TargetCell string
	Outcome    namespacereplicationpb.PeerApplyOutcome
	NewVersion int64
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
		status.NewVersion = event.NewVersion
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

const (
	// peerRetryBaseInterval is the first backoff interval; it doubles per attempt.
	peerRetryBaseInterval = time.Second
	// peerRetryMaxInterval caps the backoff so a long peer outage retries at a
	// steady cadence instead of ever-growing gaps.
	peerRetryMaxInterval = 5 * time.Minute
	// peerRetryBudget is the total wall-clock window (from the first failed
	// attempt) over which a retriable peer failure keeps retrying before it is
	// given up as FAILED_TERMINAL so the component can still complete.
	peerRetryBudget = 7 * 24 * time.Hour
)

// peerRetryBackoff returns the delay before the given attempt: exponential
// (base * 2^(attempt-1)) capped at peerRetryMaxInterval, overflow-safe.
func peerRetryBackoff(attempt int32) time.Duration {
	if attempt < 1 {
		return peerRetryBaseInterval
	}
	if attempt > 20 { // 2^19s already dwarfs the cap; avoid shift overflow
		return peerRetryMaxInterval
	}
	d := peerRetryBaseInterval * time.Duration(int64(1)<<uint(attempt-1))
	if d <= 0 || d > peerRetryMaxInterval {
		return peerRetryMaxInterval
	}
	return d
}

// EventPeerRetry records a retriable peer failure without marking the peer
// terminal: it updates the attempt count and last failure, keeps the peer
// PENDING (so ApplyPeerTask.Validate lets the next attempt run), and schedules
// the next ApplyPeerTask with backoff. On peer recovery a later attempt
// succeeds; if the peer never recovers, recordPeerOutcome flips it to
// FAILED_TERMINAL once the retry budget is exhausted.
type EventPeerRetry struct {
	Time       time.Time
	TargetCell string
	Attempt    int32
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
		// Keep PENDING: the peer isn't done, it's between retries.
		status.Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING
		status.AttemptCount = event.Attempt
		status.LastAttemptAt = timestamppb.New(event.Time)
		if event.Err != nil {
			status.LastFailure = &failurepb.Failure{Message: event.Err.Error()}
		}
		c.PeerApply[event.TargetCell] = status

		// Schedule the next attempt with capped exponential backoff. The task's
		// Attempt matches the peer's AttemptCount, so Validate admits exactly this
		// task and drops any stale/duplicate retry from an earlier attempt.
		ctx.AddTask(c, chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(peerRetryBackoff(event.Attempt)),
		}, &namespacereplicationpb.ApplyPeerTask{
			TargetCell: event.TargetCell,
			Attempt:    event.Attempt,
		})
		return nil
	},
)
