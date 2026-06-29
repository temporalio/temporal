package nsrepl

import (
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventScheduleLocal is emitted at component creation. Schedules ApplyLocalTask.
type EventScheduleLocal struct{}

// TransitionScheduleLocal: initial transition. Stays in RUNNING (component just created)
// and schedules the local apply task.
var TransitionScheduleLocal = chasm.NewTransition(
	[]nsreplpb.ComponentStatus{nsreplpb.COMPONENT_STATUS_RUNNING},
	nsreplpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, _ EventScheduleLocal) error {
		ctx.AddTask(c, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{})
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
	[]nsreplpb.ComponentStatus{nsreplpb.COMPONENT_STATUS_RUNNING},
	nsreplpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalCommitted) error {
		c.LocalApply = &nsreplpb.LocalApplyStatus{
			Outcome:    nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED,
			NewVersion: event.NewVersion,
			AppliedAt:  timestamppb.New(event.Time),
		}
		// Fan out: one ApplyPeerTask per peer cell.
		for _, cell := range c.GetMutation().GetPeerCells() {
			ctx.AddTask(c, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{
				TargetCell: cell,
				Attempt:    0,
			})
		}
		// If there are zero peers (single-cluster global namespace), the component is done.
		if len(c.GetMutation().GetPeerCells()) == 0 {
			c.SetStateMachineState(nsreplpb.COMPONENT_STATUS_COMPLETED)
		}
		return nil
	},
)

// EventLocalFailed is emitted by ApplyLocalTask on terminal failure (CAS conflict,
// validation error, store unavailable past retry budget). Transitions to FAILED;
// peers are never contacted. This is the "no divergence on caller-visible failure"
// guarantee.
type EventLocalFailed struct {
	Time time.Time
	Err  error
}

var TransitionLocalFailed = chasm.NewTransition(
	[]nsreplpb.ComponentStatus{nsreplpb.COMPONENT_STATUS_RUNNING},
	nsreplpb.COMPONENT_STATUS_FAILED,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventLocalFailed) error {
		c.LocalApply = &nsreplpb.LocalApplyStatus{
			Outcome:   nsreplpb.LOCAL_APPLY_OUTCOME_FAILED,
			AppliedAt: timestamppb.New(event.Time),
			Failure: &failurepb.Failure{
				Message: event.Err.Error(),
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
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
	Outcome    nsreplpb.PeerApplyOutcome
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
	[]nsreplpb.ComponentStatus{nsreplpb.COMPONENT_STATUS_RUNNING},
	nsreplpb.COMPONENT_STATUS_RUNNING,
	func(c *NamespaceMutationComponent, ctx chasm.MutableContext, event EventPeerCompleted) error {
		status := c.GetPeerApply()[event.TargetCell]
		if status == nil {
			status = &nsreplpb.PeerApplyStatus{}
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
	[]nsreplpb.ComponentStatus{nsreplpb.COMPONENT_STATUS_RUNNING},
	nsreplpb.COMPONENT_STATUS_COMPLETED,
	func(_ *NamespaceMutationComponent, _ chasm.MutableContext, _ EventAllPeersTerminal) error {
		return nil
	},
)
