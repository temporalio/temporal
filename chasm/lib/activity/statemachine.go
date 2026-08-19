package activity

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
)

// Ensure that Activity implements chasm.StateMachine interface
var _ chasm.StateMachine[activitypb.ActivityExecutionStatus] = (*Activity)(nil)

// StateMachineState returns the current status of the activity.
func (a *Activity) StateMachineState() activitypb.ActivityExecutionStatus {
	if a.ActivityState == nil {
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	}
	return a.Status
}

// SetStateMachineState sets the status of the activity.
func (a *Activity) SetStateMachineState(state activitypb.ActivityExecutionStatus) {
	a.Status = state
}

// TransitionScheduled transitions to Scheduled status. This is only called on the initial
// scheduling of the activity.
var TransitionScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		return a.applyScheduled(ctx)
	},
)

type rescheduleEvent struct {
	retryInterval       time.Duration
	retryIntervalSource activitypb.ActivityRetryIntervalSource
	failure             *failurepb.Failure
	timeoutType         enumspb.TimeoutType
}

// TransitionRescheduled transitions to Scheduled from Started, which happens on retries. The event
// to pass in is the failure to be recorded from the previously failed attempt.
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // For retries the activity will be in started status
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		return a.applyRescheduled(ctx, event)
	},
)

// TransitionStarted transitions to Started status.
var TransitionStarted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) error {
		return a.applyStarted(ctx, request)
	},
)

type completeEvent struct {
	req             *historyservice.RespondActivityTaskCompletedRequest
	baseHandler     metrics.Handler
	enrichedHandler metrics.Handler
}

// TransitionCompleted transitions to Completed status. SCHEDULED and PAUSED are included because
// RespondActivityTaskCompletedById can force-complete an activity that has no attempt in
// progress, mirroring workflow-activity behavior.
var TransitionCompleted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(a *Activity, ctx chasm.MutableContext, event completeEvent) error {
		return a.applyCompleted(ctx, event)
	},
)

type failedEvent struct {
	req             *historyservice.RespondActivityTaskFailedRequest
	retryState      enumspb.RetryState
	baseHandler     metrics.Handler
	enrichedHandler metrics.Handler
}

// TransitionFailed transitions to Failed status.
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(a *Activity, ctx chasm.MutableContext, event failedEvent) error {
		return a.applyFailed(ctx, event)
	},
)

type terminateEvent struct {
	request        chasm.TerminateComponentRequest
	metricsHandler metrics.Handler
	fromStatus     activitypb.ActivityExecutionStatus
}

// TransitionTerminated transitions to Terminated status.
var TransitionTerminated = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(a *Activity, ctx chasm.MutableContext, event terminateEvent) error {
		return a.applyTerminated(ctx, event)
	},
)

// TransitionCancelRequested transitions to CancelRequested status.
var TransitionCancelRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	func(a *Activity, ctx chasm.MutableContext, req *workflowservice.RequestCancelActivityExecutionRequest) error {
		return a.applyCancelRequested(ctx, req)
	},
)

type cancelEvent struct {
	details        *commonpb.Payloads
	metricsHandler metrics.Handler
	fromStatus     activitypb.ActivityExecutionStatus
}

// TransitionCanceled transitions to Canceled status.
var TransitionCanceled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
	func(a *Activity, ctx chasm.MutableContext, event cancelEvent) error {
		return a.applyCanceled(ctx, event)
	},
)

type timeoutEvent struct {
	metricsHandler metrics.Handler
	timeoutType    enumspb.TimeoutType
	retryState     enumspb.RetryState
}

// TransitionTimedOut transitions to TimedOut status.
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, event timeoutEvent) error {
		return a.applyTimedOut(ctx, event)
	},
)

type pauseEvent struct {
	req            *workflowservice.PauseActivityExecutionRequest
	metricsHandler metrics.Handler
}

// TransitionPaused transitions a SCHEDULED activity to PAUSED status. The stamp is bumped to
// invalidate any pending dispatch task so the activity is not dispatched while paused.
var TransitionPaused = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	func(a *Activity, ctx chasm.MutableContext, event pauseEvent) error {
		return a.applyPaused(ctx, event)
	},
)

// TransitionPauseRequested transitions a STARTED activity to PAUSE_REQUESTED. The worker is still
// in charge of the activity. It will be notified via ActivityPaused=true on its next heartbeat
// response, its task token is not invalidated by this transition, and there is no stamp bump since
// StartToCloseTimeoutTask and HeartbeatTimeoutTask must stay valid.
var TransitionPauseRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
	func(a *Activity, ctx chasm.MutableContext, event pauseEvent) error {
		return a.applyPauseRequested(ctx, event)
	},
)

type unpauseEvent struct {
	req            *workflowservice.UnpauseActivityExecutionRequest
	metricsHandler metrics.Handler
}

// TransitionUnpaused transitions PAUSED → SCHEDULED, triggering a dispatch task that will lead to
// another activity attempt.
var TransitionUnpaused = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event unpauseEvent) error {
		return a.applyUnpaused(ctx, event)
	},
)

// TransitionUnpausedWhilePauseRequested transitions PAUSE_REQUESTED → STARTED. The worker is still in charge
// of the activity. Its task token is not invalidated by this transition, and there is no stamp bump
// since StartToCloseTimeoutTask and HeartbeatTimeoutTask must stay valid.
var TransitionUnpausedWhilePauseRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, event unpauseEvent) error {
		return nil
	},
)

// TransitionAttemptFailedWhilePauseRequested transitions PAUSE_REQUESTED → PAUSED. It is performed instead of
// TransitionReschedule, when the activity is in PAUSE_REQUESTED and the worker yields (failure or
// timeout) with retries remaining. The failed attempt is recorded and Count is incremented.
var TransitionAttemptFailedWhilePauseRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		return a.applyFailedAttempt(ctx, event)
	},
)

type resetEvent struct {
	req            *workflowservice.ResetActivityExecutionRequest
	resetTime      time.Time
	metricsHandler metrics.Handler
}

// TransitionReset resets a SCHEDULED or PAUSED activity back to attempt 1. The stamp is bumped to
// invalidate any pending dispatch task, then a new dispatch task is added at the given schedule time.
// For STARTED activities the reset is deferred — the activity transitions to RESET_REQUESTED via
// TransitionResetRequested and lands back in SCHEDULED via TransitionResetAttemptFailedToScheduled
// when the worker yields.
var TransitionReset = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event resetEvent) error {
		return a.applyReset(ctx, event)
	},
)

// TransitionResetRequested transitions a STARTED or PAUSE_REQUESTED activity to RESET_REQUESTED.
// PAUSE_REQUESTED is allowed when the operator issues reset with keepPaused=true: ResetShouldPause is
// set so the activity lands back in PAUSED (not SCHEDULED) when the worker yields. The worker is
// still in charge of the activity; it will be notified via
// ActivityReset=true on its next heartbeat response, its task token is not invalidated by this
// transition, and there is no stamp bump since StartToCloseTimeoutTask and HeartbeatTimeoutTask
// must stay valid.
var TransitionResetRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionResetAttemptFailedToPaused transitions RESET_REQUESTED → PAUSED. It is performed
// when the worker yields in RESET_REQUESTED with ResetShouldPause set (i.e. reset was issued with
// keepPaused=true while the activity was in PAUSE_REQUESTED). The failed attempt is recorded, the
// attempt count is reset to 1, and no dispatch task is emitted — the activity stays paused until
// an explicit unpause.
var TransitionResetAttemptFailedToPaused = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		return a.applyResetAttemptFailedToPaused(ctx, event)
	},
)

// TransitionResetAttemptFailedToScheduled transitions RESET_REQUESTED → SCHEDULED. It is performed
// instead of TransitionRescheduled when the activity is in RESET_REQUESTED and the worker yields
// (failure or timeout) with retries remaining. The failed attempt is recorded and the count is
// reset to 1 (so the next attempt is "attempt 1").
var TransitionResetAttemptFailedToScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		return a.applyResetAttemptFailedToScheduled(ctx, event)
	},
)
