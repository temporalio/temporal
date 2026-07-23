package activity

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		attempt := a.LastAttempt.Get(ctx)

		attempt.Count++
		attempt.Stamp++

		// Start delay defers the dispatch and extends ScheduleToClose and ScheduleToStart timeouts. StartToClose and
		// Heartbeat timeouts are unaffected as they only start when a worker picks up the task.
		dispatchTime := a.firstDispatchTime()
		attempt.DispatchTime = timestamppb.New(dispatchTime)

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: dispatchTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Stamp: attempt.GetStamp(),
				})
		}

		if deadline := a.scheduleToCloseDeadline(); !deadline.IsZero() {
			a.ScheduleToCloseStamp++
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: deadline,
				},
				&activitypb.ScheduleToCloseTimeoutTask{Stamp: a.GetScheduleToCloseStamp()})
		}

		dispatchAttrs := chasm.TaskAttributes{}
		if dispatchTime.After(a.ScheduleTime.AsTime()) {
			dispatchAttrs.ScheduledTime = dispatchTime
		}
		ctx.AddTask(
			a,
			dispatchAttrs,
			a.newActivityDispatchTask(ctx))

		return nil
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
		if err := a.applyFailedAttempt(ctx, event); err != nil {
			return err
		}

		attempt := a.LastAttempt.Get(ctx)
		retryScheduledTime := dispatchTimeForRetry(attempt).AsTime()
		attempt.DispatchTime = timestamppb.New(retryScheduledTime)

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: retryScheduledTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Stamp: attempt.GetStamp(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: retryScheduledTime,
			},
			a.newActivityDispatchTask(ctx))

		return nil
	},
)

type startedEvent struct {
	request        *historyservice.RecordActivityTaskStartedRequest
	metricsHandler metrics.Handler
}

// TransitionStarted transitions to Started status.
var TransitionStarted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, event startedEvent) error {
		request := event.request
		attempt := a.LastAttempt.Get(ctx)
		attempt.StartedTime = timestamppb.New(ctx.Now(a))
		attempt.StartedStamp = request.GetStamp()
		// Record the first-ever worker pickup time once and never update on retries or resets.
		if a.FirstAttemptStartedTime == nil {
			a.FirstAttemptStartedTime = attempt.GetStartedTime()
		}
		attempt.StartRequestId = request.GetRequestId()
		attempt.LastWorkerIdentity = request.GetPollRequest().GetIdentity()
		attempt.SdkName = ctx.RequestHeader(headers.ClientNameHeaderName)
		attempt.SdkVersion = ctx.RequestHeader(headers.ClientVersionHeaderName)
		if versionDirective := request.GetVersionDirective().GetDeploymentVersion(); versionDirective != nil {
			attempt.LastDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{
				BuildId:        versionDirective.GetBuildId(),
				DeploymentName: versionDirective.GetDeploymentName(),
			}
		}
		startTime := attempt.GetStartedTime().AsTime()
		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: startTime.Add(a.GetStartToCloseTimeout().AsDuration()),
			},
			&activitypb.StartToCloseTimeoutTask{
				Stamp: a.LastAttempt.Get(ctx).GetStamp(),
			})

		if heartbeatTimeout := a.GetHeartbeatTimeout().AsDuration(); heartbeatTimeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: startTime.Add(heartbeatTimeout),
				},
				&activitypb.HeartbeatTimeoutTask{
					Stamp: attempt.GetStamp(),
				})
		}

		a.emitScheduleToStartLatency(ctx, event.metricsHandler)

		return nil
	},
)

type completeEvent struct {
	req            *historyservice.RespondActivityTaskCompletedRequest
	metricsHandler metrics.Handler
}

// TransitionCompleted transitions to Completed status.
var TransitionCompleted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(a *Activity, ctx chasm.MutableContext, event completeEvent) error {
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			req := event.req.GetCompleteRequest()

			attempt := a.LastAttempt.Get(ctx)
			attempt.CompleteTime = timestamppb.New(ctx.Now(a))
			attempt.LastWorkerIdentity = req.GetIdentity()
			outcome := a.Outcome.Get(ctx)
			outcome.Variant = &activitypb.ActivityOutcome_Successful_{
				Successful: &activitypb.ActivityOutcome_Successful{
					Output: req.GetResult(),
				},
			}

			a.emitOnCompletedMetrics(ctx, event.metricsHandler)

			return nil
		})
	},
)

type failedEvent struct {
	req            *historyservice.RespondActivityTaskFailedRequest
	metricsHandler metrics.Handler
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
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			req := event.req.GetFailedRequest()

			if details := req.GetLastHeartbeatDetails(); details != nil {
				heartbeat := a.getOrCreateLastHeartbeat(ctx)
				heartbeat.Details = details
				heartbeat.RecordedTime = timestamppb.New(ctx.Now(a))
			}
			attempt := a.LastAttempt.Get(ctx)
			attempt.LastWorkerIdentity = req.GetIdentity()

			if err := a.recordFailedAttempt(ctx, 0, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED, req.GetFailure(), ctx.Now(a), true); err != nil {
				return err
			}

			a.emitOnFailedMetrics(ctx, event.metricsHandler)

			return nil
		})
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
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			a.TerminateState = &activitypb.ActivityTerminateState{
				RequestId: event.request.RequestID,
			}
			outcome := a.Outcome.Get(ctx)
			failure := &failurepb.Failure{
				Message: event.request.Reason,
				FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
					TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
						Identity: event.request.Identity,
					},
				},
			}
			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			a.emitOnTerminatedMetrics(event.metricsHandler)

			return nil
		})
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
		a.CancelState = &activitypb.ActivityCancelState{
			Identity:    req.GetIdentity(),
			RequestId:   req.GetRequestId(),
			Reason:      req.GetReason(),
			RequestTime: timestamppb.New(ctx.Now(a)),
		}

		return nil
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
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			outcome := a.Outcome.Get(ctx)
			failure := &failurepb.Failure{
				Message: "Activity canceled",
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{
						Details:  event.details,
						Identity: a.GetCancelState().GetIdentity(),
					},
				},
			}
			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			a.emitOnCanceledMetrics(ctx, event.metricsHandler, event.fromStatus)

			return nil
		})
	},
)

type timeoutEvent struct {
	metricsHandler metrics.Handler
	timeoutType    enumspb.TimeoutType
	retryState     enumspb.RetryState
	fromStatus     activitypb.ActivityExecutionStatus
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
		timeoutType := event.timeoutType

		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			var err error
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = a.recordScheduleToStartOrCloseTimeoutFailure(
					ctx,
					timeoutType,
					fmt.Sprintf(common.FailureReasonActivityTimeout, timeoutType.String()),
				)
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				failure := createStartToCloseTimeoutFailure()
				failure.GetTimeoutFailureInfo().LastHeartbeatDetails = a.lastHeartbeatDetails(ctx)
				err = a.recordFailedAttempt(ctx, 0, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED, failure, ctx.Now(a), true)
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				failure := createHeartbeatTimeoutFailure()
				failure.GetTimeoutFailureInfo().LastHeartbeatDetails = a.lastHeartbeatDetails(ctx)
				err = a.recordFailedAttempt(ctx, 0, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED, failure, ctx.Now(a), true)
			default:
				err = fmt.Errorf("unhandled activity timeout: %v", timeoutType)
			}
			if err != nil {
				return err
			}
			if event.retryState == enumspb.RETRY_STATE_TIMEOUT {
				if err := a.recordScheduleToStartOrCloseTimeoutFailure(
					ctx,
					enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
					common.FailureReasonActivityRetryScheduleToCloseTimeout,
				); err != nil {
					return err
				}
			}

			a.emitOnTimedOutMetrics(ctx, event.metricsHandler, timeoutType, event.fromStatus)

			return nil
		})
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
		a.recordPauseState(ctx, event)
		attempt := a.LastAttempt.Get(ctx)
		attempt.Stamp++
		return nil
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
		a.recordPauseState(ctx, event)
		return nil
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
		a.unpause(ctx, event)
		return nil
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
	resetTime time.Time
	handler   metrics.Handler
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
		a.reset(ctx, event)
		return nil
	},
)

// TransitionResetRequested transitions a STARTED or PAUSE_REQUESTED activity to RESET_REQUESTED.
// PAUSE_REQUESTED is allowed when the operator issues reset with keepPaused=true: ResetKeepPaused is
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
// when the worker yields in RESET_REQUESTED with ResetKeepPaused set (i.e. reset was issued with
// keepPaused=true while the activity was in PAUSE_REQUESTED). The failed attempt is recorded, the
// attempt count is reset to 1, and no dispatch task is emitted — the activity stays paused until
// an explicit unpause.
var TransitionResetAttemptFailedToPaused = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		attempt := a.LastAttempt.Get(ctx)
		a.ResetKeepPaused = false
		a.applyDeferredOptionRestore(ctx)
		a.clearHeartbeatDetails(ctx)
		attempt.Count = 1
		attempt.Stamp++
		if err := a.recordFailedAttempt(ctx, event.retryInterval, event.retryIntervalSource, event.failure, ctx.Now(a), false); err != nil {
			return err
		}
		// Reset discards the retry backoff
		attempt.CurrentRetryInterval = nil
		attempt.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED
		attempt.DispatchTime = nil
		return nil
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
		attempt := a.LastAttempt.Get(ctx)
		currentTime := ctx.Now(a)

		a.ResetKeepPaused = false
		a.applyDeferredOptionRestore(ctx)
		a.clearHeartbeatDetails(ctx)

		attempt.Count = 1
		attempt.Stamp++

		if err := a.recordFailedAttempt(ctx, event.retryInterval, event.retryIntervalSource, event.failure, currentTime, false); err != nil {
			return err
		}
		// Reset discards the retry backoff
		attempt.CurrentRetryInterval = nil
		attempt.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED

		dispatchTime := a.dispatchTimeRespectingStartDelay(currentTime)
		attempt.DispatchTime = timestamppb.New(dispatchTime)
		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: dispatchTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Stamp: attempt.GetStamp(),
				})
		}
		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: dispatchTime,
			},
			a.newActivityDispatchTask(ctx))

		return nil
	},
)
