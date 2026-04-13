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
		currentTime := ctx.Now(a)
		attempt.Count++
		attempt.Stamp++

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Stamp: attempt.GetStamp(),
				})
		}

		if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
			a.Stamp++
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToCloseTimeoutTask{Stamp: a.GetStamp()})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{
				Stamp: attempt.GetStamp(),
			})

		return nil
	},
)

type rescheduleEvent struct {
	retryInterval time.Duration
	failure       *failurepb.Failure
	timeoutType   enumspb.TimeoutType
}

// TransitionRescheduled transitions to Scheduled from Started, which happens on retries. The event
// to pass in is the failure to be recorded from the previously failed attempt.
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // For retries the activity will be in started status
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		attempt := a.LastAttempt.Get(ctx)
		currentTime := ctx.Now(a)

		// Apply deferred reset: set Count to 0 so the increment below produces 1.
		if a.ActivityReset {
			attempt.Count = 0
			a.ActivityReset = false
			if a.ResetHeartbeats {
				a.ResetHeartbeats = false
				if hb, ok := a.LastHeartbeat.TryGet(ctx); ok {
					hb.Details = nil
					hb.RecordedTime = nil
				}
			}
		}

		attempt.Count++
		attempt.Stamp++

		err := a.recordFailedAttempt(ctx, event.retryInterval, event.failure, currentTime, false)
		if err != nil {
			return err
		}

		retryScheduledTime := attemptScheduleTimeForRetry(attempt).AsTime()

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
			&activitypb.ActivityDispatchTask{
				Stamp: attempt.GetStamp(),
			})

		return nil
	},
)

// TransitionStarted transitions to Started status.
var TransitionStarted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) error {
		attempt := a.LastAttempt.Get(ctx)
		attempt.StartedTime = timestamppb.New(ctx.Now(a))
		attempt.StartRequestId = request.GetRequestId()
		attempt.LastWorkerIdentity = request.GetPollRequest().GetIdentity()
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
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(a *Activity, ctx chasm.MutableContext, event completeEvent) error {
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			a.PauseState = nil

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
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(a *Activity, ctx chasm.MutableContext, event failedEvent) error {
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			req := event.req.GetFailedRequest()
			a.PauseState = nil

			if details := req.GetLastHeartbeatDetails(); details != nil {
				heartbeat := a.getOrCreateLastHeartbeat(ctx)
				heartbeat.Details = details
				heartbeat.RecordedTime = timestamppb.New(ctx.Now(a))
			}
			attempt := a.LastAttempt.Get(ctx)
			attempt.LastWorkerIdentity = req.GetIdentity()

			if err := a.recordFailedAttempt(ctx, 0, req.GetFailure(), ctx.Now(a), true); err != nil {
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
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(a *Activity, ctx chasm.MutableContext, event terminateEvent) error {
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			a.TerminateState = &activitypb.ActivityTerminateState{
				RequestId: event.request.RequestID,
			}
			a.PauseState = nil
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
// PAUSED activities (real status, no worker) are cancelled immediately in handleCancellationRequested
// rather than waiting for a worker response.
var TransitionCancelRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
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
	details    *commonpb.Payloads
	handler    metrics.Handler
	fromStatus activitypb.ActivityExecutionStatus
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
			a.PauseState = nil

			a.emitOnCanceledMetrics(ctx, event.handler, event.fromStatus)

			return nil
		})
	},
)

type timeoutEvent struct {
	metricsHandler metrics.Handler
	timeoutType    enumspb.TimeoutType
	fromStatus     activitypb.ActivityExecutionStatus
}

// TransitionTimedOut transitions to TimedOut status.
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, event timeoutEvent) error {
		timeoutType := event.timeoutType

		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			var err error
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = a.recordScheduleToStartOrCloseTimeoutFailure(ctx, timeoutType)
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				failure := createStartToCloseTimeoutFailure()
				err = a.recordFailedAttempt(ctx, 0, failure, ctx.Now(a), true)
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				failure := createHeartbeatTimeoutFailure()
				err = a.recordFailedAttempt(ctx, 0, failure, ctx.Now(a), true)
			default:
				err = fmt.Errorf("unhandled activity timeout: %v", timeoutType)
			}
			if err != nil {
				return err
			}

			a.PauseState = nil

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
//
// Note: STARTED activities are NOT paused via this transition. Pausing a STARTED activity is a
// flag-only operation (PauseState is set, status stays STARTED) so the worker's token remains
// valid and the worker is notified via ActivityPaused=true on its next heartbeat. See
// handlePauseRequested for the full hybrid logic.
var TransitionPaused = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	func(a *Activity, ctx chasm.MutableContext, event pauseEvent) error {
		a.pause(ctx, event)
		attempt := a.LastAttempt.Get(ctx)
		attempt.Stamp++
		return nil
	},
)

type unpauseEvent struct {
	req            *workflowservice.UnpauseActivityExecutionRequest
	metricsHandler metrics.Handler
}

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

type resetEvent struct {
	scheduleTime    time.Time
	resetHeartbeats bool
}

// TransitionReset resets a SCHEDULED activity back to attempt 1. The stamp is bumped to invalidate
// any pending dispatch task, then a new dispatch task is added at the given schedule time.
// For STARTED/CANCEL_REQUESTED activities the reset is deferred — see Activity.ActivityReset flag.
var TransitionReset = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event resetEvent) error {
		attempt := a.LastAttempt.Get(ctx)
		attempt.Count = 1
		attempt.Stamp++
		if event.resetHeartbeats {
			if hb, ok := a.LastHeartbeat.TryGet(ctx); ok {
				hb.Details = nil
				hb.RecordedTime = nil
			}
		}
		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{ScheduledTime: event.scheduleTime.Add(timeout)},
				&activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()},
			)
		}
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: event.scheduleTime},
			&activitypb.ActivityDispatchTask{Stamp: attempt.GetStamp()},
		)
		return nil
	},
)
