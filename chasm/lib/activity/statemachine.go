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
		attempt.Count += 1

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToCloseTimeoutTask{})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{
				Attempt: attempt.GetCount(),
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
		attempt.Count += 1

		err := a.recordFailedAttempt(ctx, event.retryInterval, event.failure, currentTime, false)
		if err != nil {
			return err
		}

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(event.retryInterval),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: currentTime.Add(event.retryInterval),
			},
			&activitypb.ActivityDispatchTask{
				Attempt: attempt.GetCount(),
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
				Attempt: a.LastAttempt.Get(ctx).GetCount(),
			})

		if heartbeatTimeout := a.GetHeartbeatTimeout().AsDuration(); heartbeatTimeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: startTime.Add(heartbeatTimeout),
				},
				&activitypb.HeartbeatTimeoutTask{
					Attempt: attempt.GetCount(),
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

// TransitionTerminated transitions to Terminated status.
var TransitionTerminated = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(a *Activity, ctx chasm.MutableContext, event terminateEvent) error {
		return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			req := event.request.GetFrontendRequest()

			a.TerminateState = &activitypb.ActivityTerminateState{
				RequestId: req.GetRequestId(),
			}
			outcome := a.Outcome.Get(ctx)
			failure := &failurepb.Failure{
				// TODO(saa-preview): if the reason isn't provided, perhaps set a default reason. Also see if we should prefix with "Activity terminated: "
				Message:     req.GetReason(),
				FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
			}
			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			metricsHandler := enrichMetricsHandler(
				a,
				event.MetricsHandlerBuilderParams.Handler,
				event.MetricsHandlerBuilderParams.NamespaceName,
				metrics.ActivityTerminatedScope,
				event.MetricsHandlerBuilderParams.BreakdownMetricsByTaskQueue)

			metrics.ActivityTerminate.With(metricsHandler).Record(1)

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
						Details: event.details,
					},
				},
			}
			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

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

			a.emitOnTimedOutMetrics(ctx, event.metricsHandler, timeoutType, event.fromStatus)

			return nil
		})
	},
)
