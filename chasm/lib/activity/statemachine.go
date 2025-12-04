package activity

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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

type scheduleEvent struct {
	handler   metrics.Handler
	namespace namespace.Name
	inputSize int
}

// TransitionScheduled transitions to Scheduled status.
var TransitionScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event scheduleEvent) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

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

		recordPayloadSize(event.inputSize, event.handler, event.namespace.String(), metrics.HistoryRecordActivityTaskStartedScope)

		return nil
	},
)

type rescheduleEvent struct {
	retryInterval               time.Duration
	failure                     *failurepb.Failure
	handler                     metrics.Handler
	namespace                   namespace.Name
	breakdownMetricsByTaskQueue dynamicconfig.BoolPropertyFnWithTaskQueueFilter
	timeoutType                 enumspb.TimeoutType
	operationTag                string
}

// TransitionRescheduled transitions to Scheduled from Started, which happens on retries. The event to pass in
// is the failure to be recorded from the previously failed attempt.
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // For retries the activity will be in started status
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		attempt.Count += 1

		err = a.recordFailedAttempt(ctx, event.retryInterval, event.failure, false)
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

		a.recordOnAttemptedMetrics(
			attempt.GetStartedTime().AsTime(),
			event.namespace.String(),
			event.handler,
			event.breakdownMetricsByTaskQueue,
			event.operationTag,
			event.timeoutType)

		return nil
	},
)

// TransitionStarted transitions to Started status
var TransitionStarted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		now := ctx.Now(a)

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: now.Add(a.GetStartToCloseTimeout().AsDuration()),
			},
			&activitypb.StartToCloseTimeoutTask{
				Attempt: attempt.GetCount(),
			})

		if heartbeatTimeout := a.GetHeartbeatTimeout().AsDuration(); heartbeatTimeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: now.Add(heartbeatTimeout),
				},
				&activitypb.HeartbeatTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		return nil
	},
)

// TransitionCompleted transitions to Completed status
var TransitionCompleted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(a *Activity, ctx chasm.MutableContext, reqWithCtx RequestWithContext[*historyservice.RespondActivityTaskCompletedRequest]) error {
		// TODO: after rebase on main, don't need error and add a helper store := a.LoadStore(ctx)
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		req := reqWithCtx.Request

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			attempt.CompleteTime = timestamppb.New(ctx.Now(a))
			attempt.LastWorkerIdentity = req.GetCompleteRequest().GetIdentity()

			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			result := req.GetCompleteRequest().GetResult()

			outcome.Variant = &activitypb.ActivityOutcome_Successful_{
				Successful: &activitypb.ActivityOutcome_Successful{
					Output: result,
				},
			}

			namespaceName := reqWithCtx.NamespaceName.String()

			a.recordOnClosedMetrics(
				attempt.GetStartedTime().AsTime(),
				namespaceName,
				reqWithCtx.MetricsHandler,
				reqWithCtx.BreakdownMetricsByTaskQueue,
				metrics.HistoryRespondActivityTaskCompletedScope,
				enumspb.TIMEOUT_TYPE_UNSPECIFIED)
			recordPayloadSize(result.Size(), reqWithCtx.MetricsHandler, namespaceName, metrics.HistoryRespondActivityTaskCompletedScope)

			return nil
		})
	},
)

// TransitionFailed transitions to Failed status
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(a *Activity, ctx chasm.MutableContext, reqWithCtx RequestWithContext[*historyservice.RespondActivityTaskFailedRequest]) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		req := reqWithCtx.Request

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			if details := req.GetFailedRequest().GetLastHeartbeatDetails(); details != nil {
				heartbeat, err := a.getOrCreateLastHeartbeat(ctx)
				if err != nil {
					return err
				}

				heartbeat.Details = details
				heartbeat.RecordedTime = timestamppb.New(ctx.Now(a))
			}

			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			attempt.LastWorkerIdentity = req.GetFailedRequest().GetIdentity()

			failure := req.GetFailedRequest().GetFailure()

			if err := a.recordFailedAttempt(ctx, 0, failure, true); err != nil {
				return err
			}

			namespaceName := reqWithCtx.NamespaceName.String()
			a.recordOnClosedMetrics(
				attempt.GetStartedTime().AsTime(),
				namespaceName,
				reqWithCtx.MetricsHandler,
				reqWithCtx.BreakdownMetricsByTaskQueue,
				metrics.HistoryRespondActivityTaskFailedScope,
				enumspb.TIMEOUT_TYPE_UNSPECIFIED)
			recordPayloadSize(failure.Size(), reqWithCtx.MetricsHandler, namespaceName, metrics.HistoryRespondActivityTaskFailedScope)

			return nil
		})
	},
)

// TransitionTerminated transitions to Terminated status
var TransitionTerminated = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(a *Activity, ctx chasm.MutableContext, req *activitypb.TerminateActivityExecutionRequest) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			if identity := req.GetFrontendRequest().GetIdentity(); identity != "" {
				attempt.LastWorkerIdentity = identity
			}

			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			failure := &failurepb.Failure{
				Message:     req.GetFrontendRequest().GetReason(),
				FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
			}

			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			return nil
		})
	},
)

// TransitionCancelRequested transitions to CancelRequested status
var TransitionCancelRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED, // Allow idempotent transition
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	func(a *Activity, ctx chasm.MutableContext, req *activitypb.CancelActivityExecutionRequest) error {
		frontendReq := req.GetFrontendRequest()

		a.CancelState = &activitypb.ActivityCancelState{
			Identity:    frontendReq.GetIdentity(),
			RequestId:   req.GetFrontendRequest().GetRequestId(),
			Reason:      frontendReq.GetReason(),
			RequestTime: timestamppb.New(ctx.Now(a)),
		}

		return nil
	},
)

// TransitionCanceled transitions to Canceled status
var TransitionCanceled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
	func(a *Activity, ctx chasm.MutableContext, reqWithCtx RequestWithContext[*historyservice.RespondActivityTaskCanceledRequest]) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			failure := &failurepb.Failure{
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{
						Details: reqWithCtx.Request.GetCancelRequest().GetDetails(),
					},
				},
			}

			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			a.recordOnClosedMetrics(
				attempt.GetStartedTime().AsTime(),
				reqWithCtx.NamespaceName.String(),
				reqWithCtx.MetricsHandler,
				reqWithCtx.BreakdownMetricsByTaskQueue,
				metrics.HistoryRespondActivityTaskCanceledScope,
				enumspb.TIMEOUT_TYPE_UNSPECIFIED)

			return nil
		})
	},
)

type timeoutEvent struct {
	namespaceName               namespace.Name
	metricsHandler              metrics.Handler
	timeoutType                 enumspb.TimeoutType
	breakdownMetricsByTaskQueue dynamicconfig.BoolPropertyFnWithTaskQueueFilter
}

// TransitionTimedOut transitions to TimedOut status
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, event timeoutEvent) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		timeoutType := event.timeoutType

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = a.recordScheduleToStartOrCloseTimeoutFailure(ctx, timeoutType)
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				failure := createStartToCloseTimeoutFailure()
				err = a.recordFailedAttempt(ctx, 0, failure, true)
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				failure := createHeartbeatTimeoutFailure()
				err = a.recordFailedAttempt(ctx, 0, failure, true)
			default:
				return fmt.Errorf("unhandled activity timeout: %v", timeoutType)
			}

			if err != nil {
				return err
			}

			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			a.recordOnClosedMetrics(
				attempt.GetStartedTime().AsTime(),
				event.namespaceName.String(),
				event.metricsHandler,
				event.breakdownMetricsByTaskQueue,
				metrics.TimerActiveTaskActivityTimeoutScope,
				timeoutType)

			return nil
		})
	},
)
