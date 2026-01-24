package updateactivityoptions

import (
	"context"

	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateActivityOptionsRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UpdateActivityOptionsResponse, retError error) {
	updateRequest := request.GetUpdateRequest()

	mask := updateRequest.GetUpdateMask()
	if mask != nil && updateRequest.GetRestoreOriginal() {
		updateFields := util.ParseFieldMask(mask)
		if len(updateFields) != 0 {
			return nil, serviceerror.NewInvalidArgument("Both UpdateMask and RestoreOriginal are provided")
		}
	}

	validator := api.NewCommandAttrValidator(
		shardContext.GetNamespaceRegistry(),
		shardContext.GetConfig(),
		nil,
	)

	var response *historyservice.UpdateActivityOptionsResponse

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			updateRequest.GetExecution().GetWorkflowId(),
			updateRequest.GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			var err error
			if updateRequest.GetRestoreOriginal() {
				response, err = restoreOriginalOptions(ctx, mutableState, updateRequest)
			} else {
				response, err = processActivityOptionsRequest(validator, mutableState, updateRequest, request.GetNamespaceId())
			}

			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return response, err
}

func processActivityOptionsRequest(
	validator *api.CommandAttrValidator,
	mutableState historyi.MutableState,
	updateRequest *workflowservice.UpdateActivityOptionsRequest,
	namespaceID string,
) (*historyservice.UpdateActivityOptionsResponse, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	mergeFrom := updateRequest.GetActivityOptions()
	if mergeFrom == nil {
		return nil, serviceerror.NewInvalidArgument("ActivityOptions are not provided")
	}

	activityIDs := getActivityIDs(updateRequest, mutableState)

	if len(activityIDs) == 0 {
		return nil, consts.ErrActivityNotFound
	}

	mask := updateRequest.GetUpdateMask()
	if mask == nil {
		return nil, serviceerror.NewInvalidArgument("UpdateMask is not provided")
	}

	updateFields := util.ParseFieldMask(mask)

	var adjustedOptions *activitypb.ActivityOptions
	var err error
	for _, activityId := range activityIDs {
		ai, activityFound := mutableState.GetActivityByActivityID(activityId)

		if !activityFound {
			return nil, consts.ErrActivityNotFound
		}

		if adjustedOptions, err = processActivityOptionsUpdate(validator, mutableState, namespaceID, ai, mergeFrom, updateFields); err != nil {
			return nil, err
		}
	}

	// fill the response
	response := historyservice.UpdateActivityOptionsResponse_builder{
		ActivityOptions: adjustedOptions,
	}.Build()
	return response, nil
}

func processActivityOptionsUpdate(
	validator *api.CommandAttrValidator,
	mutableState historyi.MutableState,
	namespaceID string,
	ai *persistencespb.ActivityInfo,
	mergeFrom *activitypb.ActivityOptions,
	updateFields map[string]struct{},
) (*activitypb.ActivityOptions, error) {

	mergeInto := activitypb.ActivityOptions_builder{
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: ai.GetTaskQueue(),
		}.Build(),
		ScheduleToCloseTimeout: ai.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: ai.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    ai.GetStartToCloseTimeout(),
		HeartbeatTimeout:       ai.GetHeartbeatTimeout(),
		Priority:               common.CloneProto(ai.GetPriority()),
		RetryPolicy: commonpb.RetryPolicy_builder{
			BackoffCoefficient: ai.GetRetryBackoffCoefficient(),
			InitialInterval:    ai.GetRetryInitialInterval(),
			MaximumInterval:    ai.GetRetryMaximumInterval(),
			MaximumAttempts:    ai.GetRetryMaximumAttempts(),
		}.Build(),
	}.Build()

	// update activity options
	if err := mergeActivityOptions(mergeInto, mergeFrom, updateFields); err != nil {
		return nil, err
	}

	// validate the updated options
	adjustedOptions, err := adjustActivityOptions(validator, namespaceID, ai.GetActivityId(), ai.GetActivityType(), mergeInto)
	if err != nil {
		return nil, err
	}

	return updateActivityOptions(mutableState, ai, adjustedOptions)
}

func mergeActivityOptions(
	mergeInto *activitypb.ActivityOptions,
	mergeFrom *activitypb.ActivityOptions,
	updateFields map[string]struct{},
) error {

	if _, ok := updateFields["taskQueue.name"]; ok {
		if !mergeFrom.HasTaskQueue() {
			return serviceerror.NewInvalidArgument("TaskQueue is not provided")
		}
		if !mergeInto.HasTaskQueue() {
			mergeInto.SetTaskQueue(mergeFrom.GetTaskQueue())
		}
		mergeInto.GetTaskQueue().SetName(mergeFrom.GetTaskQueue().GetName())
	}

	if _, ok := updateFields["scheduleToCloseTimeout"]; ok {
		mergeInto.SetScheduleToCloseTimeout(mergeFrom.GetScheduleToCloseTimeout())
	}

	if _, ok := updateFields["scheduleToStartTimeout"]; ok {
		mergeInto.SetScheduleToStartTimeout(mergeFrom.GetScheduleToStartTimeout())
	}

	if _, ok := updateFields["startToCloseTimeout"]; ok {
		mergeInto.SetStartToCloseTimeout(mergeFrom.GetStartToCloseTimeout())
	}

	if _, ok := updateFields["heartbeatTimeout"]; ok {
		mergeInto.SetHeartbeatTimeout(mergeFrom.GetHeartbeatTimeout())
	}

	if _, ok := updateFields["priority"]; ok {
		mergeInto.SetPriority(mergeFrom.GetPriority())
	}

	if _, ok := updateFields["priority.priorityKey"]; ok {
		if !mergeFrom.HasPriority() {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if !mergeInto.HasPriority() {
			mergeInto.SetPriority(&commonpb.Priority{})
		}
		mergeInto.GetPriority().SetPriorityKey(mergeFrom.GetPriority().GetPriorityKey())
	}

	if _, ok := updateFields["priority.fairnessKey"]; ok {
		if !mergeFrom.HasPriority() {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if !mergeInto.HasPriority() {
			mergeInto.SetPriority(&commonpb.Priority{})
		}
		mergeInto.GetPriority().SetFairnessKey(mergeFrom.GetPriority().GetFairnessKey())
	}

	if _, ok := updateFields["priority.fairnessWeight"]; ok {
		if !mergeFrom.HasPriority() {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if !mergeInto.HasPriority() {
			mergeInto.SetPriority(&commonpb.Priority{})
		}
		mergeInto.GetPriority().SetFairnessWeight(mergeFrom.GetPriority().GetFairnessWeight())
	}

	if !mergeInto.HasRetryPolicy() {
		mergeInto.SetRetryPolicy(&commonpb.RetryPolicy{})
	}

	if _, ok := updateFields["retryPolicy"]; ok {
		mergeInto.SetRetryPolicy(mergeFrom.GetRetryPolicy())
	}

	if _, ok := updateFields["retryPolicy.initialInterval"]; ok {
		if !mergeFrom.HasRetryPolicy() {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.GetRetryPolicy().SetInitialInterval(mergeFrom.GetRetryPolicy().GetInitialInterval())
	}

	if _, ok := updateFields["retryPolicy.backoffCoefficient"]; ok {
		if !mergeFrom.HasRetryPolicy() {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.GetRetryPolicy().SetBackoffCoefficient(mergeFrom.GetRetryPolicy().GetBackoffCoefficient())
	}

	if _, ok := updateFields["retryPolicy.maximumInterval"]; ok {
		if !mergeFrom.HasRetryPolicy() {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.GetRetryPolicy().SetMaximumInterval(mergeFrom.GetRetryPolicy().GetMaximumInterval())
	}
	if _, ok := updateFields["retryPolicy.maximumAttempts"]; ok {
		if !mergeFrom.HasRetryPolicy() {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.GetRetryPolicy().SetMaximumAttempts(mergeFrom.GetRetryPolicy().GetMaximumAttempts())
	}

	return nil
}

func adjustActivityOptions(
	validator *api.CommandAttrValidator,
	namespaceID string,
	activityID string,
	activityType *commonpb.ActivityType,
	ao *activitypb.ActivityOptions,
) (*activitypb.ActivityOptions, error) {
	attributes := commandpb.ScheduleActivityTaskCommandAttributes_builder{
		TaskQueue:              ao.GetTaskQueue(),
		ScheduleToCloseTimeout: ao.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: ao.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    ao.GetStartToCloseTimeout(),
		HeartbeatTimeout:       ao.GetHeartbeatTimeout(),
		ActivityId:             activityID,
		ActivityType:           activityType,
	}.Build()

	_, err := validator.ValidateActivityScheduleAttributes(namespace.ID(namespaceID), attributes, nil)
	if err != nil {
		return nil, err
	}

	ao.SetScheduleToCloseTimeout(attributes.GetScheduleToCloseTimeout())
	ao.SetScheduleToStartTimeout(attributes.GetScheduleToStartTimeout())
	ao.SetStartToCloseTimeout(attributes.GetStartToCloseTimeout())
	ao.SetHeartbeatTimeout(attributes.GetHeartbeatTimeout())

	return ao, nil
}

func getActivityIDs(updateRequest *workflowservice.UpdateActivityOptionsRequest, ms historyi.MutableState) []string {
	var activityIDs []string
	switch updateRequest.WhichActivity() {
	case workflowservice.UpdateActivityOptionsRequest_Id_case:
		activityIDs = append(activityIDs, updateRequest.GetId())
	case workflowservice.UpdateActivityOptionsRequest_Type_case:
		activityType := updateRequest.GetType()
		for _, ai := range ms.GetPendingActivityInfos() {
			if ai.GetActivityType().GetName() == activityType {
				activityIDs = append(activityIDs, ai.GetActivityId())
			}
		}
	}
	return activityIDs
}

func updateActivityOptions(
	ms historyi.MutableState,
	ai *persistencespb.ActivityInfo,
	activityOptions *activitypb.ActivityOptions,
) (*activitypb.ActivityOptions, error) {
	var err error
	if err = ms.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		// update activity info with new options
		activityInfo.SetTaskQueue(activityOptions.GetTaskQueue().GetName())
		activityInfo.SetScheduleToCloseTimeout(activityOptions.GetScheduleToCloseTimeout())
		activityInfo.SetScheduleToStartTimeout(activityOptions.GetScheduleToStartTimeout())
		activityInfo.SetStartToCloseTimeout(activityOptions.GetStartToCloseTimeout())
		activityInfo.SetHeartbeatTimeout(activityOptions.GetHeartbeatTimeout())
		activityInfo.SetPriority(activityOptions.GetPriority())
		activityInfo.SetRetryMaximumInterval(activityOptions.GetRetryPolicy().GetMaximumInterval())
		activityInfo.SetRetryBackoffCoefficient(activityOptions.GetRetryPolicy().GetBackoffCoefficient())
		activityInfo.SetRetryInitialInterval(activityOptions.GetRetryPolicy().GetInitialInterval())
		activityInfo.SetRetryMaximumAttempts(activityOptions.GetRetryPolicy().GetMaximumAttempts())

		// move forward activity version
		activityInfo.SetStamp(activityInfo.GetStamp() + 1)

		// invalidate timers
		activityInfo.SetTimerTaskStatus(workflow.TimerTaskStatusNone)
		return nil
	}); err != nil {
		return nil, err
	}

	if workflow.GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// in this case we always want to generate a new retry task

		// two options - activity can be in backoff, or scheduled (waiting to be started)
		// if activity in backoff
		// 		in this case there is already old retry task
		// 		it will be ignored because of stamp mismatch
		// if activity is scheduled and waiting to be started
		// 		eventually matching service will call history service (recordActivityTaskStarted)
		// 		history service will return error based on stamp. Task will be dropped

		nextScheduledTime := workflow.GetNextScheduledTime(ai)
		err = ms.RegenerateActivityRetryTask(ai, nextScheduledTime)
		if err != nil {
			return nil, err
		}
	}

	return activityOptions, nil

}

func restoreOriginalOptions(
	ctx context.Context,
	ms historyi.MutableState,
	updateRequest *workflowservice.UpdateActivityOptionsRequest,
) (*historyservice.UpdateActivityOptionsResponse, error) {

	activityIDs := getActivityIDs(updateRequest, ms)

	if len(activityIDs) == 0 {
		return nil, consts.ErrActivityNotFound
	}

	var updatedOptions *activitypb.ActivityOptions

	for _, activityId := range activityIDs {
		ai, activityFound := ms.GetActivityByActivityID(activityId)

		if !activityFound {
			return nil, consts.ErrActivityNotFound
		}

		event, err := ms.GetActivityScheduledEvent(ctx, ai.GetScheduledEventId())
		if err != nil {
			return nil, err
		}
		if event.WhichAttributes() != historypb.HistoryEvent_ActivityTaskScheduledEventAttributes_case {
			return nil, serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is invalid")
		}
		originalOptions := event.GetActivityTaskScheduledEventAttributes()
		if originalOptions == nil {
			return nil, serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is incomplete")
		}

		activityOptions := activitypb.ActivityOptions_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: originalOptions.GetTaskQueue().GetName(),
			}.Build(),
			ScheduleToCloseTimeout: originalOptions.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: originalOptions.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    originalOptions.GetStartToCloseTimeout(),
			HeartbeatTimeout:       originalOptions.GetHeartbeatTimeout(),
			Priority:               originalOptions.GetPriority(),
			RetryPolicy:            originalOptions.GetRetryPolicy(),
		}.Build()

		if updatedOptions, err = updateActivityOptions(ms, ai, activityOptions); err != nil {
			return nil, err
		}

	}

	return historyservice.UpdateActivityOptionsResponse_builder{
		ActivityOptions: updatedOptions,
	}.Build(), nil
}
