package respondactivitytaskfailed

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.RespondActivityTaskFailedRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RespondActivityTaskFailedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespace := namespaceEntry.Name()

	request := req.FailedRequest
	tokenSerializer := tasktoken.NewSerializer()
	token, err0 := tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}
	if err := api.SetActivityTaskRunID(ctx, token, workflowConsistencyChecker); err != nil {
		return nil, err
	}

	var attemptStartedTime time.Time
	var firstScheduledTime time.Time
	var taskQueue string
	var workflowTypeName string
	var closed bool
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		token.Clock,
		definition.NewWorkflowKey(
			token.NamespaceId,
			token.WorkflowId,
			token.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			workflowTypeName = mutableState.GetWorkflowType().GetName()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			if scheduledEventID == common.EmptyEventID { // client call CompleteActivityById, so get scheduledEventID by activityID
				scheduledEventID, err0 = api.GetActivityScheduledEventID(token.GetActivityId(), mutableState)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, activityRunning := mutableState.GetActivityInfo(scheduledEventID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !activityRunning && scheduledEventID >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(shard.GetMetricsHandler()).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope))
				return nil, consts.ErrStaleState
			}

			if !activityRunning ||
				ai.StartedEventId == common.EmptyEventID ||
				(token.GetScheduledEventId() != common.EmptyEventID && token.Attempt != ai.Attempt) ||
				(token.GetVersion() != common.EmptyVersion && token.Version != ai.Version) {
				return nil, consts.ErrActivityTaskNotFound
			}

			if request.GetLastHeartbeatDetails() != nil {
				// Save heartbeat details as progress
				mutableState.UpdateActivityProgress(ai, &workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: request.GetTaskToken(),
					Details:   request.GetLastHeartbeatDetails(),
					Identity:  request.GetIdentity(),
					Namespace: request.GetNamespace(),
				})
			}

			postActions := &api.UpdateWorkflowAction{}
			failure := request.GetFailure()
			mutableState.RecordLastActivityCompleteTime(ai)
			retryState, err := mutableState.RetryActivity(ai, failure)
			if err != nil {
				return nil, err
			}
			// TODO uncomment once RETRY_STATE_PAUSED is supported
			// if retryState != enumspb.RETRY_STATE_IN_PROGRESS && retryState != enumspb.RETRY_STATE_PAUSED {
			if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
				// no more retry, and we want to record the failure event
				if _, err := mutableState.AddActivityTaskFailedEvent(scheduledEventID, ai.StartedEventId, failure, retryState, request.GetIdentity(), request.GetWorkerVersion()); err != nil {
					// Unable to add ActivityTaskFailed event to history
					return nil, err
				}
				postActions.CreateWorkflowTask = true
				closed = true
			} else {
				closed = false
			}

			attemptStartedTime = ai.StartedTime.AsTime()
			firstScheduledTime = ai.FirstScheduledTime.AsTime()
			taskQueue = ai.TaskQueue
			return postActions, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err == nil {
		completionMetrics := workflow.ActivityCompletionMetrics{
			AttemptStartedTime: attemptStartedTime,
			FirstScheduledTime: firstScheduledTime,
			Status:             workflow.ActivityStatusFailed,
			Closed:             closed,
		}

		workflow.RecordActivityCompletionMetrics(
			shard,
			namespace,
			taskQueue,
			completionMetrics,
			metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
			metrics.WorkflowTypeTag(workflowTypeName),
			metrics.ActivityTypeTag(token.ActivityType),
		)
	}
	return &historyservice.RespondActivityTaskFailedResponse{}, err
}
