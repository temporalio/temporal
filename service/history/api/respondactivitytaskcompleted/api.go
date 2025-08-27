package respondactivitytaskcompleted

import (
	"context"
	"time"

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
	req *historyservice.RespondActivityTaskCompletedRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespace := namespaceEntry.Name()

	tokenSerializer := tasktoken.NewSerializer()
	request := req.CompleteRequest
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
	var fabricateStartedEvent bool
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
			isCompletedByID := false
			if scheduledEventID == common.EmptyEventID { // client call CompleteActivityById, so get scheduledEventID by activityID
				isCompletedByID = true
				scheduledEventID, err0 = api.GetActivityScheduledEventID(token.GetActivityId(), mutableState)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(shard.GetMetricsHandler()).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope))
				return nil, consts.ErrStaleState
			}

			if !isRunning ||
				(!isCompletedByID && ai.StartedEventId == common.EmptyEventID) ||
				(token.GetScheduledEventId() != common.EmptyEventID && token.Attempt != ai.Attempt) ||
				(token.GetVersion() != common.EmptyVersion && token.Version != ai.Version) {
				return nil, consts.ErrActivityTaskNotFound
			}

			// We fabricate a started event only when the activity is not started yet and
			// we need to force complete an activity
			fabricateStartedEvent = ai.StartedEventId == common.EmptyEventID
			if fabricateStartedEvent {
				_, err := mutableState.AddActivityTaskStartedEvent(
					ai,
					scheduledEventID,
					"",
					req.GetCompleteRequest().GetIdentity(),
					nil,
					nil,
					// TODO (shahab): do we need to do anything with wf redirect in this case or any
					// other case where an activity starts?
					nil,
				)
				if err != nil {
					return nil, err
				}
			}

			ai, _ = mutableState.GetActivityInfo(scheduledEventID)
			if _, err = mutableState.AddActivityTaskCompletedEvent(scheduledEventID, ai.StartedEventId, request); err != nil {
				// Unable to add ActivityTaskCompleted event to history
				return nil, err
			}
			attemptStartedTime = ai.StartedTime.AsTime()
			firstScheduledTime = ai.FirstScheduledTime.AsTime()
			taskQueue = ai.TaskQueue
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)

	if err == nil && !fabricateStartedEvent {
		workflow.RecordActivityCompletionMetrics(
			shard,
			namespace,
			taskQueue,
			workflow.ActivityCompletionMetrics{
				AttemptStartedTime: attemptStartedTime,
				FirstScheduledTime: firstScheduledTime,
				Status:             workflow.ActivityStatusSucceeded,
				Closed:             true,
			},
			metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope),
			metrics.WorkflowTypeTag(workflowTypeName),
			metrics.ActivityTypeTag(token.ActivityType),
		)
	}
	return &historyservice.RespondActivityTaskCompletedResponse{}, err
}
