package respondactivitytaskcompleted

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
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
	tokenSerializer := tasktoken.NewSerializer()
	request := req.CompleteRequest
	token, err0 := tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	// Handle standalone activity if component ref is present in the token
	if componentRef := token.GetComponentRef(); len(componentRef) > 0 {
		namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), token.ActivityId)
		if err != nil {
			return nil, err
		}
		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).HandleCompleted,
			activity.RespondCompletedEvent{
				Request: req,
				Token:   token,
				MetricsHandlerBuilderParams: activity.MetricsHandlerBuilderParams{
					Handler:                     shard.GetMetricsHandler(),
					NamespaceName:               namespaceEntry.Name().String(),
					BreakdownMetricsByTaskQueue: shard.GetConfig().BreakdownMetricsByTaskQueue,
				},
			},
		)

		if err != nil {
			return nil, err
		}

		return response, nil
	}

	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), token.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceName := namespaceEntry.Name()
	if err := api.SetActivityTaskRunID(ctx, token, workflowConsistencyChecker); err != nil {
		return nil, err
	}

	var attemptStartedTime time.Time
	var firstScheduledTime time.Time
	var taskQueue string
	var workflowTypeName string
	var fabricateStartedEvent bool
	var versioningBehavior enumspb.VersioningBehavior
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

			if !isRunning || api.IsActivityTaskNotFoundForToken(token, ai, &isCompletedByID) {
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
			if !fabricateStartedEvent {
				// leave it zero if the event is fabricated so the latency metrics are not emitted
				attemptStartedTime = ai.StartedTime.AsTime()
			}
			firstScheduledTime = ai.FirstScheduledTime.AsTime()
			taskQueue = ai.TaskQueue
			versioningBehavior = mutableState.GetEffectiveVersioningBehavior()
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)

	if err == nil {
		workflow.RecordActivityCompletionMetrics(
			shard,
			namespaceName,
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
			metrics.VersioningBehaviorTag(versioningBehavior),
		)
	}
	return &historyservice.RespondActivityTaskCompletedResponse{}, err
}
