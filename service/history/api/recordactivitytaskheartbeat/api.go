package recordactivitytaskheartbeat

import (
	"context"

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
)

func Invoke(
	ctx context.Context,
	req *historyservice.RecordActivityTaskHeartbeatRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {
	request := req.HeartbeatRequest
	tokenSerializer := tasktoken.NewSerializer()
	token, err0 := tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	// Handle as standalone activity if token has component ref.
	if componentRef := token.GetComponentRef(); len(componentRef) > 0 {
		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).RecordHeartbeat,
			activity.WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
				Token:   token,
				Request: req,
			},
		)
		return response, err
	}

	_, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), token.WorkflowId)
	if err != nil {
		return nil, err
	}
	if err := api.SetActivityTaskRunID(ctx, token, workflowConsistencyChecker); err != nil {
		return nil, err
	}

	var cancelRequested bool
	var activityPaused bool
	var activityReset bool
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
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			if scheduledEventID == common.EmptyEventID { // client call RecordActivityHeartbeatByID, so get scheduledEventID by activityID
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
					metrics.OperationTag(metrics.HistoryRecordActivityTaskHeartbeatScope))
				return nil, consts.ErrStaleState
			}

			if !isRunning || api.IsActivityTaskNotFoundForToken(token, ai, nil) {
				return nil, consts.ErrActivityTaskNotFound
			}

			// update worker identity if available
			if req.HeartbeatRequest.Identity != "" {
				ai.RetryLastWorkerIdentity = req.HeartbeatRequest.Identity
			}

			cancelRequested = ai.CancelRequested
			activityPaused = ai.Paused
			activityReset = ai.ActivityReset

			// Save progress and last HB reported time.
			mutableState.UpdateActivityProgress(ai, request)

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	return &historyservice.RecordActivityTaskHeartbeatResponse{
		CancelRequested: cancelRequested,
		ActivityPaused:  activityPaused,
		ActivityReset:   activityReset,
	}, nil
}
