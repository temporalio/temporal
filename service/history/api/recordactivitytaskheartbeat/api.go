package recordactivitytaskheartbeat

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
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
	request := req.GetHeartbeatRequest()
	tokenSerializer := tasktoken.NewSerializer()
	token, err0 := tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	_, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), token.GetWorkflowId())
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
		token.GetClock(),
		definition.NewWorkflowKey(
			token.GetNamespaceId(),
			token.GetWorkflowId(),
			token.GetRunId(),
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
			if req.GetHeartbeatRequest().GetIdentity() != "" {
				ai.SetRetryLastWorkerIdentity(req.GetHeartbeatRequest().GetIdentity())
			}

			cancelRequested = ai.GetCancelRequested()
			activityPaused = ai.GetPaused()
			activityReset = ai.GetActivityReset()

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

	return historyservice.RecordActivityTaskHeartbeatResponse_builder{
		CancelRequested: cancelRequested,
		ActivityPaused:  activityPaused,
		ActivityReset:   activityReset,
	}.Build(), nil
}
