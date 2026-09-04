package updatelocalexecutionstate

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.UpdateLocalExecutionStateRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.UpdateLocalExecutionStateResponse, error) {
	request := req.GetRequest()
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("update local execution state request is required")
	}
	if request.GetLocalServerId() == "" {
		return nil, serviceerror.NewInvalidArgument("local server ID is required")
	}
	if request.GetFencingEpoch() <= 0 {
		return nil, serviceerror.NewInvalidArgument("fencing epoch must be positive")
	}
	requestedState, err := persistenceState(request.GetState())
	if err != nil {
		return nil, err
	}

	workflowID := request.GetExecution().GetWorkflowId()
	if workflowID == "" || request.GetExecution().GetRunId() == "" {
		return nil, serviceerror.NewInvalidArgument("workflow ID and run ID are required")
	}
	if _, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), workflowID); err != nil {
		return nil, err
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(req.GetNamespaceId(), workflowID, request.GetExecution().GetRunId()),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			localInfo := mutableState.GetExecutionInfo().GetLocalExecutionInfo()
			if localInfo == nil {
				localInfo = &persistencespb.LocalExecutionInfo{}
				mutableState.GetExecutionInfo().LocalExecutionInfo = localInfo
			}
			if err := applyTransition(localInfo, request, requestedState); err != nil {
				return nil, err
			}
			if requestedState == persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE {
				mutableState.PopTasks()
				if err := workflow.NewTaskRefresher(shard).Refresh(ctx, mutableState, false); err != nil {
					return nil, err
				}
			}
			return &api.UpdateWorkflowAction{}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.UpdateLocalExecutionStateResponse{}, nil
}

func persistenceState(
	state adminservice.UpdateLocalExecutionStateRequest_State,
) (persistencespb.LocalExecutionInfo_BridgeState, error) {
	switch state {
	case adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE:
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_RUNNABLE, nil
	case adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED:
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED, nil
	case adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST:
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST, nil
	default:
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_UNSPECIFIED,
			serviceerror.NewInvalidArgument("local execution state is required")
	}
}

func applyTransition(
	localInfo *persistencespb.LocalExecutionInfo,
	request *adminservice.UpdateLocalExecutionStateRequest,
	requestedState persistencespb.LocalExecutionInfo_BridgeState,
) error {
	currentState := localInfo.GetBridgeState()
	if currentState == persistencespb.LocalExecutionInfo_BRIDGE_STATE_UNSPECIFIED {
		localInfo.LocalServerId = request.GetLocalServerId()
		localInfo.FencingEpoch = request.GetFencingEpoch()
	} else if localInfo.GetLocalServerId() != request.GetLocalServerId() ||
		localInfo.GetFencingEpoch() != request.GetFencingEpoch() {
		return serviceerror.NewFailedPrecondition("local execution state owner is fenced")
	}

	if currentState == persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST &&
		requestedState != persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST {
		return serviceerror.NewFailedPrecondition("local execution ownership loss is terminal")
	}
	localInfo.BridgeState = requestedState
	return nil
}
