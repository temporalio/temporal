package polltimeskipping

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionTimeSkippingRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.PollWorkflowExecutionTimeSkippingResponse, retError error) {
	if err := api.ValidateNamespaceUUID(namespace.ID(req.GetNamespaceId())); err != nil {
		return nil, err
	}
	execution := req.GetRequest().GetWorkflowExecution()
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(req.GetNamespaceId(), execution.GetWorkflowId(), execution.GetRunId()),
		locks.PriorityLow, // testing api
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	tsi := workflowLease.GetMutableState().GetExecutionInfo().GetTimeSkippingInfo()
	util := workflow.NewTimeSkippingInfoUtil(tsi)
	ffinfo := util.ToFastForwardInfo()
	response := &workflowservice.PollWorkflowExecutionTimeSkippingResponse{
		FastForwardInfo: ffinfo,
	}

	// if no ff or ffid doesn't match
	if ffinfo == nil || ffinfo.GetFastForwardId() != req.GetRequest().GetFastForwardId() {
		response.Result = workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_NOT_FOUND
		return &historyservice.PollWorkflowExecutionTimeSkippingResponse{Response: response}, nil
	}

	// ffid matches, and ff has completed
	if ffinfo.HasCompleted {
		response.Result = workflowservice.PollWorkflowExecutionTimeSkippingResponse_RESULT_FAST_FORWARD_COMPLETED
		return &historyservice.PollWorkflowExecutionTimeSkippingResponse{Response: response}, nil
	} else {
		// waiting for notification
		// - if time out, set result to timeout
		// - if tsc udpated with new ff-info, set current ff-info and set result to overridden
		// - if tsc ff completes, flip has_completed and set result to completion
		return &historyservice.PollWorkflowExecutionTimeSkippingResponse{Response: response}, nil
	}
}
