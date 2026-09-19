package disabletimeskipping

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.DisableTimeSkippingRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.DisableTimeSkippingResponse, retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}
	if _, err := api.GetActiveNamespace(shardContext, namespaceID, req.GetExecution().GetWorkflowId()); err != nil {
		return nil, err
	}

	archetypeID := chasm.ArchetypeID(req.GetArchetypeId())
	if archetypeID == chasm.UnspecifiedArchetypeID {
		archetypeID = chasm.WorkflowArchetypeID
	}

	lease, err := workflowConsistencyChecker.GetChasmLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			req.GetNamespaceId(),
			req.GetExecution().GetWorkflowId(),
			req.GetExecution().GetRunId(),
		),
		archetypeID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { lease.GetReleaseFn()(retError) }()

	if !lease.GetMutableState().DisableTimeSkipping() {
		return &historyservice.DisableTimeSkippingResponse{}, nil
	}
	if err := lease.GetContext().UpdateWorkflowExecutionAsActive(ctx, shardContext); err != nil {
		return nil, err
	}
	shardContext.GetLogger().Info(
		"Virtual time skipping disabled by admin",
		tag.WorkflowNamespaceID(req.GetNamespaceId()),
		tag.WorkflowID(req.GetExecution().GetWorkflowId()),
		tag.WorkflowRunID(req.GetExecution().GetRunId()),
		tag.ArchetypeID(uint32(archetypeID)),
	)
	return &historyservice.DisableTimeSkippingResponse{Disabled: true}, nil
}
