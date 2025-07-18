package describemutablestate

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.DescribeMutableStateRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	chasmLease, err := workflowConsistencyChecker.GetChasmLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.Execution.WorkflowId,
			req.Execution.RunId,
		),
		chasm.ArchetypeAny, // DescribeMutableState works for all Archetypes.
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { chasmLease.GetReleaseFn()(retError) }()

	response := &historyservice.DescribeMutableStateResponse{}
	if chasmLease.GetContext().(*workflow.ContextImpl).MutableState != nil {
		msb := chasmLease.GetContext().(*workflow.ContextImpl).MutableState
		response.CacheMutableState = msb.CloneToProto()
	}

	// clear mutable state to force reload from persistence. This API returns both cached and persisted version.
	chasmLease.GetContext().Clear()
	mutableState, err := chasmLease.GetContext().LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	response.DatabaseMutableState = mutableState.CloneToProto()
	return response, nil
}
