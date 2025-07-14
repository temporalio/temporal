package refreshworkflow

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (retError error) {
	err := api.ValidateNamespaceUUID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return err
	}

	chasmLease, err := workflowConsistencyChecker.GetChasmLease(
		ctx,
		nil,
		workflowKey,
		chasm.ArchetypeAny, // RefreshWorkflow works for all Archetypes.
		locks.PriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { chasmLease.GetReleaseFn()(retError) }()

	return chasmLease.GetContext().RefreshTasks(ctx, shardContext)
}
