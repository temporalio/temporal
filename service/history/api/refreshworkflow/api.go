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
	archetypeID chasm.ArchetypeID,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (retError error) {
	err := api.ValidateNamespaceUUID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return err
	}

	if archetypeID == chasm.UnspecifiedArchetypeID {
		archetypeID = chasm.WorkflowArchetypeID
	}

	chasmLease, err := workflowConsistencyChecker.GetChasmLease(
		ctx,
		nil,
		workflowKey,
		archetypeID,
		locks.PriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { chasmLease.GetReleaseFn()(retError) }()

	return chasmLease.GetContext().RefreshTasks(ctx, shardContext)
}
