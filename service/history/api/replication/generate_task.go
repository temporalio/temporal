package replication

import (
	"context"
	rtdebug "runtime/debug"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func GenerateTask(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {
	namespaceEntry, err := api.GetNamespace(shardContext, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	archetypeID := request.GetArchetypeId()
	if archetypeID == chasm.UnspecifiedArchetypeID {
		archetypeID = chasm.WorkflowArchetypeID
	}

	chasmLease, err := workflowConsistencyChecker.GetChasmLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			request.Execution.WorkflowId,
			request.Execution.RunId,
		),
		archetypeID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { chasmLease.GetReleaseFn()(retError) }()

	mutableState := chasmLease.GetMutableState()
	replicationTasks, stateTransitionCount, err := mutableState.GenerateMigrationTasks(request.GetTargetClusters())
	if err != nil {
		return nil, err
	}

	// ADDTASKS_DEBUG (temporary, malik): rule-out marker. This is the in-process migration/force-
	// replication task generator writing replication tasks to the source shard. If the source-side
	// task-write delta is driven by generation rather than the backfill RPC, this fires.
	shardContext.GetLogger().Info("ADDTASKS_DEBUG generate migration tasks -> AddTasks",
		tag.NewInt32("shard-id", shardContext.GetShardID()),
		tag.NewStringTag("workflow-id", request.Execution.WorkflowId),
		tag.NewInt("repl-task-count", len(replicationTasks)),
		tag.SysStackTrace(string(rtdebug.Stack())),
	)

	err = shardContext.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID: shardContext.GetShardID(),
		// RangeID is set by shard
		NamespaceID: string(namespaceID),
		WorkflowID:  request.Execution.WorkflowId,
		ArchetypeID: archetypeID,
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: replicationTasks,
		},
	})
	if err != nil {
		return nil, err
	}

	historyLength := max(mutableState.GetNextEventID()-1, 0)
	return &historyservice.GenerateLastHistoryReplicationTasksResponse{
		StateTransitionCount: stateTransitionCount,
		HistoryLength:        historyLength,
	}, nil
}
