package replication

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
)

type ExecutableDeleteExecutionTask struct {
	ProcessToolBox

	definition.WorkflowKey
	ExecutableTask
}

var _ ctasks.Task = (*ExecutableDeleteExecutionTask)(nil)
var _ TrackableExecutableTask = (*ExecutableDeleteExecutionTask)(nil)

func NewExecutableDeleteExecutionTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableDeleteExecutionTask {
	task := replicationTask.GetHistoryTaskAttributes()

	return &ExecutableDeleteExecutionTask{
		ProcessToolBox: processToolBox,
		WorkflowKey:    definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.DeleteExecutionReplicationTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		),
	}
}

func (e *ExecutableDeleteExecutionTask) QueueID() any {
	return e.WorkflowKey
}

func (e *ExecutableDeleteExecutionTask) Execute() error {
	if e.TerminalState() {
		return nil
	}
	e.MarkExecutionStart()

	callerInfo := getReplicaitonCallerInfo(e.GetPriority())
	namespaceName, apply, err := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		callerInfo,
	), e.NamespaceID, e.WorkflowID)
	if err != nil {
		return err
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.TaskID()),
		)
		metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
			1,
			metrics.OperationTag(metrics.DeleteExecutionReplicationTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}

	// Only process workflow archetype deletion for now.
	if e.archetypeID() != chasm.WorkflowArchetypeID {
		return nil
	}

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
	defer cancel()

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}

	_, err = engine.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: e.NamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: e.WorkflowID,
			RunId:      e.RunID,
		},
	})
	return err
}

func (e *ExecutableDeleteExecutionTask) archetypeID() uint32 {
	if rawInfo := e.ReplicationTask().GetRawTaskInfo(); rawInfo != nil {
		return rawInfo.ArchetypeId
	}
	return chasm.UnspecifiedArchetypeID
}

func (e *ExecutableDeleteExecutionTask) HandleErr(err error) error {
	metrics.ReplicationTasksErrorByType.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(metrics.DeleteExecutionReplicationTaskScope),
		metrics.NamespaceTag(e.NamespaceName()),
		metrics.ServiceErrorTypeTag(err),
	)
	switch err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
	default:
		e.Logger.Error("delete execution replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.TaskID()),
			tag.Error(err),
		)
		return fmt.Errorf("delete execution replication task error: %w", err)
	}
}

func (e *ExecutableDeleteExecutionTask) MarkPoisonPill() error {
	if e.ReplicationTask().GetRawTaskInfo() == nil {
		e.ReplicationTask().RawTaskInfo = &persistencespb.ReplicationTaskInfo{
			NamespaceId: e.NamespaceID,
			WorkflowId:  e.WorkflowID,
			RunId:       e.RunID,
			TaskId:      e.TaskID(),
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION,
		}
	}

	return e.ExecutableTask.MarkPoisonPill()
}
