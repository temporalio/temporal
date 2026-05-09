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
	"go.temporal.io/server/common/softassert"
	ctasks "go.temporal.io/server/common/tasks"
)

type ExecutableDeleteExecutionTask struct {
	ProcessToolBox

	chasm.ComponentRef
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
	rawInfo := replicationTask.GetRawTaskInfo()

	// ArchetypeID should never be unspecified. Default to WorkflowArchetypeID.
	archetypeID := chasm.WorkflowArchetypeID
	if rawInfo != nil && rawInfo.ArchetypeId != chasm.UnspecifiedArchetypeID {
		archetypeID = rawInfo.ArchetypeId
	} else {
		softassert.That(processToolBox.Logger, false, "delete execution replication task has unspecified archetype ID")
	}

	return &ExecutableDeleteExecutionTask{
		ProcessToolBox: processToolBox,
		ComponentRef: chasm.NewComponentRefByArchetypeID(
			chasm.ExecutionKey{
				NamespaceID: rawInfo.GetNamespaceId(),
				BusinessID:  rawInfo.GetWorkflowId(),
				RunID:       rawInfo.GetRunId(),
			},
			archetypeID,
		),
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
	return definition.NewWorkflowKey(e.NamespaceID, e.BusinessID, e.RunID)
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
	), e.NamespaceID, e.BusinessID)
	if err != nil {
		return err
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.BusinessID),
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

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
	defer cancel()

	archetypeID, err := e.ArchetypeID(e.ChasmRegistry)
	if err != nil {
		return err
	}
	switch archetypeID {
	case chasm.WorkflowArchetypeID:
		return e.deleteWorkflowExecution(ctx)
	default:
		return e.deleteChasmExecution(ctx)
	}
}

func (e *ExecutableDeleteExecutionTask) deleteWorkflowExecution(ctx context.Context) error {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.BusinessID,
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
			WorkflowId: e.BusinessID,
			RunId:      e.RunID,
		},
	})
	return err
}

func (e *ExecutableDeleteExecutionTask) deleteChasmExecution(ctx context.Context) error {
	return e.ChasmEngine.DeleteExecution(ctx, e.ComponentRef, chasm.DeleteExecutionRequest{})
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
			tag.WorkflowID(e.BusinessID),
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
			WorkflowId:  e.BusinessID,
			RunId:       e.RunID,
			TaskId:      e.TaskID(),
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION,
		}
	}

	return e.ExecutableTask.MarkPoisonPill()
}
