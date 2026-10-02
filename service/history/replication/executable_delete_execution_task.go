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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/softassert"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/wideevents"
)

type ExecutableDeleteExecutionTask struct {
	ProcessToolBox

	chasm.ComponentRef
	ExecutableTask

	// lastWriteVersion is the source execution's last write version when it was deleted.
	// It is common.EmptyVersion when the source did not stamp one, i.e. for tasks generated before
	// the version was introduced and for deletions synthesized from another replication task.
	lastWriteVersion int64
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

	// Only take the version from a genuine delete execution replication task. Other replication
	// tasks (sync/verify versioned transition) synthesize a deletion out of their own task, whose
	// version describes a different operation and must not be interpreted as a deletion version.
	lastWriteVersion := common.EmptyVersion
	if rawInfo.GetTaskType() == enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION {
		lastWriteVersion = rawInfo.GetVersion()
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
		lastWriteVersion: lastWriteVersion,
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

	if e.Config.EmitReplicationLifecycleEvents() {
		emitReplicationExecuting(e.ProcessToolBox, e.ReplicationTask(),
			definition.NewWorkflowKey(e.NamespaceID, e.BusinessID, e.RunID),
			wideevents.ReplTaskDeleteExecution, int32(e.Attempt()), e.SourceClusterName(), e.SourceShardKey().ShardID)
	}

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
	namespaceEntry, err := e.NamespaceCache.GetNamespaceByID(namespace.ID(e.NamespaceID))
	if err != nil {
		return err
	}
	currentCluster := e.ClusterMetadata.GetCurrentClusterName()
	// Legacy tasks have no execution-state fence and remain unsafe to apply on an active cluster.
	if e.lastWriteVersion == common.EmptyVersion &&
		namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: e.BusinessID}) == currentCluster {
		e.Logger.Warn("Skipping delete execution replication task on active cluster",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.BusinessID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.TaskID()),
			tag.ClusterName(currentCluster),
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
	if e.lastWriteVersion != common.EmptyVersion {
		targetLastWriteVersion, err := e.getLastWriteVersion(ctx, archetypeID)
		if err != nil {
			return err
		}
		if e.lastWriteVersion != targetLastWriteVersion {
			e.Logger.Warn("Skipping delete execution replication task due to last write version mismatch",
				tag.WorkflowNamespaceID(e.NamespaceID),
				tag.WorkflowID(e.BusinessID),
				tag.WorkflowRunID(e.RunID),
				tag.TaskID(e.TaskID()),
				tag.IncomingVersion(e.lastWriteVersion),
				tag.CurrentVersion(targetLastWriteVersion),
			)
			metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
				1,
				metrics.OperationTag(metrics.DeleteExecutionReplicationTaskScope),
				metrics.NamespaceTag(namespaceName),
			)
			return nil
		}
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

func (e *ExecutableDeleteExecutionTask) getLastWriteVersion(
	ctx context.Context,
	archetypeID chasm.ArchetypeID,
) (_ int64, retError error) {
	namespaceID := namespace.ID(e.NamespaceID)
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(namespaceID, e.BusinessID)
	if err != nil {
		return common.EmptyVersion, err
	}
	workflowContext, release, err := e.WorkflowCache.GetOrCreateChasmExecution(
		ctx,
		shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: e.BusinessID,
			RunId:      e.RunID,
		},
		archetypeID,
		locks.PriorityLow,
	)
	if err != nil {
		return common.EmptyVersion, err
	}
	defer func() { release(retError) }()

	mutableState, err := workflowContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return common.EmptyVersion, err
	}
	return mutableState.GetLastWriteVersion()
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
