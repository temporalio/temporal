package replication

import (
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableWorkflowTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.ReplicateWorkflowStateRequest

		// variables to be perhaps removed (not essential to logic)
		sourceClusterName string
	}
)

var _ ctasks.Task = (*ExecutableWorkflowTask)(nil)
var _ TrackableExecutableTask = (*ExecutableWorkflowTask)(nil)

// TODO should workflow task be batched?

func NewExecutableWorkflowTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncWorkflowStateTaskAttributes,
	sourceClusterName string,
) *ExecutableWorkflowTask {
	namespaceID := task.GetWorkflowState().ExecutionInfo.NamespaceId
	workflowID := task.GetWorkflowState().ExecutionInfo.WorkflowId
	runID := task.GetWorkflowState().ExecutionState.RunId
	return &ExecutableWorkflowTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(namespaceID, workflowID, runID),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncWorkflowTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		req: &historyservice.ReplicateWorkflowStateRequest{
			NamespaceId:   namespaceID,
			WorkflowState: task.GetWorkflowState(),
			RemoteCluster: sourceClusterName,
		},

		sourceClusterName: sourceClusterName,
	}
}

func (e *ExecutableWorkflowTask) Execute() error {
	namespaceName, apply, err := e.GetNamespaceInfo(e.NamespaceID)
	if err != nil {
		return err
	} else if !apply {
		return nil
	}
	ctx, cancel := newTaskContext(namespaceName)
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
	return engine.ReplicateWorkflowState(ctx, e.req)
}

func (e *ExecutableWorkflowTask) HandleErr(err error) error {
	// TODO original logic does not seem to have resend logic?
	return err
}
