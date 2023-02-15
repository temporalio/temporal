package replication

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableHistoryTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.ReplicateEventsV2Request

		// variables to be perhaps removed (not essential to logic)
		sourceClusterName string
	}
)

var _ ctasks.Task = (*ExecutableHistoryTask)(nil)
var _ TrackableExecutableTask = (*ExecutableHistoryTask)(nil)

func NewExecutableHistoryTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.HistoryTaskAttributes,
	sourceClusterName string,
) *ExecutableHistoryTask {
	return &ExecutableHistoryTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.HistoryReplicationTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		req: &historyservice.ReplicateEventsV2Request{
			NamespaceId: task.NamespaceId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowId,
				RunId:      task.RunId,
			},
			VersionHistoryItems: task.VersionHistoryItems,
			Events:              task.Events,
			// new run events does not need version history since there is no prior events
			NewRunEvents: task.NewRunEvents,
		},

		sourceClusterName: sourceClusterName,
	}
}

func (e *ExecutableHistoryTask) Execute() error {
	namespaceName, apply, nsError := e.GetNamespaceInfo(e.NamespaceID)
	if nsError != nil {
		return nsError
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
	return engine.ReplicateEventsV2(ctx, e.req)
}

func (e *ExecutableHistoryTask) HandleErr(err error) error {
	switch retryErr := err.(type) {
	case nil:
		return nil
	case *serviceerrors.RetryReplication:
		namespaceName, _, nsError := e.GetNamespaceInfo(e.NamespaceID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName)
		defer cancel()

		if resendErr := e.Resend(
			ctx,
			e.sourceClusterName,
			e.ProcessToolBox,
			retryErr,
		); resendErr != nil {
			return err
		}
		return e.Execute()
	default:
		return err
	}
}
