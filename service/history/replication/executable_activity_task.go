package replication

import (
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableActivityTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.SyncActivityRequest

		// variables to be perhaps removed (not essential to logic)
		sourceClusterName string
	}
)

var _ ctasks.Task = (*ExecutableActivityTask)(nil)
var _ TrackableExecutableTask = (*ExecutableActivityTask)(nil)

func NewExecutableActivityTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncActivityTaskAttributes,
	sourceClusterName string,
) *ExecutableActivityTask {
	return &ExecutableActivityTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncActivityTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		req: &historyservice.SyncActivityRequest{
			NamespaceId:        task.NamespaceId,
			WorkflowId:         task.WorkflowId,
			RunId:              task.RunId,
			Version:            task.Version,
			ScheduledEventId:   task.ScheduledEventId,
			ScheduledTime:      task.ScheduledTime,
			StartedEventId:     task.StartedEventId,
			StartedTime:        task.StartedTime,
			LastHeartbeatTime:  task.LastHeartbeatTime,
			Details:            task.Details,
			Attempt:            task.Attempt,
			LastFailure:        task.LastFailure,
			LastWorkerIdentity: task.LastWorkerIdentity,
			VersionHistory:     task.GetVersionHistory(),
		},

		sourceClusterName: sourceClusterName,
	}
}

func (e *ExecutableActivityTask) Execute() error {
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
	return engine.SyncActivity(ctx, e.req)
}

func (e *ExecutableActivityTask) HandleErr(err error) error {
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
