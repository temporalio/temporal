package history

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	outboundTaskTimeout = time.Second * 10 * debug.TimeoutMultiplier
)

type outboundQueueActiveTaskExecutor struct {
	stateMachineEnvironment
	chasmEngine chasm.Engine
}

var _ queues.Executor = &outboundQueueActiveTaskExecutor{}

func newOutboundQueueActiveTaskExecutor(
	shardCtx historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricsHandler metrics.Handler,
	chasmEngine chasm.Engine,
) *outboundQueueActiveTaskExecutor {
	return &outboundQueueActiveTaskExecutor{
		stateMachineEnvironment: stateMachineEnvironment{
			shardContext: shardCtx,
			cache:        workflowCache,
			logger:       logger,
			metricsHandler: metricsHandler.WithTags(
				metrics.OperationTag(metrics.OperationOutboundQueueProcessorScope),
			),
		},
		chasmEngine: chasmEngine,
	}
}

func (e *outboundQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		e.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	taskType := queues.GetOutboundTaskTypeTagValue(task, true, e.shardContext.ChasmRegistry())
	respond := func(err error) queues.ExecuteResponse {
		metricsTags := []metrics.Tag{
			namespaceTag,
			metrics.TaskTypeTag(taskType),
			metrics.OperationTag(taskType),
		}
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        err,
		}
	}

	// We don't want to execute outbound tasks when handing over a namespace to avoid starting work that may not be
	// committed and cause duplicate requests.
	// We check namespace handover state **once** when processing is started. Outbound tasks may take up to 10
	// seconds (by default), but we avoid checking again later, before committing the result, to attempt to commit
	// results of inflight tasks and not lose the progress.
	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: Move this logic to queues.Executable when metrics tags don't need
		// to be returned from task executor. Also check the standby queue logic.
		return respond(consts.ErrNamespaceHandover)
	}

	if err := validateTaskByClock(e.shardContext, task); err != nil {
		return respond(err)
	}

	switch task := task.(type) {
	case *tasks.StateMachineOutboundTask:
		return respond(e.executeStateMachineTask(ctx, task))
	case *tasks.ChasmTask:
		return respond(e.executeChasmSideEffectTask(ctx, task))
	}

	return respond(queueserrors.NewUnprocessableTaskError(fmt.Sprintf("unknown task type '%T'", task)))
}

func (e *outboundQueueActiveTaskExecutor) executeChasmSideEffectTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	ctx, cancel := context.WithTimeout(ctx, outboundTaskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, e.shardContext, e.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	ms, err := loadMutableStateForTransferTask(ctx, e.shardContext, weContext, task, e.metricsHandler, e.logger)
	if err != nil {
		return err
	}
	tree := ms.ChasmTree()

	// Now that we've loaded the CHASM tree, we can release the lock before task
	// execution. The task's executor must do its own locking as needed, and additional
	// mutable state validations will run at access time.
	release(nil)

	err = executeChasmSideEffectTask(
		ctx,
		e.chasmEngine,
		e.shardContext.ChasmRegistry(),
		tree,
		task,
	)
	return err
}

func (e *outboundQueueActiveTaskExecutor) executeStateMachineTask(
	ctx context.Context,
	task tasks.Task,
) error {
	// Timeout for hsm outbound tasks are determined by each component's task executor

	ref, smt, err := StateMachineTask(e.shardContext.StateMachineRegistry(), task)
	if err != nil {
		return err
	}

	smRegistry := e.shardContext.StateMachineRegistry()
	return smRegistry.ExecuteImmediateTask(ctx, e, ref, smt)
}
