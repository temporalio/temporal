package history

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type outboundQueueStandbyTaskExecutor struct {
	stateMachineEnvironment
	chasmEngine chasm.Engine
	config      *configs.Config

	clusterName string
}

var _ queues.Executor = &outboundQueueStandbyTaskExecutor{}

func newOutboundQueueStandbyTaskExecutor(
	shardCtx historyi.ShardContext,
	workflowCache wcache.Cache,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
	chasmEngine chasm.Engine,
) *outboundQueueStandbyTaskExecutor {
	return &outboundQueueStandbyTaskExecutor{
		stateMachineEnvironment: stateMachineEnvironment{
			shardContext: shardCtx,
			cache:        workflowCache,
			logger:       logger,
			metricsHandler: metricsHandler.WithTags(
				metrics.OperationTag(metrics.OperationOutboundQueueProcessorScope),
			),
		},
		config:      shardCtx.GetConfig(),
		clusterName: clusterName,
		chasmEngine: chasmEngine,
	}
}

func (e *outboundQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetOutboundTaskTypeTagValue(task, false)
	namespaceTag, _ := getNamespaceTagAndReplicationStateByID(
		e.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	respond := func(err error) queues.ExecuteResponse {
		metricsTags := []metrics.Tag{
			namespaceTag,
			metrics.TaskTypeTag(taskType),
			metrics.OperationTag(taskType),
		}
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    false,
			ExecutionErr:        err,
		}
	}

	nsRecord, err := e.shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(task.GetNamespaceID()),
	)
	if err != nil {
		return respond(err)
	}

	if !nsRecord.IsOnCluster(e.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return respond(nil)
	}

	if err := validateTaskByClock(e.shardContext, task); err != nil {
		return respond(err)
	}

	nsName := nsRecord.Name().String()

	switch task := task.(type) {
	case *tasks.StateMachineOutboundTask:
		return respond(e.executeStateMachineTask(ctx, task, nsName))
	case *tasks.ChasmTask:
		return respond(e.executeChasmSideEffectTask(ctx, task))
	}

	return respond(queues.NewUnprocessableTaskError(fmt.Sprintf("unknown task type '%T'", task)))
}

func (e *outboundQueueStandbyTaskExecutor) executeStateMachineTask(
	ctx context.Context,
	task tasks.Task,
	nsName string,
) error {
	destination := ""
	if dtask, ok := task.(tasks.HasDestination); ok {
		destination = dtask.GetDestination()
	}

	ref, _, err := StateMachineTask(e.shardContext.StateMachineRegistry(), task)
	if err != nil {
		return err
	}

	err = e.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		// If we managed to access the machine the task is still valid.
		// The logic below will either discard it or retry.
		return nil
	})

	if err != nil {
		if errors.Is(err, consts.ErrStaleReference) {
			// If the reference is stale, then the task was already executed in
			// the active queue, and there is nothing to do here.
			return nil
		}
		return err
	}

	// If there was no error from Access nor from the accessor function, then the task
	// is still valid for processing based on the current state of the machine.
	// The *likely* reasons are: a) delay in the replication stack; b) destination is down.
	// In any case, the task needs to be retried (or discarded, based on the configured discard delay).

	discardTime := task.GetVisibilityTime().Add(e.config.OutboundStandbyTaskMissingEventsDiscardDelay(nsName, destination))
	// now > task start time + discard delay
	if e.Now().After(discardTime) {
		e.logger.Warn("Discarding standby outbound task due to task being pending for too long.", tag.Task(task))
		return consts.ErrTaskDiscarded
	}

	err = consts.ErrTaskRetry
	if e.config.OutboundStandbyTaskMissingEventsDestinationDownErr(nsName, destination) {
		// Wrap the retry error with DestinationDownError so it can trigger the circuit breaker on
		// the standby side. This won't do any harm, at most some delay processing the standby task.
		// Assuming the dynamic config OutboundStandbyTaskMissingEventsDiscardDelay is long enough,
		// it should give enough time for the active side to execute the task successfully, and the
		// standby side to process it as well without discarding the task.
		err = queues.NewDestinationDownError(
			"standby task executor returned retryable error",
			err,
		)
		return err
	}

	return nil
}

func (e *outboundQueueStandbyTaskExecutor) executeChasmSideEffectTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, e.shardContext, e.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	ms, err := weContext.LoadMutableState(ctx, e.shardContext)
	if err != nil {
		return err
	}

	shouldRetry, err := validateChasmSideEffectTask(
		ctx,
		ms,
		task,
	)
	if shouldRetry != nil {
		err = consts.ErrTaskRetry
	}

	return err
}
