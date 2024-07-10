// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"context"
	"errors"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type outboundQueueStandbyTaskExecutor struct {
	stateMachineEnvironment
	config *configs.Config

	clusterName string
}

var _ queues.Executor = &outboundQueueStandbyTaskExecutor{}

func newOutboundQueueStandbyTaskExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
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
	}
}

func (e *outboundQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetOutboundTaskTypeTagValue(task, false)
	respond := func(err error) queues.ExecuteResponse {
		metricsTags := []metrics.Tag{
			getNamespaceTagByID(e.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
			metrics.TaskTypeTag(taskType),
			metrics.OperationTag(taskType),
		}
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    false,
			ExecutionErr:        err,
		}
	}

	nsName, err := e.shardContext.GetNamespaceRegistry().GetNamespaceName(
		namespace.ID(task.GetNamespaceID()),
	)
	if err != nil {
		return respond(err)
	}

	ref, smt, err := stateMachineTask(e.shardContext, task)
	if err != nil {
		return respond(err)
	}

	if err := validateTaskByClock(e.shardContext, task); err != nil {
		return respond(err)
	}

	destination := ""
	if dtask, ok := task.(tasks.HasDestination); ok {
		destination = dtask.GetDestination()
	}

	actionFn := func(ctx context.Context) (any, error) {
		err := e.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
			if smt.Concurrent() {
				//nolint:revive // concurrent tasks implements hsm.ConcurrentTask interface
				concurrentSmt := smt.(hsm.ConcurrentTask)
				return concurrentSmt.Validate(node)
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, consts.ErrStaleReference) {
				// If the reference is stale, then the task was already executed in
				// the active queue, and there is nothing to do here.
				return nil, nil
			}
			return nil, err
		}

		// If there was no error from Access nor from the accessor function, then the task
		// is still valid for processing based on the current state of the machine.
		// The *likely* reasons are: a) delay in the replication stack; b) destination is down.
		// In any case, the task needs to be retried.
		var postActionInfoErr error = consts.ErrTaskRetry
		if e.config.OutboundStandbyTaskMissingEventsDestinationDownErr(nsName.String(), destination) {
			// Wrap the retry error with DestinationDownError so it can trigger the circuit breaker on
			// the standby side. This won't do any harm, at most some delay processing the standby task.
			// Assuming the dynamic config OutboundStandbyTaskMissingEventsDiscardDelay is long enough,
			// it should give enough time for the active side to execute the task successfully, and the
			// standby side to process it as well without discarding the task.
			postActionInfoErr = queues.NewDestinationDownError(
				"standby task executor returned retryable error",
				postActionInfoErr,
			)
		}
		return postActionInfoErr, nil
	}

	err = e.processTask(
		ctx,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			e.Now,
			0, // We don't need resend delay since we don't do fetch history.
			e.config.OutboundStandbyTaskMissingEventsDiscardDelay(nsName.String(), destination),
			// We don't need to fetch history from remote for state machine to sync.
			// So, just use the noop post action which will return a retry error
			// if the task didn't succeed.
			standbyTaskPostActionNoOp,
			standbyOutboundTaskPostActionTaskDiscarded,
		),
	)

	return respond(err)
}

func (e *outboundQueueStandbyTaskExecutor) processTask(
	ctx context.Context,
	task tasks.Task,
	actionFn func(context.Context) (any, error),
	postActionFn standbyPostActionFn,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	nsRecord, err := e.shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(task.GetNamespaceID()),
	)
	if err != nil {
		return err
	}
	if !nsRecord.IsOnCluster(e.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return nil
	}

	historyResendInfo, err := actionFn(ctx)
	if err != nil {
		return err
	}

	return postActionFn(ctx, task, historyResendInfo, e.logger)
}
