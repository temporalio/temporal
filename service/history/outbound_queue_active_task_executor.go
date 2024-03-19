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
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type outboundQueueActiveTaskExecutor struct {
	taskExecutor
}

var _ queues.Executor = &outboundQueueActiveTaskExecutor{}

func newOutboundQueueActiveTaskExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *outboundQueueActiveTaskExecutor {
	return &outboundQueueActiveTaskExecutor{
		taskExecutor: taskExecutor{
			shardContext:   shardCtx,
			cache:          workflowCache,
			logger:         logger,
			metricsHandler: metricsHandler.WithTags(metrics.OperationTag(metrics.OperationOutboundQueueProcessorScope)),
		},
	}
}

func (e *outboundQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	var taskType string
	ref, smt, err := e.stateMachineTask(task)
	if err != nil {
		taskType = "ActiveUnknownOutbound"
	} else {
		taskType = "Active." + smt.Type().Name
	}

	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		e.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType),
	}

	// We don't want to execute outbound tasks when handing over a namespace to avoid starting work that may not be
	// committed and cause duplicate requests.
	// We check namespace handover state **once** when processing is started. Outbound tasks may take up to 10
	// seconds (by default), but we avoid checking again later, before committing the result, to attempt to commit
	// results of inflight tasks and not lose the progress.
	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: Move this logic to queues.Executable when metrics tags don't need to be returned from task executor.
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
	}

	if err == nil {
		err = hsm.Execute(ctx, e.shardContext.StateMachineRegistry(), e, ref, smt)
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

func (e *outboundQueueActiveTaskExecutor) stateMachineTask(task tasks.Task) (hsm.Ref, hsm.Task, error) {
	cbt, ok := task.(*tasks.StateMachineOutboundTask)
	if !ok {
		return hsm.Ref{}, nil, queues.NewUnprocessableTaskError("unknown task type")
	}
	def, ok := e.shardContext.StateMachineRegistry().TaskSerializer(cbt.Info.Type)
	if !ok {
		return hsm.Ref{}, nil, queues.NewUnprocessableTaskError(fmt.Sprintf("deserializer not registered for task type %v", cbt.Info.Type))
	}
	smt, err := def.Deserialize(cbt.Info.Data, hsm.TaskKindOutbound{Destination: cbt.Destination})
	if err != nil {
		return hsm.Ref{}, nil, fmt.Errorf(
			"%w: %w",
			queues.NewUnprocessableTaskError(fmt.Sprintf("cannot deserialize task %v", cbt.Info.Type)),
			err,
		)
	}
	return hsm.Ref{
		WorkflowKey:     taskWorkflowKey(task),
		StateMachineRef: cbt.Info.Ref,
	}, smt, nil
}
