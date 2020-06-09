// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"time"

	commongenpb "github.com/temporalio/temporal/.gen/proto/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	maxReadAckLevel func() int64

	updateTransferAckLevel func(ackLevel int64) error
	transferQueueShutdown  func() error

	transferQueueProcessorBase struct {
		shard                  ShardContext
		options                *QueueProcessorOptions
		executionManager       persistence.ExecutionManager
		maxReadAckLevel        maxReadAckLevel
		updateTransferAckLevel updateTransferAckLevel
		transferQueueShutdown  transferQueueShutdown
		logger                 log.Logger
	}
)

const (
	secondsInDay = int32(24 * time.Hour / time.Second)

	defaultNamespace = "defaultNamespace"
)

func newTransferQueueProcessorBase(
	shard ShardContext,
	options *QueueProcessorOptions,
	maxReadAckLevel maxReadAckLevel,
	updateTransferAckLevel updateTransferAckLevel,
	transferQueueShutdown transferQueueShutdown,
	logger log.Logger,
) *transferQueueProcessorBase {

	return &transferQueueProcessorBase{
		shard:                  shard,
		options:                options,
		executionManager:       shard.GetExecutionManager(),
		maxReadAckLevel:        maxReadAckLevel,
		updateTransferAckLevel: updateTransferAckLevel,
		transferQueueShutdown:  transferQueueShutdown,
		logger:                 logger,
	}
}

func (t *transferQueueProcessorBase) readTasks(
	readLevel int64,
) ([]queueTaskInfo, bool, error) {

	response, err := t.executionManager.GetTransferTasks(&persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel,
		MaxReadLevel: t.maxReadAckLevel(),
		BatchSize:    t.options.BatchSize(),
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]queueTaskInfo, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (t *transferQueueProcessorBase) updateAckLevel(
	ackLevel int64,
) error {

	return t.updateTransferAckLevel(ackLevel)
}

func (t *transferQueueProcessorBase) queueShutdown() error {
	return t.transferQueueShutdown()
}

func getTransferTaskMetricsScope(
	taskType commongenpb.TaskType,
	isActive bool,
) int {
	switch taskType {
	case commongenpb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
		if isActive {
			return metrics.TransferActiveTaskActivityScope
		}
		return metrics.TransferStandbyTaskActivityScope
	case commongenpb.TASK_TYPE_TRANSFER_DECISION_TASK:
		if isActive {
			return metrics.TransferActiveTaskDecisionScope
		}
		return metrics.TransferStandbyTaskDecisionScope
	case commongenpb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
		if isActive {
			return metrics.TransferActiveTaskCloseExecutionScope
		}
		return metrics.TransferStandbyTaskCloseExecutionScope
	case commongenpb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
		if isActive {
			return metrics.TransferActiveTaskCancelExecutionScope
		}
		return metrics.TransferStandbyTaskCancelExecutionScope
	case commongenpb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
		if isActive {
			return metrics.TransferActiveTaskSignalExecutionScope
		}
		return metrics.TransferStandbyTaskSignalExecutionScope
	case commongenpb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
		if isActive {
			return metrics.TransferActiveTaskStartChildExecutionScope
		}
		return metrics.TransferStandbyTaskStartChildExecutionScope
	case commongenpb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED:
		if isActive {
			return metrics.TransferActiveTaskRecordWorkflowStartedScope
		}
		return metrics.TransferStandbyTaskRecordWorkflowStartedScope
	case commongenpb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
		if isActive {
			return metrics.TransferActiveTaskResetWorkflowScope
		}
		return metrics.TransferStandbyTaskResetWorkflowScope
	case commongenpb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		if isActive {
			return metrics.TransferActiveTaskUpsertWorkflowSearchAttributesScope
		}
		return metrics.TransferStandbyTaskUpsertWorkflowSearchAttributesScope
	default:
		if isActive {
			return metrics.TransferActiveQueueProcessorScope
		}
		return metrics.TransferStandbyQueueProcessorScope
	}
}
