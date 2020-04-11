// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	updateVisibilityAckLevel func(ackLevel int64) error
	visibilityQueueShutdown  func() error

	visibilityQueueProcessorBase struct {
		shard                    ShardContext
		options                  *QueueProcessorOptions
		executionManager         persistence.ExecutionManager
		maxReadAckLevel          maxReadAckLevel
		updateVisibilityAckLevel updateVisibilityAckLevel
		visibilityQueueShutdown  visibilityQueueShutdown
		logger                   log.Logger
	}
)

func newVisibilityQueueProcessorBase(
	shard ShardContext,
	options *QueueProcessorOptions,
	maxReadAckLevel maxReadAckLevel,
	updateVisibilityAckLevel updateVisibilityAckLevel,
	visibilityQueueShutdown visibilityQueueShutdown,
	logger log.Logger,
) *visibilityQueueProcessorBase {

	return &visibilityQueueProcessorBase{
		shard:                    shard,
		options:                  options,
		executionManager:         shard.GetExecutionManager(),
		maxReadAckLevel:          maxReadAckLevel,
		updateVisibilityAckLevel: updateVisibilityAckLevel,
		visibilityQueueShutdown:  visibilityQueueShutdown,
		logger:                   logger,
	}
}

func (t *visibilityQueueProcessorBase) readTasks(
	readLevel int64,
) ([]queueTaskInfo, bool, error) {

	response, err := t.executionManager.GetVisibilityTasks(&persistence.GetVisibilityTasksRequest{
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

func (t *visibilityQueueProcessorBase) updateAckLevel(
	ackLevel int64,
) error {

	return t.updateVisibilityAckLevel(ackLevel)
}

func (t *visibilityQueueProcessorBase) queueShutdown() error {
	return t.visibilityQueueShutdown()
}

func (t *visibilityQueueProcessorBase) getVisibilityTaskMetricsScope(
	taskType int32,
	isActive bool,
) int {
	switch taskType {
	case persistence.VisibilityTaskTypeRecordCloseExecution:
		if isActive {
			return metrics.VisibilityActiveTaskActivityScope
		}
		return metrics.VisibilityStandbyTaskActivityScope
	case persistence.VisibilityTaskTypeRecordWorkflowStarted:
		if isActive {
			return metrics.VisibilityActiveTaskActivityScope
		}
		return metrics.VisibilityStandbyTaskActivityScope
	case persistence.VisibilityTaskTypeUpsertWorkflowSearchAttributes:
		if isActive {
			return metrics.VisibilityActiveTaskActivityScope
		}
		return metrics.VisibilityStandbyTaskActivityScope

	default:
		if isActive {
			return metrics.VisibilityActiveQueueProcessorScope
		}
		return metrics.VisibilityStandbyQueueProcessorScope
	}
}
