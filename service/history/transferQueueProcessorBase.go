package history

import (
	"time"

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

func (t *transferQueueProcessorBase) getTransferTaskMetricsScope(
	taskType int32,
	isActive bool,
) int {
	switch taskType {
	case persistence.TransferTaskTypeActivityTask:
		if isActive {
			return metrics.TransferActiveTaskActivityScope
		}
		return metrics.TransferStandbyTaskActivityScope
	case persistence.TransferTaskTypeDecisionTask:
		if isActive {
			return metrics.TransferActiveTaskDecisionScope
		}
		return metrics.TransferStandbyTaskDecisionScope
	case persistence.TransferTaskTypeCloseExecution:
		if isActive {
			return metrics.TransferActiveTaskCloseExecutionScope
		}
		return metrics.TransferStandbyTaskCloseExecutionScope
	case persistence.TransferTaskTypeCancelExecution:
		if isActive {
			return metrics.TransferActiveTaskCancelExecutionScope
		}
		return metrics.TransferStandbyTaskCancelExecutionScope
	case persistence.TransferTaskTypeSignalExecution:
		if isActive {
			return metrics.TransferActiveTaskSignalExecutionScope
		}
		return metrics.TransferStandbyTaskSignalExecutionScope
	case persistence.TransferTaskTypeStartChildExecution:
		if isActive {
			return metrics.TransferActiveTaskStartChildExecutionScope
		}
		return metrics.TransferStandbyTaskStartChildExecutionScope
	case persistence.TransferTaskTypeRecordWorkflowStarted:
		if isActive {
			return metrics.TransferActiveTaskRecordWorkflowStartedScope
		}
		return metrics.TransferStandbyTaskRecordWorkflowStartedScope
	case persistence.TransferTaskTypeResetWorkflow:
		if isActive {
			return metrics.TransferActiveTaskResetWorkflowScope
		}
		return metrics.TransferStandbyTaskResetWorkflowScope
	case persistence.TransferTaskTypeUpsertWorkflowSearchAttributes:
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
