package history

import (
	"fmt"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(
	shard ShardContext,
	logger log.Logger,
	namespaceID []byte,
	version int64,
	taskVersion int64,
	task interface{},
) (bool, error) {

	if !shard.GetService().GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	namespaceEntry, err := shard.GetNamespaceCache().GetNamespaceByID(primitives.UUIDString(namespaceID))
	if err != nil {
		logger.Debug("Cannot find namespaceID", tag.WorkflowNamespaceIDBytes(namespaceID), tag.Error(err))
		return false, err
	}
	if !namespaceEntry.IsGlobalNamespace() {
		logger.Debug("NamespaceID is not active, task version check pass", tag.WorkflowNamespaceIDBytes(namespaceID), tag.Task(task))
		return true, nil
	} else if version != taskVersion {
		logger.Debug("NamespaceID is active, task version != target version", tag.WorkflowNamespaceIDBytes(namespaceID), tag.Task(task), tag.TaskVersion(version))
		return false, nil
	}
	logger.Debug("NamespaceID is active, task version == target version", tag.WorkflowNamespaceIDBytes(namespaceID), tag.Task(task), tag.TaskVersion(version))
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	context workflowExecutionContext,
	transferTask *persistenceblobs.TransferTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (mutableState, error) {

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil, nil
		}
		return nil, err
	}
	executionInfo := msBuilder.GetExecutionInfo()

	// check to see if cache needs to be refreshed as we could potentially have stale workflow execution
	// the exception is decision consistently fail
	// there will be no event generated, thus making the decision schedule ID == next event ID
	isDecisionRetry := transferTask.TaskType == persistence.TransferTaskTypeDecisionTask &&
		executionInfo.DecisionScheduleID == transferTask.GetScheduleId() &&
		executionInfo.DecisionAttempt > 0

	if transferTask.GetScheduleId() >= msBuilder.GetNextEventID() && !isDecisionRetry {
		metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if transferTask.GetScheduleId() >= msBuilder.GetNextEventID() {
			logger.Info("Transfer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowScheduleID(transferTask.GetScheduleId()),
				tag.WorkflowNextEventID(msBuilder.GetNextEventID()))
			return nil, nil
		}
	}
	return msBuilder, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	context workflowExecutionContext,
	timerTask *persistenceblobs.TimerTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (mutableState, error) {

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil, nil
		}
		return nil, err
	}
	executionInfo := msBuilder.GetExecutionInfo()

	// check to see if cache needs to be refreshed as we could potentially have stale workflow execution
	// the exception is decision consistently fail
	// there will be no event generated, thus making the decision schedule ID == next event ID
	isDecisionRetry := timerTask.TaskType == persistence.TaskTypeDecisionTimeout &&
		executionInfo.DecisionScheduleID == timerTask.GetEventId() &&
		executionInfo.DecisionAttempt > 0

	if timerTask.GetEventId() >= msBuilder.GetNextEventID() && !isDecisionRetry {
		metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if timerTask.GetEventId() >= msBuilder.GetNextEventID() {
			logger.Info("Timer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowEventID(timerTask.GetEventId()),
				tag.WorkflowNextEventID(msBuilder.GetNextEventID()))
			return nil, nil
		}
	}
	return msBuilder, nil
}

func initializeLoggerForTask(
	shardID int,
	task queueTaskInfo,
	logger log.Logger,
) log.Logger {

	taskLogger := logger.WithTags(
		tag.ShardID(shardID),
		tag.TaskID(task.GetTaskId()),
		tag.FailoverVersion(task.GetVersion()),
		tag.TaskType(task.GetTaskType()),
		tag.WorkflowNamespaceIDBytes(task.GetNamespaceId()),
		tag.WorkflowID(task.GetWorkflowId()),
		tag.WorkflowRunIDBytes(task.GetRunId()),
	)

	switch task := task.(type) {
	case *persistenceblobs.TimerTaskInfo:
		taskLogger = taskLogger.WithTags(
			tag.WorkflowTimeoutType(int64(task.TimeoutType)),
		)
	case *persistenceblobs.TransferTaskInfo:
		// noop
	case *persistence.ReplicationTaskInfoWrapper:
		// noop
	default:
		taskLogger.Error(fmt.Sprintf("Unknown queue task type: %v", task))
	}

	return taskLogger
}
