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

package logging

import (
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
)

//
// Common logging methods
//

// LogPersistantStoreErrorEvent is used to log errors from persistence layer.
func LogPersistantStoreErrorEvent(logger bark.Logger, operation string, err error, details string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: PersistentStoreErrorEventID,
		TagStoreOperation:  operation,
		TagWorkflowErr:     err,
	}).Errorf("Persistent store operation failure. Operation Details: %v", details)
}

// LogTransferTaskProcessingFailedEvent is used to log failures from transfer task processing.
func LogTransferTaskProcessingFailedEvent(logger bark.Logger, taskID int64, taskType int, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferTaskProcessingFailed,
		TagTaskID:          taskID,
		TagTaskType:        taskType,
		TagWorkflowErr:     err,
	}).Errorf("Processor failed to process transfer task: %v, type: %v.  Error: %v", taskID, taskType, err)
}

// LogOperationFailedEvent is used to log generic operation failures.
func LogOperationFailedEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: OperationFailed,
		TagWorkflowErr:     err,
	}).Warnf("%v.  Error: %v", msg, err)
}

// LogOperationStuckEvent is used to log task processing stuck
func LogOperationStuckEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: OperationStuck,
		TagWorkflowErr:     err,
	}).Error(msg)
}

// LogInternalServiceError is used to log internal service error
func LogInternalServiceError(logger bark.Logger, err error) {
	logger.WithFields(bark.Fields{
		TagErr: err,
	}).Error("Internal service error")
}

// LogUncategorizedError is used to log error that are uncategorized
func LogUncategorizedError(logger bark.Logger, err error) {
	logger.WithFields(bark.Fields{
		TagErr: err,
	}).Error("Uncategorized error")
}

//
// History service logging methods
//

// LogInvalidHistoryActionEvent is used to log invalid history builder state
func LogInvalidHistoryActionEvent(logger bark.Logger, action string, eventID int64, state string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID:      InvalidHistoryActionEventID,
		TagHistoryBuilderAction: action,
	}).Warnf("Invalid history builder state for action: EventID: %v, State: %v", eventID, state)
}

// LogHistorySerializationErrorEvent is used to log errors serializing execution history
func LogHistorySerializationErrorEvent(logger bark.Logger, err error, msg string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistorySerializationErrorEventID,
		TagWorkflowErr:     err,
	}).Errorf("Error serializing workflow execution history.  Msg: %v", msg)
}

// LogHistoryDeserializationErrorEvent is used to log errors deserializing execution history
func LogHistoryDeserializationErrorEvent(logger bark.Logger, err error, msg string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistoryDeserializationErrorEventID,
		TagWorkflowErr:     err,
	}).Errorf("Error deserializing workflow execution history.  Msg: %v", msg)
}

// LogHistoryEngineStartingEvent is used to log history engine starting
func LogHistoryEngineStartingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistoryEngineStarting,
	}).Info("HistoryEngine starting.")
}

// LogHistoryEngineStartedEvent is used to log history engine started
func LogHistoryEngineStartedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistoryEngineStarted,
	}).Info("HistoryEngine started.")
}

// LogHistoryEngineShuttingDownEvent is used to log history shutting down
func LogHistoryEngineShuttingDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistoryEngineShuttingDown,
	}).Info("HistoryEngine shutting down.")
}

// LogHistoryEngineShutdownEvent is used to log history shut down complete
func LogHistoryEngineShutdownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: HistoryEngineShutdown,
	}).Info("HistoryEngine shutdown.")
}

// LogDuplicateTaskEvent is used to log the event when a duplicate task is detected
func LogDuplicateTaskEvent(lg bark.Logger, taskType int, taskID int64, requestID string, scheduleID, startedID int64,
	isRunning bool) {
	lg.WithFields(bark.Fields{
		TagWorkflowEventID: DuplicateTaskEventID,
	}).Debugf("Potentially duplicate task.  TaskID: %v, TaskType: %v, RequestID: %v, scheduleID: %v, startedID: %v, isRunning: %v",
		taskID, taskType, requestID, scheduleID, startedID, isRunning)
}

// LogDuplicateTransferTaskEvent is used to log the event when duplicate processing of the same transfer task is detected
func LogDuplicateTransferTaskEvent(lg bark.Logger, taskType int, taskID int64, scheduleID int64) {
	lg.WithFields(bark.Fields{
		TagWorkflowEventID: DuplicateTransferTaskEventID,
	}).Debugf("Potentially duplicate task.  TaskID: %v, TaskType: %v, scheduleID: %v",
		taskID, taskType, scheduleID)
}

// LogShardRangeUpdatedEvent is used to log rangeID update for a shard
func LogShardRangeUpdatedEvent(logger bark.Logger, shardID int, rangeID, startSequence, endSequence int64) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardRangeUpdatedEventID,
	}).Infof("Range updated for shardID '%v'.  RangeID: %v, StartSequence: %v, EndSequence: %v", shardID,
		rangeID, startSequence, endSequence)
}

// LogShardControllerStartedEvent is used to log shard controller started
func LogShardControllerStartedEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardControllerStarted,
	}).Infof("ShardController started on host: %v", host)
}

// LogShardControllerShutdownEvent is used to log shard controller shutdown complete
func LogShardControllerShutdownEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardControllerShutdown,
	}).Infof("ShardController stopped on host: %v", host)
}

// LogShardControllerShuttingDownEvent is used to log shard controller shutting down
func LogShardControllerShuttingDownEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardControllerShuttingDown,
	}).Infof("ShardController stopping on host: %v", host)
}

// LogShardControllerShutdownTimedoutEvent is used to log timeout during shard controller shutdown
func LogShardControllerShutdownTimedoutEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardControllerShutdownTimedout,
	}).Warnf("ShardController timed out during shutdown on host: %v", host)
}

// LogRingMembershipChangedEvent is used to log membership changes events received by shard controller
func LogRingMembershipChangedEvent(logger bark.Logger, host string, added, removed, updated int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: RingMembershipChangedEvent,
	}).Infof("ShardController on host '%v' received ring membership changed event: {Added: %v, Removed: %v, Updated: %v}",
		host, added, removed, updated)
}

// LogShardClosedEvent is used to log shard closed event
func LogShardClosedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardClosedEvent,
		TagHistoryShardID:  shardID,
	}).Infof("ShardController on host '%v' received shard closed event for shardID: %v", host, shardID)
}

// LogShardItemCreatedEvent is used to log creation of a shard item
func LogShardItemCreatedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardItemCreated,
	}).Infof("ShardController on host '%v' created a shard item for shardID '%v'.", host, shardID)
}

// LogShardItemRemovedEvent is used to log removal of a shard item
func LogShardItemRemovedEvent(logger bark.Logger, host string, shardID int, remainingShards int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardItemRemoved,
	}).Infof("ShardController on host '%v' removed shard item for shardID '%v'.  Remaining number of shards: %v",
		host, shardID, remainingShards)
}

// LogShardEngineCreatingEvent is used to log start of history engine creation
func LogShardEngineCreatingEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardEngineCreating,
	}).Infof("ShardController on host '%v' creating engine for shardID '%v'.", host, shardID)
}

// LogShardEngineCreatedEvent is used to log completion of history engine creation
func LogShardEngineCreatedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardEngineCreated,
	}).Infof("ShardController on host '%v' created engine for shardID '%v'.", host, shardID)
}

// LogShardEngineStoppingEvent is used to log when stopping engine for a shard has started
func LogShardEngineStoppingEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardEngineStopping,
	}).Infof("ShardController on host '%v' stopping engine for shardID '%v'.", host, shardID)
}

// LogShardEngineStoppedEvent is used to log when stopping engine for a shard has completed
func LogShardEngineStoppedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ShardEngineStopped,
	}).Infof("ShardController on host '%v' stopped engine for shardID '%v'.", host, shardID)
}

// LogMutableStateInvalidAction is used to log invalid mutable state builder state
func LogMutableStateInvalidAction(logger bark.Logger, errorMsg string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: InvalidMutableStateActionEventID,
	}).Errorf("%v.  ", errorMsg)
}

// LogMultipleCompletionDecisionsEvent is used to log multiple completion decisions for an execution
func LogMultipleCompletionDecisionsEvent(lg bark.Logger, decisionType shared.DecisionType) {
	lg.WithFields(bark.Fields{
		TagWorkflowEventID: MultipleCompletionDecisionsEventID,
		TagDecisionType:    decisionType,
	}).Warnf("Multiple completion decisions.  DecisionType: %v", decisionType)
}

// LogDecisionFailedEvent is used to log decision failures by RespondDecisionTaskCompleted handler
func LogDecisionFailedEvent(lg bark.Logger, domainID, workflowID, runID string,
	failCause shared.DecisionTaskFailedCause) {
	lg.WithFields(bark.Fields{
		TagWorkflowEventID:     DecisionFailedEventID,
		TagDomainID:            domainID,
		TagWorkflowExecutionID: workflowID,
		TagWorkflowRunID:       runID,
		TagDecisionFailCause:   failCause,
	}).Info("Failing the decision.")
}

//
// Matching service logging methods
//

// LogTaskListLoadingEvent is used to log starting of a new task list loading
func LogTaskListLoadingEvent(logger bark.Logger, taskListName string, taskListType int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TaskListLoading,
		TagTaskListName:    taskListName,
		TagTaskListType:    taskListType,
	}).Info("Loading TaskList.")
}

// LogTaskListLoadedEvent is used to log completion of a new task list loading
func LogTaskListLoadedEvent(logger bark.Logger, taskListName string, taskListType int) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TaskListLoaded,
		TagTaskListName:    taskListName,
		TagTaskListType:    taskListType,
	}).Info("Loaded TaskList.")
}

// LogTaskListLoadingFailedEvent is used to log failure of a new task list loading
func LogTaskListLoadingFailedEvent(logger bark.Logger, taskListName string, taskListType int, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TaskListLoadingFailed,
		TagTaskListName:    taskListName,
		TagTaskListType:    taskListType,
		TagWorkflowErr:     err,
	}).Info("Loading TaskList failed.")
}

// LogTaskListUnloadingEvent is used to log starting of a task list unloading
func LogTaskListUnloadingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TaskListUnloading,
	}).Info("Unloading TaskList.")
}

// LogTaskListUnloadedEvent is used to log completion of a task list unloading
func LogTaskListUnloadedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TaskListUnloaded,
	}).Info("Unloaded TaskList.")
}

// LogQueryTaskMissingWorkflowTypeErrorEvent is used to log invalid query task that is missing workflow type
func LogQueryTaskMissingWorkflowTypeErrorEvent(logger bark.Logger, workflowID, runID, queryType string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: InvalidQueryTaskEventID,
		"WorkflowID":       workflowID,
		"RunID":            runID,
		"QueryType":        queryType,
	}).Error("Cannot get WorkflowType for QueryTask.")
}

// LogQueryTaskFailedEvent is used to log query task failure
func LogQueryTaskFailedEvent(logger bark.Logger, domain, workflowID, runID, queryType string) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: QueryTaskFailedEventID,
		"Domain":           domain,
		"WorkflowID":       workflowID,
		"RunID":            runID,
		"QueryType":        queryType,
	}).Info("QueryWorkflowFailed.")
}

// LogReplicationTaskProcessorStartingEvent is used to log replication task processor starting
func LogReplicationTaskProcessorStartingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorStarting,
	}).Info("Replication task processor starting.")
}

// LogReplicationTaskProcessorStartedEvent is used to log replication task processor started
func LogReplicationTaskProcessorStartedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorStarted,
	}).Info("Replication task processor started.")
}

// LogReplicationTaskProcessorStartFailedEvent is used to log replication task processor started
func LogReplicationTaskProcessorStartFailedEvent(logger bark.Logger, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorStartFailed,
	}).WithError(err).Warn("Replication task processor failed to start.")
}

// LogReplicationTaskProcessorShuttingDownEvent is used to log replication task processing shutting down
func LogReplicationTaskProcessorShuttingDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorShuttingDown,
	}).Info("Replication task processor shutting down.")
}

// LogReplicationTaskProcessorShutdownEvent is used to log replication task processor shutdown complete
func LogReplicationTaskProcessorShutdownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorShutdown,
	}).Info("Replication task processor shutdown.")
}

// LogReplicationTaskProcessorShutdownTimedoutEvent is used to log timeout during replication task processor shutdown
func LogReplicationTaskProcessorShutdownTimedoutEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: ReplicationTaskProcessorShutdownTimedout,
	}).Warn("Replication task processor timedout on shutdown.")
}

// LogQueueProcesorStartingEvent is used to log queue processor starting
func LogQueueProcesorStartingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorStarting,
	}).Info("Queue processor starting.")
}

// LogQueueProcesorStartedEvent is used to log queue processor started
func LogQueueProcesorStartedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorStarted,
	}).Info("Queue processor started.")
}

// LogQueueProcesorShuttingDownEvent is used to log queue processor shutting down
func LogQueueProcesorShuttingDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShuttingDown,
	}).Info("Queue processor shutting down.")
}

// LogQueueProcesorShutdownEvent is used to log transfer queue processor shutdown complete
func LogQueueProcesorShutdownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShutdown,
	}).Info("Queue processor shutdown.")
}

// LogQueueProcesorShutdownTimedoutEvent is used to log timeout during transfer queue processor shutdown
func LogQueueProcesorShutdownTimedoutEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShutdownTimedout,
	}).Warn("Queue processor timedout on shutdown.")
}

// LogTaskProcessingFailedEvent is used to log failures from task processing.
func LogTaskProcessingFailedEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferTaskProcessingFailed,
		TagWorkflowErr:     err,
	}).Error(msg)
}

// LogCriticalErrorEvent is used to log critical errors by application it is expected to have alerts setup on such errors
func LogCriticalErrorEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: OperationCritical,
		TagWorkflowErr:     err,
	}).Error(msg)
}

// LogDecisionTimeoutTooLarge is used to log warning msg for workflow that contains large decision timeout
func LogDecisionTimeoutTooLarge(logger bark.Logger, t int32, domain, wid, wfType string) {
	logger.WithFields(bark.Fields{
		"Domain":          domain,
		"WorkflowID":      wid,
		"WorkflowType":    wfType,
		"DecisionTimeout": t,
	}).Warn("Decision timeout is too large")
}

// LogDecisionTimeoutLargerThanWorkflowTimeout is used to log warning msg for workflow that contains large decision timeout
func LogDecisionTimeoutLargerThanWorkflowTimeout(logger bark.Logger, t int32, domain, wid, wfType string) {
	logger.WithFields(bark.Fields{
		"Domain":          domain,
		"WorkflowID":      wid,
		"WorkflowType":    wfType,
		"DecisionTimeout": t,
	}).Warn("Decision timeout is larger than workflow timeout")
}

// LogOpenWorkflowSampled is used to log msg for open visibility requests that are sampled
func LogOpenWorkflowSampled(logger bark.Logger, domain, wid, rid, wfType string) {
	logger.WithFields(bark.Fields{
		"Domain":       domain,
		"WorkflowID":   wid,
		"RunID":        rid,
		"WorkflowType": wfType,
	}).Info("Request for open workflow is sampled")
}

// LogClosedWorkflowSampled is used to log msg for closed visibility requests that are sampled
func LogClosedWorkflowSampled(logger bark.Logger, domain, wid, rid, wfType string) {
	logger.WithFields(bark.Fields{
		"Domain":       domain,
		"WorkflowID":   wid,
		"RunID":        rid,
		"WorkflowType": wfType,
	}).Info("Request for closed workflow is sampled")
}

// LogListOpenWorkflowByFilter is used to log msg for open visibility requests using filter
func LogListOpenWorkflowByFilter(logger bark.Logger, domain, filter string) {
	logger.WithFields(bark.Fields{
		"Domain":     domain,
		"FilterType": filter,
	}).Info("List open workflow with filter")
}

// LogListClosedWorkflowByFilter is used to log msg for closed visibility requests using filter
func LogListClosedWorkflowByFilter(logger bark.Logger, domain, filter string) {
	logger.WithFields(bark.Fields{
		"Domain":     domain,
		"FilterType": filter,
	}).Info("List closed workflow with filter")
}

// LogIndexProcessorStartingEvent is used to log index processor starting
func LogIndexProcessorStartingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorStarting,
	}).Info("Index processor starting.")
}

// LogIndexProcessorStartedEvent is used to log index processor starting
func LogIndexProcessorStartedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorStarted,
	}).Info("Index processor started.")
}

// LogIndexProcessorStartFailedEvent is used to log index processor started
func LogIndexProcessorStartFailedEvent(logger bark.Logger, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorStartFailed,
	}).WithError(err).Warn("Index processor failed to start.")
}

// LogIndexProcessorShuttingDownEvent is used to log index processing shutting down
func LogIndexProcessorShuttingDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorShuttingDown,
	}).Info("Index processor shutting down.")
}

// LogIndexProcessorShutDownEvent is used to log index processing shutting down
func LogIndexProcessorShutDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorShutDown,
	}).Info("Index processor shut down.")
}

// LogIndexProcessorShutDownTimedoutEvent is used to log index processing shutting down
func LogIndexProcessorShutDownTimedoutEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: IndexProcessorShuttingDownTimedout,
	}).Info("Index processor shut down timedout.")
}

// LogSkipArchivalUpload is used to log that archival upload is being skipped
func LogSkipArchivalUpload(logger bark.Logger, reason string) {
	logger.WithFields(bark.Fields{
		TagArchivalUploadSkipReason: reason,
	}).Warn("Archival upload request is being skipped, will not retry.")
}

// LogFailArchivalUploadAttempt is used to log that archival upload attempt has failed
func LogFailArchivalUploadAttempt(logger bark.Logger, err error, reason, bucket, blobKey string) {
	logger.WithFields(bark.Fields{
		TagErr:                      err,
		TagArchivalUploadFailReason: reason,
		TagBucket:                   bucket,
		TagBlobKey:                  blobKey,
	}).Error("Archival upload attempt is giving up, possibly could retry.")
}

// LogBlobAlreadyUploaded is used to log that a blob which is requested for upload has already been uploaded
func LogBlobAlreadyUploaded(logger bark.Logger, bucket, blobKey string) {
	logger.WithFields(bark.Fields{
		TagBucket:  bucket,
		TagBlobKey: blobKey,
	}).Info("Blob has already been uploaded.")
}
