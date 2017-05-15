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

// LogOperationFailedEvent is used to log generic operation failures.
func LogOperationFailedEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: OperationFailed,
	}).Warnf("%v.  Error: %v", msg, err)
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

// LogTransferQueueProcesorStartingEvent is used to log transfer queue processor starting
func LogTransferQueueProcesorStartingEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorStarting,
	}).Info("Transfer queue processor starting.")
}

// LogTransferQueueProcesorStartedEvent is used to log transfer queue processor started
func LogTransferQueueProcesorStartedEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorStarted,
	}).Info("Transfer queue processor started.")
}

// LogTransferQueueProcesorShuttingDownEvent is used to log transfer queue processing shutting down
func LogTransferQueueProcesorShuttingDownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShuttingDown,
	}).Info("Transfer queue processor shutting down.")
}

// LogTransferQueueProcesorShutdownEvent is used to log transfer queue processor shutdown complete
func LogTransferQueueProcesorShutdownEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShutdown,
	}).Info("Transfer queue processor shutdown.")
}

// LogTransferQueueProcesorShutdownTimedoutEvent is used to log timeout during transfer queue processor shutdown
func LogTransferQueueProcesorShutdownTimedoutEvent(logger bark.Logger) {
	logger.WithFields(bark.Fields{
		TagWorkflowEventID: TransferQueueProcessorShutdownTimedout,
	}).Warn("Transfer queue processor timedout on shutdown.")
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
