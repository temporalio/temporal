// Copyright (c) 2019 Uber Technologies, Inc.
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

package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

// eventStoreVersion is already deprecated, this is just a constant for place holder.
// TODO we can remove it after fixing all the query templates
const defaultEventStoreVersionValue = -1

func applyWorkflowMutationBatch(
	batch *gocql.Batch,
	shardID int,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowMutation.ExecutionInfo
	replicationState := workflowMutation.ReplicationState
	versionHistories := workflowMutation.VersionHistories
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowMutation.Condition

	if err := updateExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		cqlNowTimestampMillis,
		condition,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateTimerInfos(
		batch,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	if err := updateChildExecutionInfos(
		batch,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfo,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateRequestCancelInfos(
		batch,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfo,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	updateSignalInfos(
		batch,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfo,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	updateSignalsRequested(
		batch,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedID,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	updateBufferedEvents(
		batch,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowMutation.TransferTasks,
		workflowMutation.ReplicationTasks,
		workflowMutation.TimerTasks,
	)
}

func applyWorkflowSnapshotBatchAsReset(
	batch *gocql.Batch,
	shardID int,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowSnapshot.ExecutionInfo
	replicationState := workflowSnapshot.ReplicationState
	versionHistories := workflowSnapshot.VersionHistories
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowSnapshot.Condition

	if err := updateExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		cqlNowTimestampMillis,
		condition,
	); err != nil {
		return err
	}

	if err := resetActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	resetTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	if err := resetChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	resetRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	resetSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	resetSignalRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	deleteBufferedEvents(
		batch,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks,
	)
}

func applyWorkflowSnapshotBatchAsNew(
	batch *gocql.Batch,
	shardID int,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowSnapshot.ExecutionInfo
	replicationState := workflowSnapshot.ReplicationState
	versionHistories := workflowSnapshot.VersionHistories
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	if err := createExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		cqlNowTimestampMillis,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	if err := updateChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	updateSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID,
	)

	updateSignalsRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		"",
		shardID,
		domainID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks,
	)
}

func createExecution(
	batch *gocql.Batch,
	shardID int,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *p.DataBlob,
	cqlNowTimestampMillis int64,
) error {

	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus); err != nil {
		return err
	}

	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	parentDomainID := emptyDomainID
	parentWorkflowID := ""
	parentRunID := emptyRunID
	initiatedID := emptyInitiatedID
	if executionInfo.ParentDomainID != "" {
		parentDomainID = executionInfo.ParentDomainID
		parentWorkflowID = executionInfo.ParentWorkflowID
		parentRunID = executionInfo.ParentRunID
		initiatedID = executionInfo.InitiatedID
	}

	// TODO we should set the start time and last update time on business logic layer
	executionInfo.StartTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))
	executionInfo.LastUpdatedTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	completionData, completionEncoding := p.FromDataBlob(executionInfo.CompletionEvent)
	if replicationState == nil && versionHistories == nil {
		// Cross DC feature is currently disabled so we will be creating workflow executions without replication state
		batch.Query(templateCreateWorkflowExecutionQuery,
			shardID,
			domainID,
			workflowID,
			runID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else if versionHistories != nil {
		// TODO also need to set the start / current / last write version
		versionHistoriesData, versionHistoriesEncoding := p.FromDataBlob(versionHistories)
		batch.Query(templateCreateWorkflowExecutionWithVersionHistoriesQuery,
			shardID,
			domainID,
			workflowID,
			runID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			versionHistoriesData,
			versionHistoriesEncoding)
	} else if replicationState != nil {
		lastReplicationInfo := make(map[string]map[string]interface{})
		for k, v := range replicationState.LastReplicationInfo {
			lastReplicationInfo[k] = createReplicationInfoMap(v)
		}

		batch.Query(templateCreateWorkflowExecutionWithReplicationQuery,
			shardID,
			domainID,
			workflowID,
			runID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion, // client_library_version
			executionInfo.ClientFeatureVersion, // client_feature_version
			executionInfo.ClientImpl,           // client_impl
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			replicationState.CurrentVersion,
			replicationState.StartVersion,
			replicationState.LastWriteVersion,
			replicationState.LastWriteEventID,
			lastReplicationInfo,
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Create workflow execution with both version histories and replication state."),
		}
	}
	return nil
}

func updateExecution(
	batch *gocql.Batch,
	shardID int,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *p.DataBlob,
	cqlNowTimestampMillis int64,
	condition int64,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus); err != nil {
		return err
	}

	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	parentDomainID := emptyDomainID
	parentWorkflowID := ""
	parentRunID := emptyRunID
	initiatedID := emptyInitiatedID
	if executionInfo.ParentDomainID != "" {
		parentDomainID = executionInfo.ParentDomainID
		parentWorkflowID = executionInfo.ParentWorkflowID
		parentRunID = executionInfo.ParentRunID
		initiatedID = executionInfo.InitiatedID
	}

	// TODO we should set the last update time on business logic layer
	executionInfo.LastUpdatedTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	completionData, completionEncoding := p.FromDataBlob(executionInfo.CompletionEvent)
	if replicationState == nil && versionHistories == nil {
		// Updates will be called with null ReplicationState while the feature is disabled
		batch.Query(templateUpdateWorkflowExecutionQuery,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			executionInfo.NextEventID,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if versionHistories != nil {
		// TODO also need to set the start / current / last write version
		versionHistoriesData, versionHistoriesEncoding := p.FromDataBlob(versionHistories)
		batch.Query(templateUpdateWorkflowExecutionWithVersionHistoriesQuery,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			executionInfo.NextEventID,
			versionHistoriesData,
			versionHistoriesEncoding,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if replicationState != nil {
		lastReplicationInfo := make(map[string]map[string]interface{})
		for k, v := range replicationState.LastReplicationInfo {
			lastReplicationInfo[k] = createReplicationInfoMap(v)
		}

		batch.Query(templateUpdateWorkflowExecutionWithReplicationQuery,
			domainID,
			workflowID,
			runID,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			executionInfo.CompletionEventBatchID,
			completionData,
			completionEncoding,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.LastEventTaskID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			executionInfo.LastUpdatedTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.SignalCount,
			executionInfo.HistorySize,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionStartedTimestamp,
			executionInfo.DecisionScheduledTimestamp,
			executionInfo.DecisionOriginalScheduledTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.AutoResetPoints.Data,
			executionInfo.AutoResetPoints.GetEncoding(),
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			defaultEventStoreVersionValue,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.SearchAttributes,
			executionInfo.Memo,
			replicationState.CurrentVersion,
			replicationState.StartVersion,
			replicationState.LastWriteVersion,
			replicationState.LastWriteEventID,
			lastReplicationInfo,
			executionInfo.NextEventID,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Update workflow execution with both version histories and replication state."),
		}
	}

	return nil
}

func applyTasks(
	batch *gocql.Batch,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
	transferTasks []p.Task,
	replicationTasks []p.Task,
	timerTasks []p.Task,
) error {

	if err := createTransferTasks(
		batch,
		transferTasks,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := createReplicationTasks(
		batch,
		replicationTasks,
		shardID,
		domainID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	return createTimerTasks(
		batch,
		timerTasks,
		shardID,
		domainID,
		workflowID,
		runID,
	)
}

func createTransferTasks(
	batch *gocql.Batch,
	transferTasks []p.Task,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	targetDomainID := domainID
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		targetWorkflowID := p.TransferTaskTransferTargetWorkflowID
		targetRunID := p.TransferTaskTransferTargetRunID
		targetChildWorkflowOnly := false
		recordVisibility := false

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			targetDomainID = task.(*p.ActivityTask).DomainID
			taskList = task.(*p.ActivityTask).TaskList
			scheduleID = task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			targetDomainID = task.(*p.DecisionTask).DomainID
			taskList = task.(*p.DecisionTask).TaskList
			scheduleID = task.(*p.DecisionTask).ScheduleID
			recordVisibility = task.(*p.DecisionTask).RecordVisibility

		case p.TransferTaskTypeCancelExecution:
			targetDomainID = task.(*p.CancelExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.CancelExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			targetDomainID = task.(*p.SignalExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.SignalExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			targetDomainID = task.(*p.StartChildExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case p.TransferTaskTypeCloseExecution,
			p.TransferTaskTypeRecordWorkflowStarted,
			p.TransferTaskTypeResetWorkflow,
			p.TransferTaskTypeUpsertWorkflowSearchAttributes:
			// No explicit property needs to be set

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknow transfer type: %v", task.GetType()),
			}
		}

		batch.Query(templateCreateTransferTaskQuery,
			shardID,
			rowTypeTransferTask,
			rowTypeTransferDomainID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			domainID,
			workflowID,
			runID,
			task.GetVisibilityTimestamp(),
			task.GetTaskID(),
			targetDomainID,
			targetWorkflowID,
			targetRunID,
			targetChildWorkflowOnly,
			taskList,
			task.GetType(),
			scheduleID,
			recordVisibility,
			task.GetVersion(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createReplicationTasks(
	batch *gocql.Batch,
	replicationTasks []p.Task,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		var lastReplicationInfo map[string]map[string]interface{}
		activityScheduleID := common.EmptyEventID
		var branchToken, newRunBranchToken []byte
		resetWorkflow := false

		switch task.GetType() {
		case p.ReplicationTaskTypeHistory:
			histTask := task.(*p.HistoryReplicationTask)
			branchToken = histTask.BranchToken
			newRunBranchToken = histTask.NewRunBranchToken
			firstEventID = histTask.FirstEventID
			nextEventID = histTask.NextEventID
			version = task.GetVersion()
			lastReplicationInfo = make(map[string]map[string]interface{})
			for k, v := range histTask.LastReplicationInfo {
				lastReplicationInfo[k] = createReplicationInfoMap(v)
			}
			resetWorkflow = histTask.ResetWorkflow

		case p.ReplicationTaskTypeSyncActivity:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			// cassandra does not like null
			lastReplicationInfo = make(map[string]map[string]interface{})

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknow replication type: %v", task.GetType()),
			}
		}

		batch.Query(templateCreateReplicationTaskQuery,
			shardID,
			rowTypeReplicationTask,
			rowTypeReplicationDomainID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			domainID,
			workflowID,
			runID,
			task.GetTaskID(),
			task.GetType(),
			firstEventID,
			nextEventID,
			version,
			lastReplicationInfo,
			activityScheduleID,
			defaultEventStoreVersionValue,
			branchToken,
			resetWorkflow,
			defaultEventStoreVersionValue,
			newRunBranchToken,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createTimerTasks(
	batch *gocql.Batch,
	timerTasks []p.Task,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	for _, task := range timerTasks {
		var eventID int64
		var attempt int64

		timeoutType := 0

		switch t := task.(type) {
		case *p.DecisionTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.ScheduleAttempt

		case *p.ActivityTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.Attempt

		case *p.UserTimerTask:
			eventID = t.EventID

		case *p.ActivityRetryTimerTask:
			eventID = t.EventID
			attempt = int64(t.Attempt)

		case *p.WorkflowBackoffTimerTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType

		case *p.WorkflowTimeoutTask:
			// noop

		case *p.DeleteHistoryEventTask:
			// noop

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknow timer type: %v", task.GetType()),
			}
		}

		// Ignoring possible type cast errors.
		ts := p.UnixNanoToDBTimestamp(task.GetVisibilityTimestamp().UnixNano())

		batch.Query(templateCreateTimerTaskQuery,
			shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			domainID,
			workflowID,
			runID,
			ts,
			task.GetTaskID(),
			task.GetType(),
			timeoutType,
			eventID,
			attempt,
			task.GetVersion(),
			ts,
			task.GetTaskID())
	}

	return nil
}

func createOrUpdateCurrentExecution(
	batch *gocql.Batch,
	createMode p.CreateWorkflowMode,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
	state int,
	closeStatus int,
	createRequestID string,
	startVersion int64,
	lastWriteVersion int64,
	previousRunID string,
	previousLastWriteVersion int64,
) error {

	switch createMode {
	case p.CreateWorkflowModeContinueAsNew:
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			runID,
			runID,
			createRequestID,
			state,
			closeStatus,
			startVersion,
			lastWriteVersion,
			lastWriteVersion,
			state,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			previousRunID,
		)
	case p.CreateWorkflowModeWorkflowIDReuse:
		batch.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
			runID,
			runID,
			createRequestID,
			state,
			closeStatus,
			startVersion,
			lastWriteVersion,
			lastWriteVersion,
			state,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			previousRunID,
			previousLastWriteVersion,
			p.WorkflowStateCompleted,
		)
	case p.CreateWorkflowModeBrandNew:
		batch.Query(templateCreateCurrentWorkflowExecutionQuery,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			runID,
			runID,
			createRequestID,
			state,
			closeStatus,
			startVersion,
			lastWriteVersion,
			lastWriteVersion,
			state,
		)
	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("unknown mode: %v", createMode),
		}
	}

	return nil
}

func updateActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	for _, a := range activityInfos {
		scheduledEventData, scheduleEncoding := p.FromDataBlob(a.ScheduledEvent)
		startedEventData, startEncoding := p.FromDataBlob(a.StartedEvent)
		if a.StartedEvent != nil && scheduleEncoding != startEncoding {
			return p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", scheduleEncoding, startEncoding))
		}

		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.Version,
			a.ScheduleID,
			a.ScheduledEventBatchID,
			scheduledEventData,
			a.ScheduledTime,
			a.StartedID,
			startedEventData,
			a.StartedTime,
			a.ActivityID,
			a.RequestID,
			a.Details,
			a.ScheduleToStartTimeout,
			a.ScheduleToCloseTimeout,
			a.StartToCloseTimeout,
			a.HeartbeatTimeout,
			a.CancelRequested,
			a.CancelRequestID,
			a.LastHeartBeatUpdatedTime,
			a.TimerTaskStatus,
			a.Attempt,
			a.TaskList,
			a.StartedIdentity,
			a.HasRetryPolicy,
			a.InitialInterval,
			a.BackoffCoefficient,
			a.MaximumInterval,
			a.ExpirationTime,
			a.MaximumAttempts,
			a.NonRetriableErrors,
			a.LastFailureReason,
			a.LastWorkerIdentity,
			a.LastFailureDetails,
			scheduleEncoding,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteInfo := range deleteInfos {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteInfo,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func deleteBufferedEvents(
	batch *gocql.Batch,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateDeleteBufferedEventsQuery,
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

}

func resetActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	infoMap, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	batch.Query(templateResetActivityInfoQuery,
		infoMap,
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateTimerInfos(
	batch *gocql.Batch,
	timerInfos []*p.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	for _, a := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			a.TimerID,
			a.Version,
			a.TimerID,
			a.StartedID,
			a.ExpiryTime,
			a.TaskID,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, t := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			t,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetTimerInfos(
	batch *gocql.Batch,
	timerInfos []*p.TimerInfo,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetTimerInfoQuery,
		resetTimerInfoMap(timerInfos),
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos []*p.InternalChildExecutionInfo,
	deleteInfo *int64,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	for _, c := range childExecutionInfos {
		initiatedEventData, initiatedEncoding := p.FromDataBlob(c.InitiatedEvent)
		startedEventData, startEncoding := p.FromDataBlob(c.StartedEvent)
		if c.StartedEvent != nil && initiatedEncoding != startEncoding {
			return p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", initiatedEncoding, startEncoding))
		}

		startedRunID := emptyRunID
		if c.StartedRunID != "" {
			startedRunID = c.StartedRunID
		}

		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEventBatchID,
			initiatedEventData,
			c.StartedID,
			c.StartedWorkflowID,
			startedRunID,
			startedEventData,
			c.CreateRequestID,
			initiatedEncoding,
			c.DomainName,
			c.WorkflowTypeName,
			int32(c.ParentClosePolicy),
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	// deleteInfo is the initiatedID for ChildInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			*deleteInfo,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos []*p.InternalChildExecutionInfo,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) error {

	infoMap, err := resetChildExecutionInfoMap(childExecutionInfos)
	if err != nil {
		return err
	}
	batch.Query(templateResetChildExecutionInfoQuery,
		infoMap,
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*p.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	for _, c := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEventBatchID,
			c.CancelRequestID,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	// deleteInfo is the initiatedID for RequestCancelInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			*deleteInfo,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*p.RequestCancelInfo,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetRequestCancelInfoQuery,
		resetRequestCancelInfoMap(requestCancelInfos),
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateSignalInfos(
	batch *gocql.Batch,
	signalInfos []*p.SignalInfo,
	deleteInfo *int64,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	for _, c := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEventBatchID,
			c.SignalRequestID,
			c.SignalName,
			c.Input,
			c.Control,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	// deleteInfo is the initiatedID for SignalInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteSignalInfoQuery,
			*deleteInfo,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetSignalInfos(
	batch *gocql.Batch,
	signalInfos []*p.SignalInfo,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalInfoQuery,
		resetSignalInfoMap(signalInfos),
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateSignalsRequested(
	batch *gocql.Batch,
	signalReqIDs []string,
	deleteSignalReqID string,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			signalReqIDs,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	if deleteSignalReqID != "" {
		req := []string{deleteSignalReqID} // for cassandra set binding
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			req,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetSignalRequested(
	batch *gocql.Batch,
	signalRequested []string,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalRequestedQuery,
		signalRequested,
		shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateBufferedEvents(
	batch *gocql.Batch,
	newBufferedEvents *p.DataBlob,
	clearBufferedEvents bool,
	shardID int,
	domainID string,
	workflowID string,
	runID string,
) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.Encoding
		values["version"] = int64(0)
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func createShardInfo(
	currentCluster string,
	result map[string]interface{},
) *p.ShardInfo {

	info := &p.ShardInfo{}
	for k, v := range result {
		switch k {
		case "shard_id":
			info.ShardID = v.(int)
		case "owner":
			info.Owner = v.(string)
		case "range_id":
			info.RangeID = v.(int64)
		case "stolen_since_renew":
			info.StolenSinceRenew = v.(int)
		case "updated_at":
			info.UpdatedAt = v.(time.Time)
		case "replication_ack_level":
			info.ReplicationAckLevel = v.(int64)
		case "transfer_ack_level":
			info.TransferAckLevel = v.(int64)
		case "timer_ack_level":
			info.TimerAckLevel = v.(time.Time)
		case "cluster_transfer_ack_level":
			info.ClusterTransferAckLevel = v.(map[string]int64)
		case "cluster_timer_ack_level":
			info.ClusterTimerAckLevel = v.(map[string]time.Time)
		case "domain_notification_version":
			info.DomainNotificationVersion = v.(int64)
		case "cluster_replication_level":
			info.ClusterReplicationLevel = v.(map[string]int64)
		}
	}

	if info.ClusterTransferAckLevel == nil {
		info.ClusterTransferAckLevel = map[string]int64{
			currentCluster: info.TransferAckLevel,
		}
	}
	if info.ClusterTimerAckLevel == nil {
		info.ClusterTimerAckLevel = map[string]time.Time{
			currentCluster: info.TimerAckLevel,
		}
	}
	if info.ClusterReplicationLevel == nil {
		info.ClusterReplicationLevel = make(map[string]int64)
	}

	return info
}

func createWorkflowExecutionInfo(
	result map[string]interface{},
) *p.InternalWorkflowExecutionInfo {

	info := &p.InternalWorkflowExecutionInfo{}
	var completionEventData []byte
	var completionEventEncoding common.EncodingType
	var autoResetPoints []byte
	var autoResetPointsEncoding common.EncodingType

	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "parent_domain_id":
			info.ParentDomainID = v.(gocql.UUID).String()
			if info.ParentDomainID == emptyDomainID {
				info.ParentDomainID = ""
			}
		case "parent_workflow_id":
			info.ParentWorkflowID = v.(string)
		case "parent_run_id":
			info.ParentRunID = v.(gocql.UUID).String()
			if info.ParentRunID == emptyRunID {
				info.ParentRunID = ""
			}
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "completion_event_batch_id":
			info.CompletionEventBatchID = v.(int64)
		case "completion_event":
			completionEventData = v.([]byte)
		case "completion_event_data_encoding":
			completionEventEncoding = common.EncodingType(v.(string))
		case "auto_reset_points":
			autoResetPoints = v.([]byte)
		case "auto_reset_points_encoding":
			autoResetPointsEncoding = common.EncodingType(v.(string))
		case "task_list":
			info.TaskList = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		case "workflow_timeout":
			info.WorkflowTimeout = int32(v.(int))
		case "decision_task_timeout":
			info.DecisionTimeoutValue = int32(v.(int))
		case "execution_context":
			info.ExecutionContext = v.([]byte)
		case "state":
			info.State = v.(int)
		case "close_status":
			info.CloseStatus = v.(int)
		case "last_first_event_id":
			info.LastFirstEventID = v.(int64)
		case "last_event_task_id":
			info.LastEventTaskID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "last_processed_event":
			info.LastProcessedEvent = v.(int64)
		case "start_time":
			info.StartTimestamp = v.(time.Time)
		case "last_updated_time":
			info.LastUpdatedTimestamp = v.(time.Time)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "signal_count":
			info.SignalCount = int32(v.(int))
		case "history_size":
			info.HistorySize = v.(int64)
		case "decision_version":
			info.DecisionVersion = v.(int64)
		case "decision_schedule_id":
			info.DecisionScheduleID = v.(int64)
		case "decision_started_id":
			info.DecisionStartedID = v.(int64)
		case "decision_request_id":
			info.DecisionRequestID = v.(string)
		case "decision_timeout":
			info.DecisionTimeout = int32(v.(int))
		case "decision_attempt":
			info.DecisionAttempt = v.(int64)
		case "decision_timestamp":
			info.DecisionStartedTimestamp = v.(int64)
		case "decision_scheduled_timestamp":
			info.DecisionScheduledTimestamp = v.(int64)
		case "decision_original_scheduled_timestamp":
			info.DecisionOriginalScheduledTimestamp = v.(int64)
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		case "sticky_task_list":
			info.StickyTaskList = v.(string)
		case "sticky_schedule_to_start_timeout":
			info.StickyScheduleToStartTimeout = int32(v.(int))
		case "client_library_version":
			info.ClientLibraryVersion = v.(string)
		case "client_feature_version":
			info.ClientFeatureVersion = v.(string)
		case "client_impl":
			info.ClientImpl = v.(string)
		case "attempt":
			info.Attempt = int32(v.(int))
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = int32(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = int32(v.(int))
		case "max_attempts":
			info.MaximumAttempts = int32(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "cron_schedule":
			info.CronSchedule = v.(string)
		case "expiration_seconds":
			info.ExpirationSeconds = int32(v.(int))
		case "search_attributes":
			info.SearchAttributes = v.(map[string][]byte)
		case "memo":
			info.Memo = v.(map[string][]byte)
		}
	}
	info.CompletionEvent = p.NewDataBlob(completionEventData, completionEventEncoding)
	info.AutoResetPoints = p.NewDataBlob(autoResetPoints, autoResetPointsEncoding)
	return info
}

func createReplicationState(
	result map[string]interface{},
) *p.ReplicationState {

	if len(result) == 0 {
		return nil
	}

	info := &p.ReplicationState{}
	for k, v := range result {
		switch k {
		case "current_version":
			info.CurrentVersion = v.(int64)
		case "start_version":
			info.StartVersion = v.(int64)
		case "last_write_version":
			info.LastWriteVersion = v.(int64)
		case "last_write_event_id":
			info.LastWriteEventID = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		}
	}

	return info
}

func createTransferTaskInfo(
	result map[string]interface{},
) *p.TransferTaskInfo {

	info := &p.TransferTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "target_domain_id":
			info.TargetDomainID = v.(gocql.UUID).String()
		case "target_workflow_id":
			info.TargetWorkflowID = v.(string)
		case "target_run_id":
			info.TargetRunID = v.(gocql.UUID).String()
			if info.TargetRunID == p.TransferTaskTransferTargetRunID {
				info.TargetRunID = ""
			}
		case "target_child_workflow_only":
			info.TargetChildWorkflowOnly = v.(bool)
		case "task_list":
			info.TaskList = v.(string)
		case "type":
			info.TaskType = v.(int)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "record_visibility":
			info.RecordVisibility = v.(bool)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationTaskInfo(
	result map[string]interface{},
) *p.ReplicationTaskInfo {

	info := &p.ReplicationTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "first_event_id":
			info.FirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "version":
			info.Version = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		case "scheduled_id":
			info.ScheduledID = v.(int64)
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "reset_workflow":
			info.ResetWorkflow = v.(bool)
		case "new_run_branch_token":
			info.NewRunBranchToken = v.([]byte)
		}
	}

	return info
}

func createActivityInfo(
	domainID string,
	result map[string]interface{},
) *p.InternalActivityInfo {

	info := &p.InternalActivityInfo{}
	var sharedEncoding common.EncodingType
	var scheduledEventData, startedEventData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "scheduled_event_batch_id":
			info.ScheduledEventBatchID = v.(int64)
		case "scheduled_event":
			scheduledEventData = v.([]byte)
		case "scheduled_time":
			info.ScheduledTime = v.(time.Time)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			startedEventData = v.([]byte)
		case "started_time":
			info.StartedTime = v.(time.Time)
		case "activity_id":
			info.ActivityID = v.(string)
		case "request_id":
			info.RequestID = v.(string)
		case "details":
			info.Details = v.([]byte)
		case "schedule_to_start_timeout":
			info.ScheduleToStartTimeout = int32(v.(int))
		case "schedule_to_close_timeout":
			info.ScheduleToCloseTimeout = int32(v.(int))
		case "start_to_close_timeout":
			info.StartToCloseTimeout = int32(v.(int))
		case "heart_beat_timeout":
			info.HeartbeatTimeout = int32(v.(int))
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(int64)
		case "last_hb_updated_time":
			info.LastHeartBeatUpdatedTime = v.(time.Time)
		case "timer_task_status":
			info.TimerTaskStatus = int32(v.(int))
		case "attempt":
			info.Attempt = int32(v.(int))
		case "task_list":
			info.TaskList = v.(string)
		case "started_identity":
			info.StartedIdentity = v.(string)
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = (int32)(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = (int32)(v.(int))
		case "max_attempts":
			info.MaximumAttempts = (int32)(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		case "last_failure_reason":
			info.LastFailureReason = v.(string)
		case "last_worker_identity":
			info.LastWorkerIdentity = v.(string)
		case "last_failure_details":
			info.LastFailureDetails = v.([]byte)
		case "event_data_encoding":
			sharedEncoding = common.EncodingType(v.(string))
		}
	}
	info.DomainID = domainID
	info.ScheduledEvent = p.NewDataBlob(scheduledEventData, sharedEncoding)
	info.StartedEvent = p.NewDataBlob(startedEventData, sharedEncoding)

	return info
}

func createTimerInfo(
	result map[string]interface{},
) *p.TimerInfo {

	info := &p.TimerInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "timer_id":
			info.TimerID = v.(string)
		case "started_id":
			info.StartedID = v.(int64)
		case "expiry_time":
			info.ExpiryTime = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		}
	}
	return info
}

func createChildExecutionInfo(
	result map[string]interface{},
) *p.InternalChildExecutionInfo {

	info := &p.InternalChildExecutionInfo{}
	var encoding common.EncodingType
	var initiatedData []byte
	var startedData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event_batch_id":
			info.InitiatedEventBatchID = v.(int64)
		case "initiated_event":
			initiatedData = v.([]byte)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_workflow_id":
			info.StartedWorkflowID = v.(string)
		case "started_run_id":
			info.StartedRunID = v.(gocql.UUID).String()
		case "started_event":
			startedData = v.([]byte)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "event_data_encoding":
			encoding = common.EncodingType(v.(string))
		case "domain_name":
			info.DomainName = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		case "parent_close_policy":
			info.ParentClosePolicy = workflow.ParentClosePolicy(v.(int))
		}
	}
	info.InitiatedEvent = p.NewDataBlob(initiatedData, encoding)
	info.StartedEvent = p.NewDataBlob(startedData, encoding)
	return info
}

func createRequestCancelInfo(
	result map[string]interface{},
) *p.RequestCancelInfo {

	info := &p.RequestCancelInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event_batch_id":
			info.InitiatedEventBatchID = v.(int64)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		}
	}

	return info
}

func createSignalInfo(
	result map[string]interface{},
) *p.SignalInfo {

	info := &p.SignalInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event_batch_id":
			info.InitiatedEventBatchID = v.(int64)
		case "signal_request_id":
			info.SignalRequestID = v.(gocql.UUID).String()
		case "signal_name":
			info.SignalName = v.(string)
		case "input":
			info.Input = v.([]byte)
		case "control":
			info.Control = v.([]byte)
		}
	}

	return info
}

func resetActivityInfoMap(
	activityInfos []*p.InternalActivityInfo,
) (map[int64]map[string]interface{}, error) {

	aMap := make(map[int64]map[string]interface{})
	for _, a := range activityInfos {
		scheduledEventData, scheduleEncoding := p.FromDataBlob(a.ScheduledEvent)
		startedEventData, startEncoding := p.FromDataBlob(a.StartedEvent)
		if a.StartedEvent != nil && scheduleEncoding != startEncoding {
			return nil, p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", scheduleEncoding, startEncoding))
		}
		aInfo := make(map[string]interface{})
		aInfo["version"] = a.Version
		aInfo["event_data_encoding"] = scheduleEncoding
		aInfo["schedule_id"] = a.ScheduleID
		aInfo["scheduled_event_batch_id"] = a.ScheduledEventBatchID
		aInfo["scheduled_event"] = scheduledEventData
		aInfo["scheduled_time"] = a.ScheduledTime
		aInfo["started_id"] = a.StartedID
		aInfo["started_event"] = startedEventData
		aInfo["started_time"] = a.StartedTime
		aInfo["activity_id"] = a.ActivityID
		aInfo["request_id"] = a.RequestID
		aInfo["details"] = a.Details
		aInfo["schedule_to_start_timeout"] = a.ScheduleToStartTimeout
		aInfo["schedule_to_close_timeout"] = a.ScheduleToCloseTimeout
		aInfo["start_to_close_timeout"] = a.StartToCloseTimeout
		aInfo["heart_beat_timeout"] = a.HeartbeatTimeout
		aInfo["cancel_requested"] = a.CancelRequested
		aInfo["cancel_request_id"] = a.CancelRequestID
		aInfo["last_hb_updated_time"] = a.LastHeartBeatUpdatedTime
		aInfo["timer_task_status"] = a.TimerTaskStatus
		aInfo["attempt"] = a.Attempt
		aInfo["task_list"] = a.TaskList
		aInfo["started_identity"] = a.StartedIdentity
		aInfo["has_retry_policy"] = a.HasRetryPolicy
		aInfo["init_interval"] = a.InitialInterval
		aInfo["backoff_coefficient"] = a.BackoffCoefficient
		aInfo["max_interval"] = a.MaximumInterval
		aInfo["expiration_time"] = a.ExpirationTime
		aInfo["max_attempts"] = a.MaximumAttempts
		aInfo["non_retriable_errors"] = a.NonRetriableErrors
		aInfo["last_failure_reason"] = a.LastFailureReason
		aInfo["last_worker_identity"] = a.LastWorkerIdentity
		aInfo["last_failure_details"] = a.LastFailureDetails

		aMap[a.ScheduleID] = aInfo
	}

	return aMap, nil
}

func resetTimerInfoMap(
	timerInfos []*p.TimerInfo,
) map[string]map[string]interface{} {

	tMap := make(map[string]map[string]interface{})
	for _, t := range timerInfos {
		tInfo := make(map[string]interface{})
		tInfo["version"] = t.Version
		tInfo["timer_id"] = t.TimerID
		tInfo["started_id"] = t.StartedID
		tInfo["expiry_time"] = t.ExpiryTime
		tInfo["task_id"] = t.TaskID

		tMap[t.TimerID] = tInfo
	}

	return tMap
}

func resetChildExecutionInfoMap(
	childExecutionInfos []*p.InternalChildExecutionInfo,
) (map[int64]map[string]interface{}, error) {

	cMap := make(map[int64]map[string]interface{})
	for _, c := range childExecutionInfos {
		cInfo := make(map[string]interface{})
		initiatedEventData, initiatedEncoding := p.FromDataBlob(c.InitiatedEvent)
		startedEventData, startEncoding := p.FromDataBlob(c.StartedEvent)
		if c.StartedEvent != nil && initiatedEncoding != startEncoding {
			return nil, p.NewCadenceSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", initiatedEncoding, startEncoding))
		}
		cInfo["version"] = c.Version
		cInfo["event_data_encoding"] = initiatedEncoding
		cInfo["initiated_id"] = c.InitiatedID
		cInfo["initiated_event_batch_id"] = c.InitiatedEventBatchID
		cInfo["initiated_event"] = initiatedEventData
		cInfo["started_id"] = c.StartedID
		cInfo["started_event"] = startedEventData
		cInfo["create_request_id"] = c.CreateRequestID
		cInfo["started_workflow_id"] = c.StartedWorkflowID
		startedRunID := emptyRunID
		if c.StartedRunID != "" {
			startedRunID = c.StartedRunID
		}
		cInfo["started_run_id"] = startedRunID
		cInfo["domain_name"] = c.DomainName
		cInfo["workflow_type_name"] = c.WorkflowTypeName
		cInfo["parent_close_policy"] = int32(c.ParentClosePolicy)

		cMap[c.InitiatedID] = cInfo
	}

	return cMap, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos []*p.RequestCancelInfo,
) map[int64]map[string]interface{} {

	rcMap := make(map[int64]map[string]interface{})
	for _, rc := range requestCancelInfos {
		rcInfo := make(map[string]interface{})
		rcInfo["version"] = rc.Version
		rcInfo["initiated_id"] = rc.InitiatedID
		rcInfo["initiated_event_batch_id"] = rc.InitiatedEventBatchID
		rcInfo["cancel_request_id"] = rc.CancelRequestID

		rcMap[rc.InitiatedID] = rcInfo
	}

	return rcMap
}

func resetSignalInfoMap(
	signalInfos []*p.SignalInfo,
) map[int64]map[string]interface{} {

	sMap := make(map[int64]map[string]interface{})
	for _, s := range signalInfos {
		sInfo := make(map[string]interface{})
		sInfo["version"] = s.Version
		sInfo["initiated_id"] = s.InitiatedID
		sInfo["initiated_event_batch_id"] = s.InitiatedEventBatchID
		sInfo["signal_request_id"] = s.SignalRequestID
		sInfo["signal_name"] = s.SignalName
		sInfo["input"] = s.Input
		sInfo["control"] = s.Control

		sMap[s.InitiatedID] = sInfo
	}

	return sMap
}

func createHistoryEventBatchBlob(
	result map[string]interface{},
) *p.DataBlob {

	eventBatch := &p.DataBlob{Encoding: common.EncodingTypeJSON}
	for k, v := range result {
		switch k {
		case "encoding_type":
			eventBatch.Encoding = common.EncodingType(v.(string))
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func createTaskInfo(
	result map[string]interface{},
) *p.TaskInfo {

	info := &p.TaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "created_time":
			info.CreatedTime = v.(time.Time)
		}
	}

	return info
}

func createTimerTaskInfo(
	result map[string]interface{},
) *p.TimerTaskInfo {

	info := &p.TimerTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "timeout_type":
			info.TimeoutType = v.(int)
		case "event_id":
			info.EventID = v.(int64)
		case "schedule_attempt":
			info.ScheduleAttempt = v.(int64)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationInfo(
	result map[string]interface{},
) *p.ReplicationInfo {

	info := &p.ReplicationInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "last_event_id":
			info.LastEventID = v.(int64)
		}
	}

	return info
}

func createReplicationInfoMap(
	info *p.ReplicationInfo,
) map[string]interface{} {

	rInfoMap := make(map[string]interface{})
	rInfoMap["version"] = info.Version
	rInfoMap["last_event_id"] = info.LastEventID

	return rInfoMap
}

func isTimeoutError(err error) bool {
	if err == gocql.ErrTimeoutNoResponse {
		return true
	}
	if err == gocql.ErrConnectionClosed {
		return true
	}
	_, ok := err.(*gocql.RequestErrWriteTimeout)
	return ok
}

func isThrottlingError(err error) bool {
	if req, ok := err.(gocql.RequestError); ok {
		// gocql does not expose the constant errOverloaded = 0x1001
		return req.Code() == 0x1001
	}
	return false
}
