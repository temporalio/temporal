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

package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/checksum"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

func applyWorkflowMutationBatch(
	batch *gocql.Batch,
	shardID int,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	executionInfo := workflowMutation.ExecutionInfo
	replicationState := workflowMutation.ReplicationState
	versionHistories := workflowMutation.VersionHistories
	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowMutation.Condition

	startVersion := workflowMutation.StartVersion
	lastWriteVersion := workflowMutation.LastWriteVersion
	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	if err := updateExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		cqlNowTimestampMillis,
		condition,
		workflowMutation.Checksum,
		startVersion,
		currentVersion,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfo,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfo,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfo,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedID,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	updateBufferedEvents(
		batch,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		namespaceID,
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
	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	condition := workflowSnapshot.Condition

	startVersion := workflowSnapshot.StartVersion
	lastWriteVersion := workflowSnapshot.LastWriteVersion
	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	if err := updateExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		cqlNowTimestampMillis,
		condition,
		workflowSnapshot.Checksum,
		startVersion,
		currentVersion,
	); err != nil {
		return err
	}

	if err := resetActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	resetSignalRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	deleteBufferedEvents(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		namespaceID,
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
	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	startVersion := workflowSnapshot.StartVersion
	lastWriteVersion := workflowSnapshot.LastWriteVersion
	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	if err := createExecution(
		batch,
		shardID,
		executionInfo,
		replicationState,
		versionHistories,
		workflowSnapshot.Checksum,
		cqlNowTimestampMillis,
		startVersion,
		currentVersion,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		"",
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		namespaceID,
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
	versionHistories *serialization.DataBlob,
	checksum checksum.Checksum,
	cqlNowTimestampMillis int64,
	startVersion int64,
	currentVersion int64,
) error {

	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		executionInfo.State,
		executionInfo.Status); err != nil {
		return err
	}

	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	// TODO we should set the start time and last update time on business logic layer
	executionInfo.StartTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))
	executionInfo.LastUpdateTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	protoExecution, protoState, err := p.InternalWorkflowExecutionInfoToProto(executionInfo, startVersion, currentVersion, replicationState, versionHistories)
	if err != nil {
		return err
	}

	executionDatablob, err := serialization.WorkflowExecutionInfoToBlob(protoExecution)
	if err != nil {
		return err
	}

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(protoState)
	if err != nil {
		return err
	}

	checksumDatablob, err := serialization.ChecksumToBlob(checksum.ToProto())
	if err != nil {
		return err
	}

	if replicationState == nil && versionHistories == nil {
		// Cross DC feature is currently disabled so we will be creating workflow executions without replication state
		batch.Query(templateCreateWorkflowExecutionQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			rowTypeExecution,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String())
	} else if versionHistories != nil {
		// TODO also need to set the start / current / last write version
		versionHistoriesData, versionHistoriesEncoding := p.FromDataBlob(versionHistories)
		batch.Query(templateCreateWorkflowExecutionWithVersionHistoriesQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			rowTypeExecution,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			versionHistoriesData,
			versionHistoriesEncoding,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String())
	} else if replicationState != nil {
		replicationVersions, err := serialization.ReplicationVersionsToBlob(replicationState.GenerateVersionProto())
		if err != nil {
			return err
		}
		batch.Query(templateCreateWorkflowExecutionWithReplicationQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			rowTypeExecution,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			replicationVersions.Data,
			replicationVersions.Encoding.String(),
			executionInfo.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String())
	} else {
		return serviceerror.NewInternal(fmt.Sprintf("Create workflow execution with both version histories and replication state."))
	}
	return nil
}

func updateExecution(
	batch *gocql.Batch,
	shardID int,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *serialization.DataBlob,
	cqlNowTimestampMillis int64,
	condition int64,
	checksum checksum.Checksum,
	startVersion int64,
	currentVersion int64,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionInfo.State,
		executionInfo.Status); err != nil {
		return err
	}

	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	// TODO we should set the last update time on business logic layer
	executionInfo.LastUpdateTimestamp = time.Unix(0, p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	protoExecution, protoState, err := p.InternalWorkflowExecutionInfoToProto(executionInfo, startVersion, currentVersion, replicationState, versionHistories)
	if err != nil {
		return err
	}

	executionDatablob, err := serialization.WorkflowExecutionInfoToBlob(protoExecution)
	if err != nil {
		return err
	}

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(protoState)
	if err != nil {
		return err
	}

	checksumDatablob, err := serialization.ChecksumToBlob(checksum.ToProto())
	if err != nil {
		return err
	}

	if replicationState == nil && versionHistories == nil {
		// Updates will be called with null ReplicationState while the feature is disabled
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			executionInfo.NextEventID,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if versionHistories != nil {
		// TODO also need to set the start / current / last write version
		versionHistoriesData, versionHistoriesEncoding := p.FromDataBlob(versionHistories)
		batch.Query(templateUpdateWorkflowExecutionWithVersionHistoriesQuery,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			executionInfo.NextEventID,
			versionHistoriesData,
			versionHistoriesEncoding,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if replicationState != nil {
		replicationVersions, err := serialization.ReplicationVersionsToBlob(replicationState.GenerateVersionProto())
		if err != nil {
			return err
		}
		batch.Query(templateUpdateWorkflowExecutionWithReplicationQuery,
			executionDatablob.Data,
			executionDatablob.Encoding.String(),
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			replicationVersions.Data,
			replicationVersions.Encoding.String(),
			executionInfo.NextEventID,
			checksumDatablob.Data,
			checksumDatablob.Encoding.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else {
		return serviceerror.NewInternal(fmt.Sprintf("Update workflow execution with both version histories and replication state."))
	}

	return nil
}

func applyTasks(
	batch *gocql.Batch,
	shardID int,
	namespaceID string,
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
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := createReplicationTasks(
		batch,
		replicationTasks,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	return createTimerTasks(
		batch,
		timerTasks,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)
}

func createTransferTasks(
	batch *gocql.Batch,
	transferTasks []p.Task,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	targetNamespaceID := namespaceID
	for _, task := range transferTasks {
		var taskQueue string
		var scheduleID int64
		targetWorkflowID := p.TransferTaskTransferTargetWorkflowID
		targetRunID := ""
		targetChildWorkflowOnly := false
		recordVisibility := false

		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			targetNamespaceID = task.(*p.ActivityTask).NamespaceID
			taskQueue = task.(*p.ActivityTask).TaskQueue
			scheduleID = task.(*p.ActivityTask).ScheduleID

		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			targetNamespaceID = task.(*p.WorkflowTask).NamespaceID
			taskQueue = task.(*p.WorkflowTask).TaskQueue
			scheduleID = task.(*p.WorkflowTask).ScheduleID
			recordVisibility = task.(*p.WorkflowTask).RecordVisibility

		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			targetNamespaceID = task.(*p.CancelExecutionTask).TargetNamespaceID
			targetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.CancelExecutionTask).TargetRunID

			targetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			targetNamespaceID = task.(*p.SignalExecutionTask).TargetNamespaceID
			targetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.SignalExecutionTask).TargetRunID

			targetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			targetNamespaceID = task.(*p.StartChildExecutionTask).TargetNamespaceID
			targetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
			enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED,
			enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW,
			enumsspb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			// No explicit property needs to be set

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow transfer type: %v", task.GetType()))
		}

		taskVisTs, err := types.TimestampProto(task.GetVisibilityTimestamp())

		if err != nil {
			return err
		}

		// todo ~~~ come back for record visibility
		p := &persistenceblobs.TransferTaskInfo{
			NamespaceId:             namespaceID,
			WorkflowId:              workflowID,
			RunId:                   runID,
			TaskType:                task.GetType(),
			TargetNamespaceId:       targetNamespaceID,
			TargetWorkflowId:        targetWorkflowID,
			TargetRunId:             targetRunID,
			TaskQueue:               taskQueue,
			TargetChildWorkflowOnly: targetChildWorkflowOnly,
			ScheduleId:              scheduleID,
			Version:                 task.GetVersion(),
			TaskId:                  task.GetTaskID(),
			VisibilityTime:          taskVisTs,
			RecordVisibility:        recordVisibility,
		}

		datablob, err := serialization.TransferTaskInfoToBlob(p)
		if err != nil {
			return err
		}
		batch.Query(templateCreateTransferTaskQuery,
			shardID,
			rowTypeTransferTask,
			rowTypeTransferNamespaceID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			datablob.Data,
			datablob.Encoding,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createReplicationTasks(
	batch *gocql.Batch,
	replicationTasks []p.Task,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion // nolint:ineffassign
		var lastReplicationInfo map[string]*replicationspb.ReplicationInfo
		activityScheduleID := common.EmptyEventID
		var branchToken, newRunBranchToken []byte
		resetWorkflow := false

		switch task.GetType() {
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			histTask := task.(*p.HistoryReplicationTask)
			branchToken = histTask.BranchToken
			newRunBranchToken = histTask.NewRunBranchToken
			firstEventID = histTask.FirstEventID
			nextEventID = histTask.NextEventID
			version = task.GetVersion()
			lastReplicationInfo = make(map[string]*replicationspb.ReplicationInfo)
			for k, v := range histTask.LastReplicationInfo {
				lastReplicationInfo[k] = v
			}
			resetWorkflow = histTask.ResetWorkflow

		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			// cassandra does not like null
			lastReplicationInfo = make(map[string]*replicationspb.ReplicationInfo)

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow replication type: %v", task.GetType()))
		}

		datablob, err := serialization.ReplicationTaskInfoToBlob(&persistenceblobs.ReplicationTaskInfo{
			NamespaceId:             namespaceID,
			WorkflowId:              workflowID,
			RunId:                   runID,
			TaskId:                  task.GetTaskID(),
			TaskType:                task.GetType(),
			Version:                 version,
			FirstEventId:            firstEventID,
			NextEventId:             nextEventID,
			ScheduledId:             activityScheduleID,
			EventStoreVersion:       p.EventStoreVersion,
			NewRunBranchToken:       newRunBranchToken,
			NewRunEventStoreVersion: p.EventStoreVersion,
			BranchToken:             branchToken,
			LastReplicationInfo:     lastReplicationInfo,
			ResetWorkflow:           resetWorkflow,
		})

		if err != nil {
			return err
		}

		batch.Query(templateCreateReplicationTaskQuery,
			shardID,
			rowTypeReplicationTask,
			rowTypeReplicationNamespaceID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			datablob.Data,
			datablob.Encoding,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createTimerTasks(
	batch *gocql.Batch,
	timerTasks []p.Task,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range timerTasks {
		eventID := int64(0)
		attempt := int64(1)

		timeoutType := enumspb.TIMEOUT_TYPE_UNSPECIFIED
		workflowBackoffType := enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED

		switch t := task.(type) {
		case *p.WorkflowTaskTimeoutTask:
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
			workflowBackoffType = t.WorkflowBackoffType

		case *p.WorkflowTimeoutTask:
			// noop

		case *p.DeleteHistoryEventTask:
			// noop

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow timer type: %v", task.GetType()))
		}

		// Ignoring possible type cast errors.
		goTs := task.GetVisibilityTimestamp()
		dbTs := p.UnixNanoToDBTimestamp(goTs.UnixNano())
		protoTs, err := types.TimestampProto(goTs)

		if err != nil {
			return err
		}

		datablob, err := serialization.TimerTaskInfoToBlob(&persistenceblobs.TimerTaskInfo{
			NamespaceId:         namespaceID,
			WorkflowId:          workflowID,
			RunId:               runID,
			TaskType:            task.GetType(),
			TimeoutType:         timeoutType,
			WorkflowBackoffType: workflowBackoffType,
			Version:             task.GetVersion(),
			ScheduleAttempt:     attempt,
			EventId:             eventID,
			TaskId:              task.GetTaskID(),
			VisibilityTime:      protoTs,
		})

		if err != nil {
			return err
		}

		batch.Query(templateCreateTimerTaskQuery,
			shardID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			datablob.Data,
			datablob.Encoding,
			dbTs,
			task.GetTaskID())
	}

	return nil
}

func createOrUpdateCurrentExecution(
	batch *gocql.Batch,
	createMode p.CreateWorkflowMode,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	createRequestID string,
	startVersion int64,
	lastWriteVersion int64,
	previousRunID string,
	previousLastWriteVersion int64,
) error {

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistenceblobs.WorkflowExecutionState{
		RunId:           runID,
		CreateRequestId: createRequestID,
		State:           state,
		Status:          status,
	})

	if err != nil {
		return err
	}

	replicationVersions, err := serialization.ReplicationVersionsToBlob(
		&persistenceblobs.ReplicationVersions{
			StartVersion:     &types.Int64Value{Value: startVersion},
			LastWriteVersion: &types.Int64Value{Value: startVersion},
		})

	if err != nil {
		return err
	}

	switch createMode {
	case p.CreateWorkflowModeContinueAsNew:
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			runID,
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			replicationVersions.Data,
			replicationVersions.Encoding.String(),
			lastWriteVersion,
			state,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			previousRunID,
		)
	case p.CreateWorkflowModeWorkflowIDReuse:
		batch.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
			runID,
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			replicationVersions.Data,
			replicationVersions.Encoding.String(),
			lastWriteVersion,
			state,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			previousRunID,
			previousLastWriteVersion,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		)
	case p.CreateWorkflowModeBrandNew:
		batch.Query(templateCreateCurrentWorkflowExecutionQuery,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			runID,
			executionStateDatablob.Data,
			executionStateDatablob.Encoding.String(),
			replicationVersions.Data,
			replicationVersions.Encoding.String(),
			lastWriteVersion,
			state,
		)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown mode: %v", createMode))
	}

	return nil
}

func updateActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	deleteInfos []int64,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, a := range activityInfos {
		if a.StartedEvent != nil && a.ScheduledEvent.Encoding != a.StartedEvent.Encoding {
			return p.NewSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}

		protoActivityInfo := a.ToProto()
		activityBlob, err := serialization.ActivityInfoToBlob(protoActivityInfo)
		if err != nil {
			return p.NewSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}

		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			activityBlob.Data,
			activityBlob.Encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
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
	namespaceID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateDeleteBufferedEventsQuery,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

}

func resetActivityInfos(
	batch *gocql.Batch,
	activityInfos []*p.InternalActivityInfo,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	batch.Query(templateResetActivityInfoQuery,
		infoMap,
		encoding,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateTimerInfos(
	batch *gocql.Batch,
	timerInfos []*persistenceblobs.TimerInfo,
	deleteInfos []string,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	for _, a := range timerInfos {
		datablob, err := serialization.TimerInfoToBlob(a)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateTimerInfoQuery,
			a.GetTimerId(),    // timermap key
			datablob.Data,     // timermap data
			datablob.Encoding, // timermap encoding
			shardID,           // where ...
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func resetTimerInfos(
	batch *gocql.Batch,
	timerInfos []*persistenceblobs.TimerInfo,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	timerMap, timerMapEncoding, err := resetTimerInfoMap(timerInfos)

	if err != nil {
		return err
	}

	batch.Query(templateResetTimerInfoQuery,
		timerMap,
		timerMapEncoding,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos []*p.InternalChildExecutionInfo,
	deleteInfo *int64,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, c := range childExecutionInfos {
		if c.StartedEvent != nil && c.InitiatedEvent.Encoding != c.StartedEvent.Encoding {
			return p.NewSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", c.InitiatedEvent.Encoding, c.StartedEvent.Encoding))
		}

		datablob, err := serialization.ChildExecutionInfoToBlob(c.ToProto())
		if err != nil {
			return nil
		}

		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			datablob.Data,
			datablob.Encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
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
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetChildExecutionInfoMap(childExecutionInfos)
	if err != nil {
		return err
	}
	batch.Query(templateResetChildExecutionInfoQuery,
		infoMap,
		encoding,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func updateRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*persistenceblobs.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, c := range requestCancelInfos {
		datablob, err := serialization.RequestCancelInfoToBlob(c)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateRequestCancelInfoQuery,
			c.GetInitiatedId(),
			datablob.Data,
			datablob.Encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func resetRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos []*persistenceblobs.RequestCancelInfo,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	rciMap, rciMapEncoding, err := resetRequestCancelInfoMap(requestCancelInfos)

	if err != nil {
		return err
	}

	batch.Query(templateResetRequestCancelInfoQuery,
		rciMap,
		rciMapEncoding,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateSignalInfos(
	batch *gocql.Batch,
	signalInfos []*persistenceblobs.SignalInfo,
	deleteInfo *int64,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, c := range signalInfos {
		datablob, err := serialization.SignalInfoToBlob(c)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateSignalInfoQuery,
			c.GetInitiatedId(),
			datablob.Data,
			datablob.Encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func resetSignalInfos(
	batch *gocql.Batch,
	signalInfos []*persistenceblobs.SignalInfo,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	sMap, sMapEncoding, err := resetSignalInfoMap(signalInfos)

	if err != nil {
		return err
	}

	batch.Query(templateResetSignalInfoQuery,
		sMap,
		sMapEncoding,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateSignalsRequested(
	batch *gocql.Batch,
	signalReqIDs []string,
	deleteSignalReqID string,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			signalReqIDs,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
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
	namespaceID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalRequestedQuery,
		signalRequested,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateBufferedEvents(
	batch *gocql.Batch,
	newBufferedEvents *serialization.DataBlob,
	clearBufferedEvents bool,
	shardID int,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			shardID,
			rowTypeExecution,
			namespaceID,
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
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func ReplicationStateFromProtos(wei *persistenceblobs.WorkflowExecutionInfo, rv *persistenceblobs.ReplicationVersions) *p.ReplicationState {
	if rv == nil && wei.ReplicationData == nil {
		return nil
	}

	info := &p.ReplicationState{}
	info.CurrentVersion = wei.CurrentVersion

	if rv != nil {
		info.StartVersion = rv.GetStartVersion().GetValue()
		info.LastWriteVersion = rv.GetLastWriteVersion().GetValue()
	}

	if wei.ReplicationData != nil {
		info.LastReplicationInfo = wei.ReplicationData.LastReplicationInfo
		info.LastWriteEventID = wei.ReplicationData.LastWriteEventId
	}

	if info.LastReplicationInfo == nil {
		info.LastReplicationInfo = make(map[string]*replicationspb.ReplicationInfo, 0)
	}

	return info
}

func resetActivityInfoMap(
	activityInfos []*p.InternalActivityInfo,
) (map[int64][]byte, common.EncodingType, error) {

	encoding := common.EncodingTypeUnknown
	aMap := make(map[int64][]byte)
	for _, a := range activityInfos {
		if a.StartedEvent != nil && a.ScheduledEvent.Encoding != a.StartedEvent.Encoding {
			return nil, common.EncodingTypeUnknown, p.NewSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}

		aBlob, err := serialization.ActivityInfoToBlob(a.ToProto())
		if err != nil {
			return nil, common.EncodingTypeUnknown, p.NewSerializationError(fmt.Sprintf("failed to serialize activity infos - ActivityId: %v", a.ActivityID))
		}

		aMap[a.ScheduleID] = aBlob.Data
		encoding = aBlob.Encoding
	}

	return aMap, encoding, nil
}

func resetTimerInfoMap(
	timerInfos []*persistenceblobs.TimerInfo,
) (map[string][]byte, common.EncodingType, error) {

	tMap := make(map[string][]byte)
	var encoding common.EncodingType
	for _, t := range timerInfos {
		datablob, err := serialization.TimerInfoToBlob(t)

		if err != nil {
			return nil, common.EncodingTypeUnknown, err
		}

		encoding = datablob.Encoding

		tMap[t.GetTimerId()] = datablob.Data
	}

	return tMap, encoding, nil
}

func resetChildExecutionInfoMap(
	childExecutionInfos []*p.InternalChildExecutionInfo,
) (map[int64][]byte, common.EncodingType, error) {

	cMap := make(map[int64][]byte)
	encoding := common.EncodingTypeUnknown
	for _, c := range childExecutionInfos {
		if c.StartedEvent != nil && c.InitiatedEvent.Encoding != c.StartedEvent.Encoding {
			return nil, common.EncodingTypeUnknown, p.NewSerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", c.InitiatedEvent.Encoding, c.StartedEvent.Encoding))
		}

		datablob, err := serialization.ChildExecutionInfoToBlob(c.ToProto())
		if err != nil {
			return nil, common.EncodingTypeUnknown, p.NewSerializationError(fmt.Sprintf("failed to serialize child execution infos - Execution: %v", c.InitiatedID))
		}
		cMap[c.InitiatedID] = datablob.Data
		encoding = datablob.Encoding
	}

	return cMap, encoding, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos []*persistenceblobs.RequestCancelInfo,
) (map[int64][]byte, common.EncodingType, error) {

	rcMap := make(map[int64][]byte)
	var encoding common.EncodingType
	for _, rc := range requestCancelInfos {
		datablob, err := serialization.RequestCancelInfoToBlob(rc)

		if err != nil {
			return nil, common.EncodingTypeUnknown, err
		}

		encoding = datablob.Encoding

		rcMap[rc.GetInitiatedId()] = datablob.Data
	}

	return rcMap, encoding, nil
}

func resetSignalInfoMap(
	signalInfos []*persistenceblobs.SignalInfo,
) (map[int64][]byte, common.EncodingType, error) {

	sMap := make(map[int64][]byte)
	var encoding common.EncodingType
	for _, s := range signalInfos {
		datablob, err := serialization.SignalInfoToBlob(s)

		if err != nil {
			return nil, common.EncodingTypeUnknown, err
		}

		encoding = datablob.Encoding

		sMap[s.GetInitiatedId()] = datablob.Data
	}

	return sMap, encoding, nil
}

func createHistoryEventBatchBlob(
	result map[string]interface{},
) *serialization.DataBlob {

	eventBatch := &serialization.DataBlob{Encoding: common.EncodingTypeJSON}
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

func createReplicationInfo(
	result map[string]interface{},
) *replicationspb.ReplicationInfo {

	info := &replicationspb.ReplicationInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "last_event_id":
			info.LastEventId = v.(int64)
		}
	}

	return info
}

func createReplicationInfoMap(
	info *replicationspb.ReplicationInfo,
) map[string]interface{} {

	rInfoMap := make(map[string]interface{})
	rInfoMap["version"] = info.Version
	rInfoMap["last_event_id"] = info.LastEventId

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
