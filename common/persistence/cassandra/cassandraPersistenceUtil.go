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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

func applyWorkflowMutationBatch(
	batch *gocql.Batch,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	namespaceID := workflowMutation.ExecutionInfo.NamespaceId
	workflowID := workflowMutation.ExecutionInfo.WorkflowId
	runID := workflowMutation.ExecutionState.RunId

	if err := updateExecution(
		batch,
		shardID,
		workflowMutation.ExecutionInfo,
		workflowMutation.ExecutionState,
		workflowMutation.NextEventID,
		cqlNowTimestampMillis,
		workflowMutation.Condition,
		workflowMutation.Checksum,
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
		workflowMutation.DeleteChildExecutionInfos,
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
		workflowMutation.DeleteRequestCancelInfos,
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
		workflowMutation.DeleteSignalInfos,
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
		workflowMutation.DeleteSignalRequestedIDs,
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
		workflowMutation.VisibilityTasks,
	)
}

func applyWorkflowSnapshotBatchAsReset(
	batch *gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	namespaceID := workflowSnapshot.ExecutionInfo.NamespaceId
	workflowID := workflowSnapshot.ExecutionInfo.WorkflowId
	runID := workflowSnapshot.ExecutionState.RunId
	condition := workflowSnapshot.Condition

	if err := updateExecution(
		batch,
		shardID,
		workflowSnapshot.ExecutionInfo,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		cqlNowTimestampMillis,
		condition,
		workflowSnapshot.Checksum,
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
		workflowSnapshot.VisibilityTasks,
	)
}

func applyWorkflowSnapshotBatchAsNew(
	batch *gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	cqlNowTimestampMillis := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	namespaceID := workflowSnapshot.ExecutionInfo.NamespaceId
	workflowID := workflowSnapshot.ExecutionInfo.WorkflowId
	runID := workflowSnapshot.ExecutionState.RunId

	if err := createExecution(
		batch,
		shardID,
		workflowSnapshot.ExecutionInfo,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		workflowSnapshot.Checksum,
		cqlNowTimestampMillis,
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
		nil,
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
		workflowSnapshot.VisibilityTasks,
	)
}

func createExecution(
	batch *gocql.Batch,
	shardID int32,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	checksum *persistencespb.Checksum,
	cqlNowTimestampMillis int64,
) error {

	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId
	runID := executionState.RunId

	// TODO we should set the start time and last update time on business logic layer
	executionInfo.LastUpdateTime = timestamp.UnixOrZeroTimePtr(p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	executionDatablob, err := serialization.WorkflowExecutionInfoToBlob(executionInfo)
	if err != nil {
		return err
	}

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(executionState)
	if err != nil {
		return err
	}

	checksumDatablob, err := serialization.ChecksumToBlob(checksum)
	if err != nil {
		return err
	}

	if executionInfo.VersionHistories == nil {
		// Cross DC feature is currently disabled so we will be creating workflow executions without versioned history
		batch.Query(templateCreateWorkflowExecutionQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			rowTypeExecution,
			executionDatablob.Data,
			executionDatablob.EncodingType.String(),
			executionStateDatablob.Data,
			executionStateDatablob.EncodingType.String(),
			nextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			checksumDatablob.Data,
			checksumDatablob.EncodingType.String())
	} else {
		// TODO also need to set the start / current / last write version
		batch.Query(templateCreateWorkflowExecutionQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			rowTypeExecution,
			executionDatablob.Data,
			executionDatablob.EncodingType.String(),
			executionStateDatablob.Data,
			executionStateDatablob.EncodingType.String(),
			nextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			checksumDatablob.Data,
			checksumDatablob.EncodingType.String())
	}
	return nil
}

func updateExecution(
	batch *gocql.Batch,
	shardID int32,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	cqlNowTimestampMillis int64,
	condition int64,
	checksum *persistencespb.Checksum,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId
	runID := executionState.RunId

	// TODO we should set the last update time on business logic layer
	executionInfo.LastUpdateTime = timestamp.UnixOrZeroTimePtr(p.DBTimestampToUnixNano(cqlNowTimestampMillis))

	executionDatablob, err := serialization.WorkflowExecutionInfoToBlob(executionInfo)
	if err != nil {
		return err
	}

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(executionState)
	if err != nil {
		return err
	}

	checksumDatablob, err := serialization.ChecksumToBlob(checksum)
	if err != nil {
		return err
	}

	if executionInfo.VersionHistories == nil {
		// Updates will be called with null ReplicationState while the feature is disabled
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionDatablob.Data,
			executionDatablob.EncodingType.String(),
			executionStateDatablob.Data,
			executionStateDatablob.EncodingType.String(),
			nextEventID,
			checksumDatablob.Data,
			checksumDatablob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else {
		// TODO also need to set the start / current / last write version
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionDatablob.Data,
			executionDatablob.EncodingType.String(),
			executionStateDatablob.Data,
			executionStateDatablob.EncodingType.String(),
			nextEventID,
			checksumDatablob.Data,
			checksumDatablob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	return nil
}

func applyTasks(
	batch *gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	transferTasks []p.Task,
	replicationTasks []p.Task,
	timerTasks []p.Task,
	visibilityTasks []p.Task,
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

	if err := createVisibilityTasks(
		batch,
		visibilityTasks,
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
	shardID int32,
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
			enumsspb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			// No explicit property needs to be set

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow transfer type: %v", task.GetType()))
		}

		// todo ~~~ come back for record visibility
		p := &persistencespb.TransferTaskInfo{
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
			VisibilityTime:          timestamp.TimePtr(task.GetVisibilityTimestamp()),
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
			datablob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createReplicationTasks(
	batch *gocql.Batch,
	replicationTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion // nolint:ineffassign
		activityScheduleID := common.EmptyEventID
		var branchToken, newRunBranchToken []byte

		switch task.GetType() {
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			histTask := task.(*p.HistoryReplicationTask)
			branchToken = histTask.BranchToken
			newRunBranchToken = histTask.NewRunBranchToken
			firstEventID = histTask.FirstEventID
			nextEventID = histTask.NextEventID
			version = task.GetVersion()

		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow replication type: %v", task.GetType()))
		}

		datablob, err := serialization.ReplicationTaskInfoToBlob(&persistencespb.ReplicationTaskInfo{
			NamespaceId:       namespaceID,
			WorkflowId:        workflowID,
			RunId:             runID,
			TaskId:            task.GetTaskID(),
			TaskType:          task.GetType(),
			Version:           version,
			FirstEventId:      firstEventID,
			NextEventId:       nextEventID,
			ScheduledId:       activityScheduleID,
			BranchToken:       branchToken,
			NewRunBranchToken: newRunBranchToken,
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
			datablob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createVisibilityTasks(
	batch *gocql.Batch,
	visibilityTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range visibilityTasks {
		version := common.EmptyVersion
		version = task.GetVersion()

		switch task.GetType() {
		case enumsspb.TASK_TYPE_VISIBILITY_RECORD_WORKFLOW_STARTED:
		case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
		case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow visibility task type: %v", task.GetType()))
		}

		datablob, err := serialization.VisibilityTaskInfoToBlob(&persistencespb.VisibilityTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskId:      task.GetTaskID(),
			TaskType:    task.GetType(),
			Version:     version,
		})

		if err != nil {
			return err
		}

		batch.Query(templateCreateVisibilityTaskQuery,
			shardID,
			rowTypeVisibilityTask,
			rowTypeVisibilityNamespaceID,
			rowTypeVisibilityWorkflowID,
			rowTypeVisibilityRunID,
			datablob.Data,
			datablob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createTimerTasks(
	batch *gocql.Batch,
	timerTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range timerTasks {
		eventID := int64(0)
		attempt := int32(1)

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
			attempt = t.Attempt

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

		datablob, err := serialization.TimerTaskInfoToBlob(&persistencespb.TimerTaskInfo{
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
			VisibilityTime:      &goTs,
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
			datablob.EncodingType.String(),
			dbTs,
			task.GetTaskID())
	}

	return nil
}

func createOrUpdateCurrentExecution(
	batch *gocql.Batch,
	createMode p.CreateWorkflowMode,
	shardID int32,
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

	executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
		RunId:           runID,
		CreateRequestId: createRequestID,
		State:           state,
		Status:          status,
	})

	if err != nil {
		return err
	}

	replicationVersions, err := serialization.ReplicationVersionsToBlob(
		&persistencespb.ReplicationVersions{
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
			executionStateDatablob.EncodingType.String(),
			replicationVersions.Data,
			replicationVersions.EncodingType.String(),
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
			executionStateDatablob.EncodingType.String(),
			replicationVersions.Data,
			replicationVersions.EncodingType.String(),
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
			executionStateDatablob.EncodingType.String(),
			replicationVersions.Data,
			replicationVersions.EncodingType.String(),
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
	activityInfos []*persistencespb.ActivityInfo,
	deleteIDs []int64,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, a := range activityInfos {
		activityBlob, err := serialization.ActivityInfoToBlob(a)
		if err != nil {
			return p.NewSerializationError(fmt.Sprintf("activity info serialization error: %v", err))
		}

		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleId,
			activityBlob.Data,
			activityBlob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteID := range deleteIDs {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteID,
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
	shardID int32,
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
	activityInfos []*persistencespb.ActivityInfo,
	shardID int32,
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
		encoding.String(),
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
	timerInfos []*persistencespb.TimerInfo,
	deleteInfos []string,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	for _, timerInfo := range timerInfos {
		datablob, err := serialization.TimerInfoToBlob(timerInfo)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateTimerInfoQuery,
			timerInfo.GetTimerId(),         // timermap key
			datablob.Data,                  // timermap data
			datablob.EncodingType.String(), // timermap encoding
			shardID,                        // where ...
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteInfoID := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			deleteInfoID,
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
	timerInfos []*persistencespb.TimerInfo,
	shardID int32,
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
		timerMapEncoding.String(),
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
	childExecutionInfos []*persistencespb.ChildExecutionInfo,
	deleteIDs []int64,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, childInfo := range childExecutionInfos {
		datablob, err := serialization.ChildExecutionInfoToBlob(childInfo)
		if err != nil {
			return nil
		}

		batch.Query(templateUpdateChildExecutionInfoQuery,
			childInfo.InitiatedId,
			datablob.Data,
			datablob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteID := range deleteIDs {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			deleteID,
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
	childExecutionInfos []*persistencespb.ChildExecutionInfo,
	shardID int32,
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
		encoding.String(),
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
	requestCancelInfos []*persistencespb.RequestCancelInfo,
	deleteIDs []int64,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, requestCancelInfo := range requestCancelInfos {
		datablob, err := serialization.RequestCancelInfoToBlob(requestCancelInfo)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateRequestCancelInfoQuery,
			requestCancelInfo.GetInitiatedId(),
			datablob.Data,
			datablob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteID := range deleteIDs {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			deleteID,
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
	requestCancelInfos []*persistencespb.RequestCancelInfo,
	shardID int32,
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
		rciMapEncoding.String(),
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
	signalInfos []*persistencespb.SignalInfo,
	deleteIDs []int64,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, signalInfo := range signalInfos {
		datablob, err := serialization.SignalInfoToBlob(signalInfo)
		if err != nil {
			return err
		}

		batch.Query(templateUpdateSignalInfoQuery,
			signalInfo.GetInitiatedId(),
			datablob.Data,
			datablob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for _, deleteID := range deleteIDs {
		batch.Query(templateDeleteSignalInfoQuery,
			deleteID,
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
	signalInfos []*persistencespb.SignalInfo,
	shardID int32,
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
		sMapEncoding.String(),
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
	deleteSignalReqIDs []string,
	shardID int32,
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

	if len(deleteSignalReqIDs) > 0 {
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			deleteSignalReqIDs,
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
	shardID int32,
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
	newBufferedEvents *commonpb.DataBlob,
	clearBufferedEvents bool,
	shardID int32,
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
		values["encoding_type"] = newBufferedEvents.EncodingType.String()
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

func resetActivityInfoMap(
	activityInfos []*persistencespb.ActivityInfo,
) (map[int64][]byte, enumspb.EncodingType, error) {

	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	aMap := make(map[int64][]byte)
	for _, a := range activityInfos {
		aBlob, err := serialization.ActivityInfoToBlob(a)
		if err != nil {
			return nil, enumspb.ENCODING_TYPE_UNSPECIFIED, p.NewSerializationError(fmt.Sprintf("failed to serialize activity infos - ActivityId: %v", a.ActivityId))
		}

		aMap[a.ScheduleId] = aBlob.Data
		encoding = aBlob.EncodingType
	}

	return aMap, encoding, nil
}

func resetTimerInfoMap(
	timerInfos []*persistencespb.TimerInfo,
) (map[string][]byte, enumspb.EncodingType, error) {

	tMap := make(map[string][]byte)
	var encoding enumspb.EncodingType
	for _, t := range timerInfos {
		datablob, err := serialization.TimerInfoToBlob(t)

		if err != nil {
			return nil, enumspb.ENCODING_TYPE_UNSPECIFIED, err
		}

		encoding = datablob.EncodingType

		tMap[t.GetTimerId()] = datablob.Data
	}

	return tMap, encoding, nil
}

func resetChildExecutionInfoMap(
	childExecutionInfos []*persistencespb.ChildExecutionInfo,
) (map[int64][]byte, enumspb.EncodingType, error) {

	cMap := make(map[int64][]byte)
	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	for _, c := range childExecutionInfos {
		datablob, err := serialization.ChildExecutionInfoToBlob(c)
		if err != nil {
			return nil, enumspb.ENCODING_TYPE_UNSPECIFIED, p.NewSerializationError(fmt.Sprintf("failed to serialize child execution infos - Execution: %v", c.InitiatedId))
		}
		cMap[c.InitiatedId] = datablob.Data
		encoding = datablob.EncodingType
	}

	return cMap, encoding, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos []*persistencespb.RequestCancelInfo,
) (map[int64][]byte, enumspb.EncodingType, error) {

	rcMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for _, rc := range requestCancelInfos {
		datablob, err := serialization.RequestCancelInfoToBlob(rc)

		if err != nil {
			return nil, enumspb.ENCODING_TYPE_UNSPECIFIED, err
		}

		encoding = datablob.EncodingType

		rcMap[rc.GetInitiatedId()] = datablob.Data
	}

	return rcMap, encoding, nil
}

func resetSignalInfoMap(
	signalInfos []*persistencespb.SignalInfo,
) (map[int64][]byte, enumspb.EncodingType, error) {

	sMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for _, s := range signalInfos {
		datablob, err := serialization.SignalInfoToBlob(s)

		if err != nil {
			return nil, enumspb.ENCODING_TYPE_UNSPECIFIED, err
		}

		encoding = datablob.EncodingType

		sMap[s.GetInitiatedId()] = datablob.Data
	}

	return sMap, encoding, nil
}

func createHistoryEventBatchBlob(
	result map[string]interface{},
) *commonpb.DataBlob {
	eventBatch := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED}
	for k, v := range result {
		switch k {
		case "encoding_type":
			encodingStr := v.(string)
			if encoding, ok := enumspb.EncodingType_value[encodingStr]; ok {
				eventBatch.EncodingType = enumspb.EncodingType(encoding)
			}
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
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
