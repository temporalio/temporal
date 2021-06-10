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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

func applyWorkflowMutationBatch(
	batch gocql.Batch,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	// TODO update all call sites to update LastUpdatetime
	//cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowMutation.ExecutionInfo,
		workflowMutation.ExecutionState,
		workflowMutation.ExecutionStateBlob,
		workflowMutation.NextEventID,
		workflowMutation.Condition,
		workflowMutation.DBRecordVersion,
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
		workflowMutation.TimerTasks,
		workflowMutation.ReplicationTasks,
		workflowMutation.VisibilityTasks,
	)
}

func applyWorkflowSnapshotBatchAsReset(
	batch gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	// TODO: update call site
	//cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.ExecutionInfo,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.ExecutionStateBlob,
		workflowSnapshot.NextEventID,
		workflowSnapshot.Condition,
		workflowSnapshot.DBRecordVersion,
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
		workflowSnapshot.TimerTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.VisibilityTasks,
	)
}

func applyWorkflowSnapshotBatchAsNew(
	batch gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := createExecution(
		batch,
		shardID,
		workflowSnapshot,
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
		workflowSnapshot.TimerTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.VisibilityTasks,
	)
}

func createExecution(
	batch gocql.Batch,
	shardID int32,
	snapshot *p.InternalWorkflowSnapshot,
) error {
	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		snapshot.ExecutionState.State,
		snapshot.ExecutionState.Status); err != nil {
		return err
	}

	// TODO also need to set the start / current / last write version
	batch.Query(templateCreateWorkflowExecutionQuery,
		shardID,
		snapshot.NamespaceID,
		snapshot.WorkflowID,
		snapshot.RunID,
		rowTypeExecution,
		snapshot.ExecutionInfo.Data,
		snapshot.ExecutionInfo.EncodingType.String(),
		snapshot.ExecutionStateBlob.Data,
		snapshot.ExecutionStateBlob.EncodingType.String(),
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		snapshot.Checksum.Data,
		snapshot.Checksum.EncodingType.String(),
	)

	return nil
}

func updateExecution(
	batch gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionInfoBlob *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	condition int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	if dbRecordVersion == 0 {
		batch.Query(templateUpdateWorkflowExecutionQueryDeprecated,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition,
		)
	} else {
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			dbRecordVersion,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			dbRecordVersion-1,
		)
	}

	return nil
}

func applyTasks(
	batch gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	transferTasks []p.Task,
	timerTasks []p.Task,
	replicationTasks []p.Task,
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

	if err := createTimerTasks(
		batch,
		timerTasks,
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

	return nil
}

func createTransferTasks(
	batch gocql.Batch,
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

		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			targetNamespaceID = task.(*p.ActivityTask).NamespaceID
			taskQueue = task.(*p.ActivityTask).TaskQueue
			scheduleID = task.(*p.ActivityTask).ScheduleID

		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			targetNamespaceID = task.(*p.WorkflowTask).NamespaceID
			taskQueue = task.(*p.WorkflowTask).TaskQueue
			scheduleID = task.(*p.WorkflowTask).ScheduleID

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
			enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			// No explicit property needs to be set

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknow transfer type: %v", task.GetType()))
		}

		transferTaskInfo := &persistencespb.TransferTaskInfo{
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
		}

		dataBlob, err := serialization.TransferTaskInfoToBlob(transferTaskInfo)
		if err != nil {
			return err
		}
		batch.Query(templateCreateTransferTaskQuery,
			shardID,
			rowTypeTransferTask,
			rowTypeTransferNamespaceID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			dataBlob.Data,
			dataBlob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createReplicationTasks(
	batch gocql.Batch,
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
	batch gocql.Batch,
	visibilityTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for _, task := range visibilityTasks {
		datablob, err := serialization.VisibilityTaskInfoToBlob(&persistencespb.VisibilityTaskInfo{
			NamespaceId:    namespaceID,
			WorkflowId:     workflowID,
			RunId:          runID,
			TaskId:         task.GetTaskID(),
			TaskType:       task.GetType(),
			Version:        task.GetVersion(),
			VisibilityTime: timestamp.TimePtr(task.GetVisibilityTimestamp()),
		})
		if err != nil {
			return err
		}

		switch task.GetType() {
		case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
			// noop

		case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
			// noop

		case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
			// noop

		case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
			// noop

		default:
			return serviceerror.NewInternal(fmt.Sprintf("createVisibilityTasks failed. Unknow visibility type: %v", task.GetType()))
		}

		batch.Query(templateCreateVisibilityTaskQuery,
			shardID,
			rowTypeVisibilityTask,
			rowTypeVisibilityTaskNamespaceID,
			rowTypeVisibilityTaskWorkflowID,
			rowTypeVisibilityTaskRunID,
			datablob.Data,
			datablob.EncodingType.String(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}

	return nil
}

func createTimerTasks(
	batch gocql.Batch,
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
		dbTs := p.UnixMilliseconds(goTs)

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
	batch gocql.Batch,
	createMode p.CreateWorkflowMode,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	state enumsspb.WorkflowExecutionState,
	lastWriteVersion int64,
	previousRunID string,
	previousLastWriteVersion int64,
	executionStateBlob *commonpb.DataBlob,
) error {
	switch createMode {
	case p.CreateWorkflowModeContinueAsNew:
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			runID,
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
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
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
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
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			lastWriteVersion,
			state,
		)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown mode: %v", createMode))
	}

	return nil
}

func updateActivityInfos(
	batch gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for scheduledID, blob := range activityInfos {
		batch.Query(templateUpdateActivityInfoQuery,
			scheduledID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
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
	batch gocql.Batch,
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
	batch gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
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
	batch gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
	deleteInfos map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	for timerID, blob := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			timerID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteInfoID := range deleteInfos {
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
	batch gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
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
	batch gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range childExecutionInfos {
		batch.Query(templateUpdateChildExecutionInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
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
	batch gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
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
	batch gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
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
	batch gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
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
	batch gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
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
	batch gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
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
	batch gocql.Batch,
	signalReqIDs map[string]struct{},
	deleteSignalReqIDs map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			convert.StringSetToSlice(signalReqIDs),
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
			convert.StringSetToSlice(deleteSignalReqIDs),
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
	batch gocql.Batch,
	signalRequested map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalRequestedQuery,
		convert.StringSetToSlice(signalRequested),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateBufferedEvents(
	batch gocql.Batch,
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
	activityInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	aMap := make(map[int64][]byte)
	for scheduledID, blob := range activityInfos {
		aMap[scheduledID] = blob.Data
		encoding = blob.EncodingType
	}

	return aMap, encoding, nil
}

func resetTimerInfoMap(
	timerInfos map[string]*commonpb.DataBlob,
) (map[string][]byte, enumspb.EncodingType, error) {

	tMap := make(map[string][]byte)
	var encoding enumspb.EncodingType
	for timerID, blob := range timerInfos {
		encoding = blob.EncodingType
		tMap[timerID] = blob.Data
	}

	return tMap, encoding, nil
}

func resetChildExecutionInfoMap(
	childExecutionInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	cMap := make(map[int64][]byte)
	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	for initiatedID, blob := range childExecutionInfos {
		cMap[initiatedID] = blob.Data
		encoding = blob.EncodingType
	}

	return cMap, encoding, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	rcMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range requestCancelInfos {
		encoding = blob.EncodingType
		rcMap[initiatedID] = blob.Data
	}

	return rcMap, encoding, nil
}

func resetSignalInfoMap(
	signalInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	sMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range signalInfos {
		encoding = blob.EncodingType
		sMap[initiatedID] = blob.Data
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
