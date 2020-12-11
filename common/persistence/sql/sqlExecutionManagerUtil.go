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

package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

func applyWorkflowMutationTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {
	lastWriteVersion := workflowMutation.LastWriteVersion
	namespaceID := workflowMutation.ExecutionInfo.NamespaceId
	workflowID := workflowMutation.ExecutionInfo.WorkflowId
	runID := workflowMutation.ExecutionState.RunId

	namespaceIDBytes, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("uuid parse failed. Error: %v", err))
	}

	runIDBytes, err := primitives.ParseUUID(runID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("uuid parse failed. Error: %v", err))
	}

	// TODO Remove me if UPDATE holds the lock to the end of a transaction
	if err := lockAndCheckNextEventID(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
		workflowMutation.Condition,
	); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Failed to lock executions row. Error: %v", err))
		}
	}

	if err := updateExecution(ctx,
		tx,
		workflowMutation.ExecutionInfo,
		workflowMutation.ExecutionState,
		workflowMutation.NextEventID,
		lastWriteVersion,
		shardID,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Failed to update executions row. Erorr: %v", err))
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowMutation.TransferTasks,
		workflowMutation.ReplicationTasks,
		workflowMutation.TimerTasks,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(ctx,
		tx,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateTimerInfos(ctx,
		tx,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateChildExecutionInfos(ctx,
		tx,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfos,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateRequestCancelInfos(ctx,
		tx,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfos,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateSignalInfos(ctx,
		tx,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfos,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateSignalsRequested(ctx,
		tx,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedIDs,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if workflowMutation.ClearBufferedEvents {
		if err := deleteBufferedEvents(ctx,
			tx,
			shardID,
			namespaceIDBytes,
			workflowID,
			runIDBytes,
		); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
		}
	}

	if err := updateBufferedEvents(ctx,
		tx,
		workflowMutation.NewBufferedEvents,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}
	return nil
}

func applyWorkflowSnapshotTxAsReset(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	lastWriteVersion := workflowSnapshot.LastWriteVersion
	workflowID := workflowSnapshot.ExecutionInfo.WorkflowId
	namespaceID := workflowSnapshot.ExecutionInfo.NamespaceId
	runID := workflowSnapshot.ExecutionState.RunId
	namespaceIDBytes, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		return err
	}
	runIDBytes, err := primitives.ParseUUID(runID)
	if err != nil {
		return err
	}

	// TODO Is there a way to modify the various map tables without fear of other people adding rows after we delete, without locking the executions row?
	if err := lockAndCheckNextEventID(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
		workflowSnapshot.Condition,
	); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to lock executions row. Error: %v", err))
		}
	}

	if err := updateExecution(ctx,
		tx,
		workflowSnapshot.ExecutionInfo,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		lastWriteVersion,
		shardID,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to update executions row. Erorr: %v", err))
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks,
	); err != nil {
		return err
	}

	if err := deleteActivityInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear activity info map. Error: %v", err))
	}

	if err := updateActivityInfos(ctx,
		tx,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := deleteTimerInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear timer info map. Error: %v", err))
	}

	if err := updateTimerInfos(ctx,
		tx,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into timer info map after clearing. Error: %v", err))
	}

	if err := deleteChildExecutionInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear child execution info map. Error: %v", err))
	}

	if err := updateChildExecutionInfos(ctx,
		tx,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := deleteRequestCancelInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear request cancel info map. Error: %v", err))
	}

	if err := updateRequestCancelInfos(ctx,
		tx,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into request cancel info map after clearing. Error: %v", err))
	}

	if err := deleteSignalInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear signal info map. Error: %v", err))
	}

	if err := updateSignalInfos(ctx,
		tx,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into signal info map after clearing. Error: %v", err))
	}

	if err := deleteSignalsRequestedSet(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear signals requested set. Error: %v", err))
	}

	if err := updateSignalsRequested(ctx,
		tx,
		workflowSnapshot.SignalRequestedIDs,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into signals requested set after clearing. Error: %v", err))
	}

	if err := deleteBufferedEvents(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear buffered events. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionManager) applyWorkflowSnapshotTxAsNew(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	lastWriteVersion := workflowSnapshot.LastWriteVersion
	workflowID := workflowSnapshot.ExecutionInfo.WorkflowId
	namespaceID := workflowSnapshot.ExecutionInfo.NamespaceId
	runID := workflowSnapshot.ExecutionState.RunId
	namespaceIDBytes, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		return err
	}
	runIDBytes, err := primitives.ParseUUID(runID)
	if err != nil {
		return err
	}

	if err := m.createExecution(ctx,
		tx,
		workflowSnapshot.ExecutionInfo,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		lastWriteVersion,
		shardID,
	); err != nil {
		return err
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(ctx,
		tx,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := updateTimerInfos(ctx,
		tx,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into timer info map after clearing. Error: %v", err))
	}

	if err := updateChildExecutionInfos(ctx,
		tx,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := updateRequestCancelInfos(ctx,
		tx,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into request cancel info map after clearing. Error: %v", err))
	}

	if err := updateSignalInfos(ctx,
		tx,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into signal info map after clearing. Error: %v", err))
	}

	if err := updateSignalsRequested(ctx,
		tx,
		workflowSnapshot.SignalRequestedIDs,
		nil,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into signals requested set after clearing. Error: %v", err))
	}

	return nil
}

func applyTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	transferTasks []p.Task,
	replicationTasks []p.Task,
	timerTasks []p.Task,
) error {

	if err := createTransferTasks(ctx,
		tx,
		transferTasks,
		shardID,
		namespaceID,
		workflowID,
		runID); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyTasks failed. Failed to create transfer tasks. Error: %v", err))
	}

	// TODO (alex): add createVisibilityTasks ?

	if err := createReplicationTasks(ctx,
		tx,
		replicationTasks,
		shardID,
		namespaceID,
		workflowID,
		runID); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyTasks failed. Failed to create replication tasks. Error: %v", err))
	}

	if err := createTimerTasks(ctx,
		tx,
		timerTasks,
		shardID,
		namespaceID,
		workflowID,
		runID); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("applyTasks failed. Failed to create timer tasks. Error: %v", err))
	}

	return nil
}

// lockCurrentExecutionIfExists returns current execution or nil if none is found for the workflowID
// locking it in the DB
func lockCurrentExecutionIfExists(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
) (*sqlplugin.CurrentExecutionsRow, error) {
	rows, err := tx.LockCurrentExecutionsJoinExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID: shardID, NamespaceID: namespaceID, WorkflowID: workflowID,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, serviceerror.NewInternal(fmt.Sprintf("lockCurrentExecutionIfExists failed. Failed to get current_executions row for (shard,namespace,workflow) = (%v, %v, %v). Error: %v", shardID, namespaceID, workflowID, err))
		}
	}
	size := len(rows)
	if size > 1 {
		return nil, serviceerror.NewInternal(fmt.Sprintf("lockCurrentExecutionIfExists failed. Multiple current_executions rows for (shard,namespace,workflow) = (%v, %v, %v).", shardID, namespaceID, workflowID))
	}
	if size == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func createOrUpdateCurrentExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	createMode p.CreateWorkflowMode,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	createRequestID string,
	startVersion int64,
	lastWriteVersion int64,
) error {

	row := sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  createRequestID,
		State:            state,
		Status:           status,
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,
	}

	switch createMode {
	case p.CreateWorkflowModeContinueAsNew:
		if err := updateCurrentExecution(ctx,
			tx,
			shardID,
			namespaceID,
			workflowID,
			runID,
			createRequestID,
			state,
			status,
			row.StartVersion,
			row.LastWriteVersion); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("createOrUpdateCurrentExecution failed. Failed to continue as new. Error: %v", err))
		}
	case p.CreateWorkflowModeWorkflowIDReuse:
		if err := updateCurrentExecution(ctx,
			tx,
			shardID,
			namespaceID,
			workflowID,
			runID,
			createRequestID,
			state,
			status,
			row.StartVersion,
			row.LastWriteVersion); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("createOrUpdateCurrentExecution failed. Failed to reuse workflow ID. Error: %v", err))
		}
	case p.CreateWorkflowModeBrandNew:
		if _, err := tx.InsertIntoCurrentExecutions(ctx, &row); err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("createOrUpdateCurrentExecution failed. Failed to insert into current_executions table. Error: %v", err))
		}
	case p.CreateWorkflowModeZombie:
		// noop
	default:
		return fmt.Errorf("createOrUpdateCurrentExecution failed. Unknown workflow creation mode: %v", createMode)
	}

	return nil
}

func lockAndCheckNextEventID(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	condition int64,
) error {

	nextEventID, err := lockNextEventID(ctx, tx, shardID, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}
	if *nextEventID != condition {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("lockAndCheckNextEventID failed. Next_event_id was %v when it should have been %v.", nextEventID, condition),
		}
	}
	return nil
}

func lockNextEventID(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (*int64, error) {

	nextEventID, err := tx.WriteLockExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("lockNextEventID failed. Unable to lock executions row with (shard, namespace, workflow, run) = (%v,%v,%v,%v) which does not exist.",
				shardID,
				namespaceID,
				workflowID,
				runID))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("lockNextEventID failed. Error: %v", err))
	}
	result := int64(nextEventID)
	return &result, nil
}

func createTransferTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	transferTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	if len(transferTasks) == 0 {
		return nil
	}

	transferTasksRows := make([]sqlplugin.TransferTasksRow, len(transferTasks))
	for i, task := range transferTasks {
		info := &persistencespb.TransferTaskInfo{
			NamespaceId:       namespaceID,
			WorkflowId:        workflowID,
			RunId:             runID,
			TargetNamespaceId: namespaceID,
			TargetWorkflowId:  p.TransferTaskTransferTargetWorkflowID,
			ScheduleId:        0,
			TaskId:            task.GetTaskID(),
		}

		transferTasksRows[i].ShardID = shardID
		transferTasksRows[i].TaskID = task.GetTaskID()

		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			info.TargetNamespaceId = task.(*p.ActivityTask).NamespaceID
			info.TaskQueue = task.(*p.ActivityTask).TaskQueue
			info.ScheduleId = task.(*p.ActivityTask).ScheduleID

		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			info.TargetNamespaceId = task.(*p.WorkflowTask).NamespaceID
			info.TaskQueue = task.(*p.WorkflowTask).TaskQueue
			info.ScheduleId = task.(*p.WorkflowTask).ScheduleID

		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			info.TargetNamespaceId = task.(*p.CancelExecutionTask).TargetNamespaceID
			info.TargetWorkflowId = task.(*p.CancelExecutionTask).TargetWorkflowID
			if task.(*p.CancelExecutionTask).TargetRunID != "" {
				info.TargetRunId = task.(*p.CancelExecutionTask).TargetRunID
			}
			info.TargetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			info.ScheduleId = task.(*p.CancelExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			info.TargetNamespaceId = task.(*p.SignalExecutionTask).TargetNamespaceID
			info.TargetWorkflowId = task.(*p.SignalExecutionTask).TargetWorkflowID
			if task.(*p.SignalExecutionTask).TargetRunID != "" {
				info.TargetRunId = task.(*p.SignalExecutionTask).TargetRunID
			}
			info.TargetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			info.ScheduleId = task.(*p.SignalExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			info.TargetNamespaceId = task.(*p.StartChildExecutionTask).TargetNamespaceID
			info.TargetWorkflowId = task.(*p.StartChildExecutionTask).TargetWorkflowID
			info.ScheduleId = task.(*p.StartChildExecutionTask).InitiatedID

		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
			enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED,
			enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW,
			enumsspb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			// No explicit property needs to be set

		default:
			return serviceerror.NewInternal(fmt.Sprintf("createTransferTasks failed. Unknow transfer type: %v", task.GetType()))
		}

		info.TaskType = task.GetType()
		info.Version = task.GetVersion()

		info.VisibilityTime = timestamp.TimePtr(task.GetVisibilityTimestamp().UTC())

		blob, err := serialization.TransferTaskInfoToBlob(info)
		if err != nil {
			return err
		}
		transferTasksRows[i].Data = blob.Data
		transferTasksRows[i].DataEncoding = blob.EncodingType.String()
	}

	result, err := tx.InsertIntoTransferTasks(ctx, transferTasksRows)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createTransferTasks failed. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createTransferTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(transferTasks) {
		return serviceerror.NewInternal(fmt.Sprintf("createTransferTasks failed. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(transferTasks), err))
	}
	return nil
}

func createReplicationTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	replicationTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	if len(replicationTasks) == 0 {
		return nil
	}

	replicationTasksRows := make([]sqlplugin.ReplicationTasksRow, len(replicationTasks))
	for i, task := range replicationTasks {

		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		activityScheduleID := common.EmptyEventID

		var branchToken, newRunBranchToken []byte

		switch task.GetType() {
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			historyReplicationTask, ok := task.(*p.HistoryReplicationTask)
			if !ok {
				return serviceerror.NewInternal(fmt.Sprintf("createReplicationTasks failed. Failed to cast %v to HistoryReplicationTask", task))
			}
			firstEventID = historyReplicationTask.FirstEventID
			nextEventID = historyReplicationTask.NextEventID
			version = task.GetVersion()
			branchToken = historyReplicationTask.BranchToken
			newRunBranchToken = historyReplicationTask.NewRunBranchToken

		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID

		default:
			return serviceerror.NewInternal(fmt.Sprintf("Unknown replication task: %v", task.GetType()))
		}

		blob, err := serialization.ReplicationTaskInfoToBlob(&persistencespb.ReplicationTaskInfo{
			TaskId:            task.GetTaskID(),
			NamespaceId:       namespaceID,
			WorkflowId:        workflowID,
			RunId:             runID,
			TaskType:          task.GetType(),
			FirstEventId:      firstEventID,
			NextEventId:       nextEventID,
			Version:           version,
			ScheduledId:       activityScheduleID,
			BranchToken:       branchToken,
			NewRunBranchToken: newRunBranchToken,
		})
		if err != nil {
			return err
		}
		replicationTasksRows[i].ShardID = shardID
		replicationTasksRows[i].TaskID = task.GetTaskID()
		replicationTasksRows[i].Data = blob.Data
		replicationTasksRows[i].DataEncoding = blob.EncodingType.String()
	}

	result, err := tx.InsertIntoReplicationTasks(ctx, replicationTasksRows)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createReplicationTasks failed. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createReplicationTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(replicationTasks) {
		return serviceerror.NewInternal(fmt.Sprintf("createReplicationTasks failed. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(replicationTasks), err))
	}
	return nil
}

func createTimerTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	timerTasks []p.Task,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	if len(timerTasks) == 0 {
		return nil
	}

	timerTasksRows := make([]sqlplugin.TimerTasksRow, len(timerTasks))
	for i, task := range timerTasks {
		info := &persistencespb.TimerTaskInfo{}
		switch t := task.(type) {
		case *p.WorkflowTaskTimeoutTask:
			info.EventId = t.EventID
			info.TimeoutType = t.TimeoutType
			info.ScheduleAttempt = t.ScheduleAttempt

		case *p.ActivityTimeoutTask:
			info.EventId = t.EventID
			info.TimeoutType = t.TimeoutType
			info.ScheduleAttempt = t.Attempt

		case *p.UserTimerTask:
			info.EventId = t.EventID

		case *p.ActivityRetryTimerTask:
			info.EventId = t.EventID
			info.ScheduleAttempt = t.Attempt

		case *p.WorkflowBackoffTimerTask:
			info.EventId = t.EventID
			info.WorkflowBackoffType = t.WorkflowBackoffType

		case *p.WorkflowTimeoutTask:
			// noop

		case *p.DeleteHistoryEventTask:
			// noop

		default:
			return serviceerror.NewInternal(fmt.Sprintf("createTimerTasks failed. Unknown timer task: %v", task.GetType()))
		}

		info.NamespaceId = namespaceID
		info.WorkflowId = workflowID
		info.RunId = runID
		info.Version = task.GetVersion()
		info.TaskType = task.GetType()
		info.TaskId = task.GetTaskID()

		goVisTs := timestamp.TimePtr(task.GetVisibilityTimestamp().UTC())

		info.VisibilityTime = goVisTs
		blob, err := serialization.TimerTaskInfoToBlob(info)
		if err != nil {
			return err
		}

		timerTasksRows[i].ShardID = shardID
		timerTasksRows[i].VisibilityTimestamp = *goVisTs
		timerTasksRows[i].TaskID = task.GetTaskID()
		timerTasksRows[i].Data = blob.Data
		timerTasksRows[i].DataEncoding = blob.EncodingType.String()
	}

	result, err := tx.InsertIntoTimerTasks(ctx, timerTasksRows)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createTimerTasks failed. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createTimerTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(timerTasks) {
		return serviceerror.NewInternal(fmt.Sprintf("createTimerTasks failed. Inserted %v instead of %v rows into timer_tasks. Error: %v", rowsAffected, len(timerTasks), err))
	}
	return nil
}

func assertRunIDAndUpdateCurrentExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	newRunID primitives.UUID,
	previousRunID primitives.UUID,
	createRequestID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	startVersion int64,
	lastWriteVersion int64,
) error {

	assertFn := func(currentRow *sqlplugin.CurrentExecutionsRow) error {
		if !bytes.Equal(currentRow.RunID, previousRunID) {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"assertRunIDAndUpdateCurrentExecution failed. Current RunId was %v, expected %v",
				currentRow.RunID,
				previousRunID,
			)}
		}
		return nil
	}
	if err := assertCurrentExecution(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
		assertFn,
	); err != nil {
		return err
	}

	return updateCurrentExecution(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
		newRunID,
		createRequestID,
		state,
		status,
		startVersion,
		lastWriteVersion,
	)
}

func assertCurrentExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	assertFn func(currentRow *sqlplugin.CurrentExecutionsRow) error,
) error {

	currentRow, err := tx.LockCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("assertCurrentExecution failed. Unable to load current record. Error: %v", err))
	}
	return assertFn(currentRow)
}

func assertRunIDMismatch(runID primitives.UUID, currentRunID primitives.UUID) error {
	// zombie workflow creation with existence of current record, this is a noop
	if bytes.Equal(currentRunID, runID) {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"assertRunIDMismatch failed. Current RunId was %v, input %v",
			currentRunID,
			runID,
		)}
	}
	return nil
}

func updateCurrentExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	createRequestID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	startVersion int64,
	lastWriteVersion int64,
) error {

	result, err := tx.UpdateCurrentExecutions(ctx, &sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  createRequestID,
		State:            state,
		Status:           status,
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,
	})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateCurrentExecution failed. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateCurrentExecution failed. Failed to check number of rows updated in current_executions table. Error: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("updateCurrentExecution failed. %v rows of current_executions updated instead of 1.", rowsAffected))
	}
	return nil
}

func buildExecutionRow(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	shardID int32,
) (row *sqlplugin.ExecutionsRow, err error) {

	infoBlob, err := serialization.WorkflowExecutionInfoToBlob(executionInfo)
	if err != nil {
		return nil, err
	}

	stateBlob, err := serialization.WorkflowExecutionStateToBlob(executionState)
	if err != nil {
		return nil, err
	}

	nsBytes, err := primitives.ParseUUID(executionInfo.NamespaceId)
	if err != nil {
		return nil, err
	}

	ridBytes, err := primitives.ParseUUID(executionState.RunId)
	if err != nil {
		return nil, err
	}

	return &sqlplugin.ExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      nsBytes,
		WorkflowID:       executionInfo.WorkflowId,
		RunID:            ridBytes,
		NextEventID:      nextEventID,
		LastWriteVersion: lastWriteVersion,
		Data:             infoBlob.Data,
		DataEncoding:     infoBlob.EncodingType.String(),
		State:            stateBlob.Data,
		StateEncoding:    stateBlob.EncodingType.String(),
	}, nil
}

func (m *sqlExecutionManager) createExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	shardID int32,
) error {

	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	// TODO we should set the start time and last update time on business logic layer
	executionInfo.LastUpdateTime = executionInfo.StartTime

	row, err := buildExecutionRow(
		executionInfo,
		executionState,
		nextEventID,
		lastWriteVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.InsertIntoExecutions(ctx, row)
	if err != nil {
		if m.db.IsDupEntryError(err) {
			return &p.WorkflowExecutionAlreadyStartedError{
				Msg:              fmt.Sprintf("Workflow execution already running. WorkflowId: %v", executionInfo.WorkflowId),
				StartRequestID:   executionState.CreateRequestId,
				RunID:            executionState.RunId,
				State:            executionState.State,
				Status:           executionState.Status,
				LastWriteVersion: row.LastWriteVersion,
			}
		}
		return serviceerror.NewInternal(fmt.Sprintf("createExecution failed. Erorr: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("createExecution failed. Failed to verify number of rows affected. Erorr: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewNotFound(fmt.Sprintf("createExecution failed. Affected %v rows updated instead of 1.", rowsAffected))
	}

	return nil
}

func updateExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	shardID int32,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	// TODO we should set the last update time on business logic layer
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()

	row, err := buildExecutionRow(
		executionInfo,
		executionState,
		nextEventID,
		lastWriteVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.UpdateExecutions(ctx, row)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateExecution failed. Erorr: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("updateExecution failed. Failed to verify number of rows affected. Erorr: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewNotFound(fmt.Sprintf("updateExecution failed. Affected %v rows updated instead of 1.", rowsAffected))
	}

	return nil
}
