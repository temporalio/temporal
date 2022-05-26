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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
)

func applyWorkflowMutationTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {
	lastWriteVersion := workflowMutation.LastWriteVersion
	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
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
	if err := lockAndCheckExecution(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
		workflowMutation.Condition,
		workflowMutation.DBRecordVersion,
	); err != nil {
		switch err.(type) {
		case *p.WorkflowConditionFailedError, *p.ConditionFailedError:
			return err
		default:
			return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Failed to lock executions row. Error: %v", err))
		}
	}

	if err := updateExecution(ctx,
		tx,
		namespaceID,
		workflowID,
		workflowMutation.ExecutionInfoBlob,
		workflowMutation.ExecutionState,
		workflowMutation.NextEventID,
		lastWriteVersion,
		workflowMutation.DBRecordVersion,
		shardID,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Failed to update executions row. Erorr: %v", err))
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		workflowMutation.Tasks,
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if err := updateSignalsRequested(ctx,
		tx,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedIDs,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
	}

	if workflowMutation.ClearBufferedEvents {
		if err := deleteBufferedEvents(ctx,
			tx,
			shardID,
			namespaceIDBytes,
			workflowID,
			runIDBytes,
		); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowMutationTx failed. Error: %v", err))
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
	workflowID := workflowSnapshot.WorkflowID
	namespaceID := workflowSnapshot.NamespaceID
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
	if err := lockAndCheckExecution(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
		workflowSnapshot.Condition,
		workflowSnapshot.DBRecordVersion,
	); err != nil {
		switch err.(type) {
		case *p.WorkflowConditionFailedError, *p.ConditionFailedError:
			return err
		default:
			return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to lock executions row. Error: %v", err))
		}
	}

	if err := updateExecution(ctx,
		tx,
		namespaceID,
		workflowID,
		workflowSnapshot.ExecutionInfoBlob,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		lastWriteVersion,
		workflowSnapshot.DBRecordVersion,
		shardID,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to update executions row. Erorr: %v", err))
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		workflowSnapshot.Tasks,
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear activity info map. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := deleteTimerInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear timer info map. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into timer info map after clearing. Error: %v", err))
	}

	if err := deleteChildExecutionInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear child execution info map. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into activity info map after clearing. Error: %v", err))
	}

	if err := deleteRequestCancelInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear request cancel info map. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into request cancel info map after clearing. Error: %v", err))
	}

	if err := deleteSignalInfoMap(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear signal info map. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into signal info map after clearing. Error: %v", err))
	}

	if err := deleteSignalsRequestedSet(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear signals requested set. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to insert into signals requested set after clearing. Error: %v", err))
	}

	if err := deleteBufferedEvents(ctx,
		tx,
		shardID,
		namespaceIDBytes,
		workflowID,
		runIDBytes,
	); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsReset failed. Failed to clear buffered events. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) applyWorkflowSnapshotTxAsNew(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	lastWriteVersion := workflowSnapshot.LastWriteVersion
	workflowID := workflowSnapshot.WorkflowID
	namespaceID := workflowSnapshot.NamespaceID
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
		namespaceID,
		workflowID,
		workflowSnapshot.ExecutionInfoBlob,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.NextEventID,
		lastWriteVersion,
		workflowSnapshot.DBRecordVersion,
		shardID,
	); err != nil {
		return err
	}

	if err := applyTasks(ctx,
		tx,
		shardID,
		workflowSnapshot.Tasks,
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into activity info map after clearing. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into timer info map after clearing. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into activity info map after clearing. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into request cancel info map after clearing. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into signal info map after clearing. Error: %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("applyWorkflowSnapshotTxAsNew failed. Failed to insert into signals requested set after clearing. Error: %v", err))
	}

	return nil
}

func applyTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	insertTasks map[tasks.Category][]p.InternalHistoryTask,
) error {

	var err error
	for category, tasksByCategory := range insertTasks {
		switch category.ID() {
		case tasks.CategoryIDTransfer:
			err = createTransferTasks(ctx, tx, shardID, tasksByCategory)
		case tasks.CategoryIDTimer:
			err = createTimerTasks(ctx, tx, shardID, tasksByCategory)
		case tasks.CategoryIDVisibility:
			err = createVisibilityTasks(ctx, tx, shardID, tasksByCategory)
		case tasks.CategoryIDReplication:
			err = createReplicationTasks(ctx, tx, shardID, tasksByCategory)
		default:
			err = serviceerror.NewInternal(fmt.Sprintf("Unknown task category: %v", category))
		}

		if err != nil {
			return err
		}
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
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("lockCurrentExecutionIfExists failed. Failed to get current_executions row for (shard,namespace,workflow) = (%v, %v, %v). Error: %v", shardID, namespaceID, workflowID, err))
		}
	}
	size := len(rows)
	if size > 1 {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("lockCurrentExecutionIfExists failed. Multiple current_executions rows for (shard,namespace,workflow) = (%v, %v, %v).", shardID, namespaceID, workflowID))
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
		LastWriteVersion: lastWriteVersion,
	}

	switch createMode {
	case p.CreateWorkflowModeUpdateCurrent:
		if err := updateCurrentExecution(ctx,
			tx,
			shardID,
			namespaceID,
			workflowID,
			runID,
			createRequestID,
			state,
			status,
			row.LastWriteVersion,
		); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("createOrUpdateCurrentExecution failed. Failed to reuse workflow ID. Error: %v", err))
		}
	case p.CreateWorkflowModeBrandNew:
		if _, err := tx.InsertIntoCurrentExecutions(ctx, &row); err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("createOrUpdateCurrentExecution failed. Failed to insert into current_executions table. Error: %v", err))
		}
	case p.CreateWorkflowModeBypassCurrent:
		// noop
	default:
		return fmt.Errorf("createOrUpdateCurrentExecution failed. Unknown workflow creation mode: %v", createMode)
	}

	return nil
}

func lockAndCheckExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	condition int64,
	dbRecordVersion int64,
) error {

	version, nextEventID, err := lockExecution(ctx, tx, shardID, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	if dbRecordVersion == 0 {
		if nextEventID != condition {
			return &p.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("lockAndCheckExecution failed. Next_event_id was %v when it should have been %v.", nextEventID, condition),
				NextEventID:     nextEventID,
				DBRecordVersion: version,
			}
		}
	} else {
		dbRecordVersion -= 1
		if version != dbRecordVersion {
			return &p.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("lockAndCheckExecution failed. DBRecordVersion expected: %v, actually %v.", dbRecordVersion, version),
				NextEventID:     nextEventID,
				DBRecordVersion: version,
			}
		}
	}

	return nil
}

func lockExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (int64, int64, error) {

	dbRecordVersion, nextEventID, err := tx.WriteLockExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, &p.ConditionFailedError{
				Msg: fmt.Sprintf("WriteLockExecutions failed. Unable to lock (shard, namespace, workflow, run) = (%v,%v,%v,%v) which does not exist.",
					shardID,
					namespaceID,
					workflowID,
					runID),
			}
		}
		return 0, 0, serviceerror.NewUnavailable(fmt.Sprintf("lockNextEventID failed. Error: %v", err))
	}
	return dbRecordVersion, nextEventID, nil
}

func createTransferTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	transferTasks []p.InternalHistoryTask,
) error {

	if len(transferTasks) == 0 {
		return nil
	}

	transferTasksRows := make([]sqlplugin.TransferTasksRow, 0, len(transferTasks))
	for _, task := range transferTasks {
		transferTasksRows = append(transferTasksRows, sqlplugin.TransferTasksRow{
			ShardID:      shardID,
			TaskID:       task.Key.TaskID,
			Data:         task.Blob.Data,
			DataEncoding: task.Blob.EncodingType.String(),
		})
	}

	result, err := tx.InsertIntoTransferTasks(ctx, transferTasksRows)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(transferTasks) {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(transferTasks), err))
	}
	return nil
}

func createTimerTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	timerTasks []p.InternalHistoryTask,
) error {

	if len(timerTasks) == 0 {
		return nil
	}

	timerTasksRows := make([]sqlplugin.TimerTasksRow, 0, len(timerTasks))
	for _, task := range timerTasks {
		timerTasksRows = append(timerTasksRows, sqlplugin.TimerTasksRow{
			ShardID:             shardID,
			VisibilityTimestamp: task.Key.FireTime,
			TaskID:              task.Key.TaskID,
			Data:                task.Blob.Data,
			DataEncoding:        task.Blob.EncodingType.String(),
		})
	}

	result, err := tx.InsertIntoTimerTasks(ctx, timerTasksRows)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTimerTasks failed. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTimerTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(timerTasks) {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTimerTasks failed. Inserted %v instead of %v rows into timer_tasks. Error: %v", rowsAffected, len(timerTasks), err))
	}
	return nil
}

func createReplicationTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	replicationTasks []p.InternalHistoryTask,
) error {

	if len(replicationTasks) == 0 {
		return nil
	}

	replicationTasksRows := make([]sqlplugin.ReplicationTasksRow, 0, len(replicationTasks))
	for _, task := range replicationTasks {
		replicationTasksRows = append(replicationTasksRows, sqlplugin.ReplicationTasksRow{
			ShardID:      shardID,
			TaskID:       task.Key.TaskID,
			Data:         task.Blob.Data,
			DataEncoding: task.Blob.EncodingType.String(),
		})
	}

	result, err := tx.InsertIntoReplicationTasks(ctx, replicationTasksRows)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createReplicationTasks failed. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createReplicationTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(replicationTasks) {
		return serviceerror.NewUnavailable(fmt.Sprintf("createReplicationTasks failed. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(replicationTasks), err))
	}
	return nil
}

func createVisibilityTasks(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	visibilityTasks []p.InternalHistoryTask,
) error {

	if len(visibilityTasks) == 0 {
		return nil
	}

	visibilityTasksRows := make([]sqlplugin.VisibilityTasksRow, 0, len(visibilityTasks))
	for _, task := range visibilityTasks {
		visibilityTasksRows = append(visibilityTasksRows, sqlplugin.VisibilityTasksRow{
			ShardID:      shardID,
			TaskID:       task.Key.TaskID,
			Data:         task.Blob.Data,
			DataEncoding: task.Blob.EncodingType.String(),
		})
	}

	result, err := tx.InsertIntoVisibilityTasks(ctx, visibilityTasksRows)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Error: %v", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Could not verify number of rows inserted. Error: %v", err))
	}

	if int(rowsAffected) != len(visibilityTasksRows) {
		return serviceerror.NewUnavailable(fmt.Sprintf("createTransferTasks failed. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(visibilityTasksRows), err))
	}
	return nil
}

func assertNotCurrentExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) error {
	currentRow, err := tx.LockCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			// allow bypassing no current record
			return nil
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("assertCurrentExecution failed. Unable to load current record. Error: %v", err))
	}
	return assertRunIDMismatch(runID, currentRow)
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
	lastWriteVersion int64,
) error {

	assertFn := func(currentRow *sqlplugin.CurrentExecutionsRow) error {
		if !bytes.Equal(currentRow.RunID, previousRunID) {
			return &p.CurrentWorkflowConditionFailedError{
				Msg: fmt.Sprintf(
					"assertRunIDAndUpdateCurrentExecution failed. current run ID: %v, request run ID: %v",
					currentRow.RunID,
					previousRunID,
				),
				RequestID:        currentRow.CreateRequestID,
				RunID:            currentRow.RunID.String(),
				State:            currentRow.State,
				Status:           currentRow.Status,
				LastWriteVersion: currentRow.LastWriteVersion,
			}
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
		return serviceerror.NewUnavailable(fmt.Sprintf("assertCurrentExecution failed. Unable to load current record. Error: %v", err))
	}
	return assertFn(currentRow)
}

func assertRunIDMismatch(requestRunID primitives.UUID, currentRow *sqlplugin.CurrentExecutionsRow) error {
	// zombie workflow creation with existence of current record, this is a noop
	if currentRow == nil {
		return nil
	}
	if bytes.Equal(currentRow.RunID, requestRunID) {
		return extractCurrentWorkflowConflictError(
			currentRow,
			fmt.Sprintf(
				"assertRunIDMismatch failed. request run ID: %v, current run ID: %v",
				requestRunID,
				currentRow.RunID.String(),
			),
		)
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
		LastWriteVersion: lastWriteVersion,
	})
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateCurrentExecution failed. Error: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateCurrentExecution failed. Failed to check number of rows updated in current_executions table. Error: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateCurrentExecution failed. %v rows of current_executions updated instead of 1.", rowsAffected))
	}
	return nil
}

func buildExecutionRow(
	namespaceID string,
	workflowID string,
	executionInfo *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	dbRecordVersion int64,
	shardID int32,
) (row *sqlplugin.ExecutionsRow, err error) {

	stateBlob, err := serialization.WorkflowExecutionStateToBlob(executionState)
	if err != nil {
		return nil, err
	}

	nsBytes, err := primitives.ParseUUID(namespaceID)
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
		WorkflowID:       workflowID,
		RunID:            ridBytes,
		NextEventID:      nextEventID,
		LastWriteVersion: lastWriteVersion,
		Data:             executionInfo.Data,
		DataEncoding:     executionInfo.EncodingType.String(),
		State:            stateBlob.Data,
		StateEncoding:    stateBlob.EncodingType.String(),
		DBRecordVersion:  dbRecordVersion,
	}, nil
}

func (m *sqlExecutionStore) createExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	namespaceID string,
	workflowID string,
	executionInfo *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	dbRecordVersion int64,
	shardID int32,
) error {

	row, err := buildExecutionRow(
		namespaceID,
		workflowID,
		executionInfo,
		executionState,
		nextEventID,
		lastWriteVersion,
		dbRecordVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.InsertIntoExecutions(ctx, row)
	if err != nil {
		if m.Db.IsDupEntryError(err) {
			return &p.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("Workflow execution already running. WorkflowId: %v", workflowID),
				NextEventID:     0,
				DBRecordVersion: 0,
			}
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("createExecution failed. Erorr: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("createExecution failed. Failed to verify number of rows affected. Erorr: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewNotFound(fmt.Sprintf("createExecution failed. Affected %v rows updated instead of 1.", rowsAffected))
	}

	return nil
}

func updateExecution(
	ctx context.Context,
	tx sqlplugin.Tx,
	namespaceID string,
	workflowID string,
	executionInfo *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	lastWriteVersion int64,
	dbRecordVersion int64,
	shardID int32,
) error {
	row, err := buildExecutionRow(
		namespaceID,
		workflowID,
		executionInfo,
		executionState,
		nextEventID,
		lastWriteVersion,
		dbRecordVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.UpdateExecutions(ctx, row)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateExecution failed. Erorr: %v", err))
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("updateExecution failed. Failed to verify number of rows affected. Erorr: %v", err))
	}
	if rowsAffected != 1 {
		return serviceerror.NewNotFound(fmt.Sprintf("updateExecution failed. Affected %v rows updated instead of 1.", rowsAffected))
	}

	return nil
}
