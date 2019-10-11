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

package sql

import (
	"bytes"
	"database/sql"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

func applyWorkflowMutationTx(
	tx sqldb.Tx,
	shardID int,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	executionInfo := workflowMutation.ExecutionInfo
	replicationState := workflowMutation.ReplicationState
	versionHistories := workflowMutation.VersionHistories
	startVersion := workflowMutation.StartVersion
	lastWriteVersion := workflowMutation.LastWriteVersion
	domainID := sqldb.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqldb.MustParseUUID(executionInfo.RunID)

	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	// TODO Remove me if UPDATE holds the lock to the end of a transaction
	if err := lockAndCheckNextEventID(tx,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowMutation.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx,
		executionInfo,
		replicationState,
		versionHistories,
		startVersion,
		lastWriteVersion,
		currentVersion,
		shardID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := applyTasks(tx,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowMutation.TransferTasks,
		workflowMutation.ReplicationTasks,
		workflowMutation.TimerTasks); err != nil {
		return err
	}

	if err := updateActivityInfos(tx,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfo,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfo,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfo,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedID,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if workflowMutation.ClearBufferedEvents {
		if err := deleteBufferedEvents(tx,
			shardID,
			domainID,
			workflowID,
			runID); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
			}
		}
	}

	if err := updateBufferedEvents(tx,
		workflowMutation.NewBufferedEvents,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}
	return nil
}

func applyWorkflowSnapshotTxAsReset(
	tx sqldb.Tx,
	shardID int,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	executionInfo := workflowSnapshot.ExecutionInfo
	replicationState := workflowSnapshot.ReplicationState
	versionHistories := workflowSnapshot.VersionHistories
	startVersion := workflowSnapshot.StartVersion
	lastWriteVersion := workflowSnapshot.LastWriteVersion
	domainID := sqldb.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqldb.MustParseUUID(executionInfo.RunID)

	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	// TODO Is there a way to modify the various map tables without fear of other people adding rows after we delete, without locking the executions row?
	if err := lockAndCheckNextEventID(tx,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowSnapshot.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx,
		executionInfo,
		replicationState,
		versionHistories,
		startVersion,
		lastWriteVersion,
		currentVersion,
		shardID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := applyTasks(tx,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks); err != nil {
		return err
	}

	if err := deleteActivityInfoMap(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear activity info map. Error: %v", err),
		}
	}

	if err := updateActivityInfos(tx,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteTimerInfoMap(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear timer info map. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into timer info map after clearing. Error: %v", err),
		}
	}

	if err := deleteChildExecutionInfoMap(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear child execution info map. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteRequestCancelInfoMap(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear request cancel info map. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into request cancel info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalInfoMap(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear signal info map. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into signal info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalsRequestedSet(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear signals requested set. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		workflowSnapshot.SignalRequestedIDs,
		"",
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into signals requested set after clearing. Error: %v", err),
		}
	}

	if err := deleteBufferedEvents(tx,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to clear buffered events. Error: %v", err),
		}
	}
	return nil
}

func applyWorkflowSnapshotTxAsNew(
	tx sqldb.Tx,
	shardID int,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	executionInfo := workflowSnapshot.ExecutionInfo
	replicationState := workflowSnapshot.ReplicationState
	versionHistories := workflowSnapshot.VersionHistories
	startVersion := workflowSnapshot.StartVersion
	lastWriteVersion := workflowSnapshot.LastWriteVersion
	domainID := sqldb.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqldb.MustParseUUID(executionInfo.RunID)

	// TODO remove once 2DC is deprecated
	//  since current version is only used by 2DC
	currentVersion := lastWriteVersion
	if replicationState != nil {
		currentVersion = replicationState.CurrentVersion
	}

	if err := createExecution(tx,
		executionInfo,
		replicationState,
		versionHistories,
		startVersion,
		lastWriteVersion,
		currentVersion,
		shardID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := applyTasks(tx,
		shardID,
		domainID,
		workflowID,
		runID,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.TimerTasks); err != nil {
		return err
	}

	if err := updateActivityInfos(tx,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into timer info map after clearing. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into request cancel info map after clearing. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into signal info map after clearing. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		workflowSnapshot.SignalRequestedIDs,
		"",
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Failed to insert into signals requested set after clearing. Error: %v", err),
		}
	}

	return nil
}

func applyTasks(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
	transferTasks []p.Task,
	replicationTasks []p.Task,
	timerTasks []p.Task,
) error {

	if err := createTransferTasks(tx,
		transferTasks,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
		}
	}

	if err := createReplicationTasks(tx,
		replicationTasks,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
		}
	}

	if err := createTimerTasks(tx,
		timerTasks,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
		}
	}

	return nil
}

// lockCurrentExecutionIfExists returns current execution or nil if none is found for the workflowID
// locking it in the DB
func lockCurrentExecutionIfExists(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
) (*sqldb.CurrentExecutionsRow, error) {

	rows, err := tx.LockCurrentExecutionsJoinExecutions(&sqldb.CurrentExecutionsFilter{
		ShardID: int64(shardID), DomainID: domainID, WorkflowID: workflowID})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to get current_executions row for (shard,domain,workflow) = (%v, %v, %v). Error: %v", shardID, domainID, workflowID, err),
			}
		}
	}
	size := len(rows)
	if size > 1 {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Multiple current_executions rows for (shard,domain,workflow) = (%v, %v, %v).", shardID, domainID, workflowID),
		}
	}
	if size == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func createOrUpdateCurrentExecution(
	tx sqldb.Tx,
	createMode p.CreateWorkflowMode,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
	state int,
	closeStatus int,
	createRequestID string,
	startVersion int64,
	lastWriteVersion int64,
) error {

	row := sqldb.CurrentExecutionsRow{
		ShardID:          int64(shardID),
		DomainID:         domainID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  createRequestID,
		State:            state,
		CloseStatus:      closeStatus,
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,
	}

	switch createMode {
	case p.CreateWorkflowModeContinueAsNew:
		if err := updateCurrentExecution(tx,
			shardID,
			domainID,
			workflowID,
			runID,
			createRequestID,
			state,
			closeStatus,
			row.StartVersion,
			row.LastWriteVersion); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to continue as new. Error: %v", err),
			}
		}
	case p.CreateWorkflowModeWorkflowIDReuse:
		if err := updateCurrentExecution(tx,
			shardID,
			domainID,
			workflowID,
			runID,
			createRequestID,
			state,
			closeStatus,
			row.StartVersion,
			row.LastWriteVersion); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to reuse workflow ID. Error: %v", err),
			}
		}
	case p.CreateWorkflowModeBrandNew:
		if _, err := tx.InsertIntoCurrentExecutions(&row); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to insert into current_executions table. Error: %v", err),
			}
		}
	case p.CreateWorkflowModeZombie:
		// noop
	default:
		return fmt.Errorf("Unknown workflow creation mode: %v", createMode)
	}

	return nil
}

func lockAndCheckNextEventID(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
	condition int64,
) error {

	nextEventID, err := lockNextEventID(tx, shardID, domainID, workflowID, runID)
	if err != nil {
		return err
	}
	if *nextEventID != condition {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("next_event_id was %v when it should have been %v.", nextEventID, condition),
		}
	}
	return nil
}

func lockNextEventID(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
) (*int64, error) {

	nextEventID, err := tx.WriteLockExecutions(&sqldb.ExecutionsFilter{
		ShardID:    shardID,
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf(
					"Failed to lock executions row with (shard, domain, workflow, run) = (%v,%v,%v,%v) which does not exist.",
					shardID,
					domainID,
					workflowID,
					runID,
				),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock executions row. Error: %v", err),
		}
	}
	result := int64(nextEventID)
	return &result, nil
}

func createTransferTasks(
	tx sqldb.Tx,
	transferTasks []p.Task,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
) error {

	if len(transferTasks) == 0 {
		return nil
	}

	transferTasksRows := make([]sqldb.TransferTasksRow, len(transferTasks))
	for i, task := range transferTasks {
		info := &sqlblobs.TransferTaskInfo{
			DomainID:         domainID,
			WorkflowID:       &workflowID,
			RunID:            runID,
			TargetDomainID:   domainID,
			TargetWorkflowID: common.StringPtr(p.TransferTaskTransferTargetWorkflowID),
			ScheduleID:       common.Int64Ptr(0),
		}

		transferTasksRows[i].ShardID = shardID
		transferTasksRows[i].TaskID = task.GetTaskID()

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			info.TargetDomainID = sqldb.MustParseUUID(task.(*p.ActivityTask).DomainID)
			info.TaskList = &task.(*p.ActivityTask).TaskList
			info.ScheduleID = &task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			info.TargetDomainID = sqldb.MustParseUUID(task.(*p.DecisionTask).DomainID)
			info.TaskList = &task.(*p.DecisionTask).TaskList
			info.ScheduleID = &task.(*p.DecisionTask).ScheduleID

		case p.TransferTaskTypeCancelExecution:
			info.TargetDomainID = sqldb.MustParseUUID(task.(*p.CancelExecutionTask).TargetDomainID)
			info.TargetWorkflowID = &task.(*p.CancelExecutionTask).TargetWorkflowID
			if task.(*p.CancelExecutionTask).TargetRunID != "" {
				info.TargetRunID = sqldb.MustParseUUID(task.(*p.CancelExecutionTask).TargetRunID)
			}
			info.TargetChildWorkflowOnly = &task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			info.ScheduleID = &task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			info.TargetDomainID = sqldb.MustParseUUID(task.(*p.SignalExecutionTask).TargetDomainID)
			info.TargetWorkflowID = &task.(*p.SignalExecutionTask).TargetWorkflowID
			if task.(*p.SignalExecutionTask).TargetRunID != "" {
				info.TargetRunID = sqldb.MustParseUUID(task.(*p.SignalExecutionTask).TargetRunID)
			}
			info.TargetChildWorkflowOnly = &task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			info.ScheduleID = &task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			info.TargetDomainID = sqldb.MustParseUUID(task.(*p.StartChildExecutionTask).TargetDomainID)
			info.TargetWorkflowID = &task.(*p.StartChildExecutionTask).TargetWorkflowID
			info.ScheduleID = &task.(*p.StartChildExecutionTask).InitiatedID

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

		info.TaskType = common.Int16Ptr(int16(task.GetType()))
		info.Version = common.Int64Ptr(task.GetVersion())
		info.VisibilityTimestampNanos = common.Int64Ptr(task.GetVisibilityTimestamp().UnixNano())

		blob, err := transferTaskInfoToBlob(info)
		if err != nil {
			return err
		}
		transferTasksRows[i].Data = blob.Data
		transferTasksRows[i].DataEncoding = string(blob.Encoding)
	}

	result, err := tx.InsertIntoTransferTasks(transferTasksRows)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Error: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Could not verify number of rows inserted. Error: %v", err),
		}
	}

	if int(rowsAffected) != len(transferTasks) {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(transferTasks), err),
		}
	}

	return nil
}

func createReplicationTasks(
	tx sqldb.Tx,
	replicationTasks []p.Task,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
) error {

	if len(replicationTasks) == 0 {
		return nil
	}
	replicationTasksRows := make([]sqldb.ReplicationTasksRow, len(replicationTasks))

	for i, task := range replicationTasks {

		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		activityScheduleID := common.EmptyEventID
		var lastReplicationInfo map[string]*sqlblobs.ReplicationInfo

		var branchToken, newRunBranchToken []byte
		var resetWorkflow bool

		switch task.GetType() {
		case p.ReplicationTaskTypeHistory:
			historyReplicationTask, ok := task.(*p.HistoryReplicationTask)
			if !ok {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to cast %v to HistoryReplicationTask", task),
				}
			}
			firstEventID = historyReplicationTask.FirstEventID
			nextEventID = historyReplicationTask.NextEventID
			version = task.GetVersion()
			branchToken = historyReplicationTask.BranchToken
			newRunBranchToken = historyReplicationTask.NewRunBranchToken
			resetWorkflow = historyReplicationTask.ResetWorkflow
			lastReplicationInfo = make(map[string]*sqlblobs.ReplicationInfo, len(historyReplicationTask.LastReplicationInfo))
			for k, v := range historyReplicationTask.LastReplicationInfo {
				lastReplicationInfo[k] = &sqlblobs.ReplicationInfo{Version: &v.Version, LastEventID: &v.LastEventID}
			}

		case p.ReplicationTaskTypeSyncActivity:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			lastReplicationInfo = map[string]*sqlblobs.ReplicationInfo{}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknown replication task: %v", task.GetType()),
			}
		}

		blob, err := replicationTaskInfoToBlob(&sqlblobs.ReplicationTaskInfo{
			DomainID:            domainID,
			WorkflowID:          &workflowID,
			RunID:               runID,
			TaskType:            common.Int16Ptr(int16(task.GetType())),
			FirstEventID:        &firstEventID,
			NextEventID:         &nextEventID,
			Version:             &version,
			LastReplicationInfo: lastReplicationInfo,
			ScheduledID:         &activityScheduleID,
			BranchToken:         branchToken,
			NewRunBranchToken:   newRunBranchToken,
			ResetWorkflow:       &resetWorkflow,
		})
		if err != nil {
			return err
		}
		replicationTasksRows[i].ShardID = shardID
		replicationTasksRows[i].TaskID = task.GetTaskID()
		replicationTasksRows[i].Data = blob.Data
		replicationTasksRows[i].DataEncoding = string(blob.Encoding)
	}

	result, err := tx.InsertIntoReplicationTasks(replicationTasksRows)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Error: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Could not verify number of rows inserted. Error: %v", err),
		}
	}

	if int(rowsAffected) != len(replicationTasks) {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(replicationTasks), err),
		}
	}

	return nil
}

func createTimerTasks(
	tx sqldb.Tx,
	timerTasks []p.Task,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
) error {

	if len(timerTasks) > 0 {
		timerTasksRows := make([]sqldb.TimerTasksRow, len(timerTasks))

		for i, task := range timerTasks {
			info := &sqlblobs.TimerTaskInfo{}
			switch t := task.(type) {
			case *p.DecisionTimeoutTask:
				info.EventID = &t.EventID
				info.TimeoutType = common.Int16Ptr(int16(t.TimeoutType))
				info.ScheduleAttempt = &t.ScheduleAttempt

			case *p.ActivityTimeoutTask:
				info.EventID = &t.EventID
				info.TimeoutType = common.Int16Ptr(int16(t.TimeoutType))
				info.ScheduleAttempt = &t.Attempt

			case *p.UserTimerTask:
				info.EventID = &t.EventID

			case *p.ActivityRetryTimerTask:
				info.EventID = &t.EventID
				info.ScheduleAttempt = common.Int64Ptr(int64(t.Attempt))

			case *p.WorkflowBackoffTimerTask:
				info.EventID = &t.EventID
				info.TimeoutType = common.Int16Ptr(int16(t.TimeoutType))

			case *p.WorkflowTimeoutTask:
				// noop

			case *p.DeleteHistoryEventTask:
				// noop

			default:
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Unknown timer task: %v", task.GetType()),
				}
			}

			info.DomainID = domainID
			info.WorkflowID = &workflowID
			info.RunID = runID
			info.Version = common.Int64Ptr(task.GetVersion())
			info.TaskType = common.Int16Ptr(int16(task.GetType()))

			blob, err := timerTaskInfoToBlob(info)
			if err != nil {
				return err
			}

			timerTasksRows[i].ShardID = shardID
			timerTasksRows[i].VisibilityTimestamp = task.GetVisibilityTimestamp()
			timerTasksRows[i].TaskID = task.GetTaskID()
			timerTasksRows[i].Data = blob.Data
			timerTasksRows[i].DataEncoding = string(blob.Encoding)
		}

		result, err := tx.InsertIntoTimerTasks(timerTasksRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Error: %v", err),
			}
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Could not verify number of rows inserted. Error: %v", err),
			}
		}

		if int(rowsAffected) != len(timerTasks) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Inserted %v instead of %v rows into timer_tasks. Error: %v", rowsAffected, len(timerTasks), err),
			}
		}
	}

	return nil
}

func assertNotCurrentExecution(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
) error {

	assertFn := func(currentRow *sqldb.CurrentExecutionsRow) error {
		return assertRunIDMismatch(runID, currentRow.RunID)
	}
	if err := assertCurrentExecution(tx, shardID, domainID, workflowID, assertFn); err != nil {
		return err
	}
	return nil
}

func assertRunIDAndUpdateCurrentExecution(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	newRunID sqldb.UUID,
	previousRunID sqldb.UUID,
	createRequestID string,
	state int,
	closeStatus int,
	startVersion int64,
	lastWriteVersion int64,
) error {

	assertFn := func(currentRow *sqldb.CurrentExecutionsRow) error {
		if !bytes.Equal(currentRow.RunID, previousRunID) {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"Update current record failed failed. Current run ID was %v, expected %v",
				currentRow.RunID,
				previousRunID,
			)}
		}
		return nil
	}
	if err := assertCurrentExecution(tx, shardID, domainID, workflowID, assertFn); err != nil {
		return err
	}

	return updateCurrentExecution(tx, shardID, domainID, workflowID, newRunID, createRequestID, state, closeStatus, startVersion, lastWriteVersion)
}

func assertAndUpdateCurrentExecution(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	newRunID sqldb.UUID,
	previousRunID sqldb.UUID,
	previousLastWriteVersion int64,
	previousState int,
	createRequestID string,
	state int,
	closeStatus int,
	startVersion int64,
	lastWriteVersion int64,
) error {

	assertFn := func(currentRow *sqldb.CurrentExecutionsRow) error {
		if !bytes.Equal(currentRow.RunID, previousRunID) {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"Update current record failed failed. Current run ID was %v, expected %v",
				currentRow.RunID,
				previousRunID,
			)}
		}
		if currentRow.LastWriteVersion != previousLastWriteVersion {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"Update current record failed failed. Current last write version was %v, expected %v",
				currentRow.LastWriteVersion,
				previousLastWriteVersion,
			)}
		}
		if currentRow.State != previousState {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"Update current record failed failed. Current state %v, expected %v",
				currentRow.State,
				previousState,
			)}
		}
		return nil
	}
	if err := assertCurrentExecution(tx, shardID, domainID, workflowID, assertFn); err != nil {
		return err
	}

	return updateCurrentExecution(tx, shardID, domainID, workflowID, newRunID, createRequestID, state, closeStatus, startVersion, lastWriteVersion)
}

func assertCurrentExecution(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	assertFn func(currentRow *sqldb.CurrentExecutionsRow) error,
) error {

	currentRow, err := tx.LockCurrentExecutions(&sqldb.CurrentExecutionsFilter{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Unable to load current record. Error: %v", err),
		}
	}
	return assertFn(currentRow)
}

func assertRunIDMismatch(runID sqldb.UUID, currentRunID sqldb.UUID) error {
	// zombie workflow creation with existence of current record, this is a noop
	if bytes.Equal(currentRunID, runID) {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"Assert not current record failed failed. Current run ID was %v, input %v",
			currentRunID,
			runID,
		)}
	}
	return nil
}

func updateCurrentExecution(
	tx sqldb.Tx,
	shardID int,
	domainID sqldb.UUID,
	workflowID string,
	runID sqldb.UUID,
	createRequestID string,
	state int,
	closeStatus int,
	startVersion int64,
	lastWriteVersion int64,
) error {

	result, err := tx.UpdateCurrentExecutions(&sqldb.CurrentExecutionsRow{
		ShardID:          int64(shardID),
		DomainID:         domainID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  createRequestID,
		State:            state,
		CloseStatus:      closeStatus,
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to update current_executions table. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to check number of rows updated in current_executions table. Error: %v", err),
		}
	}
	if rowsAffected != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. %v rows of current_executions updated instead of 1.", rowsAffected),
		}
	}
	return nil
}

func buildExecutionRow(
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *p.DataBlob,
	startVersion int64,
	lastWriteVersion int64,
	currentVersion int64,
	shardID int,
) (row *sqldb.ExecutionsRow, err error) {

	info := &sqlblobs.WorkflowExecutionInfo{
		TaskList:                                &executionInfo.TaskList,
		WorkflowTypeName:                        &executionInfo.WorkflowTypeName,
		WorkflowTimeoutSeconds:                  &executionInfo.WorkflowTimeout,
		DecisionTaskTimeoutSeconds:              &executionInfo.DecisionTimeoutValue,
		ExecutionContext:                        executionInfo.ExecutionContext,
		State:                                   common.Int32Ptr(int32(executionInfo.State)),
		CloseStatus:                             common.Int32Ptr(int32(executionInfo.CloseStatus)),
		LastFirstEventID:                        &executionInfo.LastFirstEventID,
		LastEventTaskID:                         &executionInfo.LastEventTaskID,
		LastProcessedEvent:                      &executionInfo.LastProcessedEvent,
		StartTimeNanos:                          common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
		LastUpdatedTimeNanos:                    common.Int64Ptr(executionInfo.LastUpdatedTimestamp.UnixNano()),
		CreateRequestID:                         &executionInfo.CreateRequestID,
		DecisionVersion:                         &executionInfo.DecisionVersion,
		DecisionScheduleID:                      &executionInfo.DecisionScheduleID,
		DecisionStartedID:                       &executionInfo.DecisionStartedID,
		DecisionRequestID:                       &executionInfo.DecisionRequestID,
		DecisionTimeout:                         &executionInfo.DecisionTimeout,
		DecisionAttempt:                         &executionInfo.DecisionAttempt,
		DecisionStartedTimestampNanos:           &executionInfo.DecisionStartedTimestamp,
		DecisionScheduledTimestampNanos:         &executionInfo.DecisionScheduledTimestamp,
		DecisionOriginalScheduledTimestampNanos: &executionInfo.DecisionOriginalScheduledTimestamp,
		StickyTaskList:                          &executionInfo.StickyTaskList,
		StickyScheduleToStartTimeout:            common.Int64Ptr(int64(executionInfo.StickyScheduleToStartTimeout)),
		ClientLibraryVersion:                    &executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:                    &executionInfo.ClientFeatureVersion,
		ClientImpl:                              &executionInfo.ClientImpl,
		SignalCount:                             common.Int64Ptr(int64(executionInfo.SignalCount)),
		HistorySize:                             &executionInfo.HistorySize,
		CronSchedule:                            &executionInfo.CronSchedule,
		CompletionEventBatchID:                  &executionInfo.CompletionEventBatchID,
		HasRetryPolicy:                          &executionInfo.HasRetryPolicy,
		RetryAttempt:                            common.Int64Ptr(int64(executionInfo.Attempt)),
		RetryInitialIntervalSeconds:             &executionInfo.InitialInterval,
		RetryBackoffCoefficient:                 &executionInfo.BackoffCoefficient,
		RetryMaximumIntervalSeconds:             &executionInfo.MaximumInterval,
		RetryMaximumAttempts:                    &executionInfo.MaximumAttempts,
		RetryExpirationSeconds:                  &executionInfo.ExpirationSeconds,
		RetryExpirationTimeNanos:                common.Int64Ptr(executionInfo.ExpirationTime.UnixNano()),
		RetryNonRetryableErrors:                 executionInfo.NonRetriableErrors,
		EventBranchToken:                        executionInfo.BranchToken,
		AutoResetPoints:                         executionInfo.AutoResetPoints.Data,
		AutoResetPointsEncoding:                 common.StringPtr(string(executionInfo.AutoResetPoints.GetEncoding())),
		SearchAttributes:                        executionInfo.SearchAttributes,
		Memo:                                    executionInfo.Memo,
	}

	completionEvent := executionInfo.CompletionEvent
	if completionEvent != nil {
		info.CompletionEvent = completionEvent.Data
		info.CompletionEventEncoding = common.StringPtr(string(completionEvent.Encoding))
	}

	info.StartVersion = &startVersion
	info.CurrentVersion = &currentVersion
	if replicationState == nil && versionHistories == nil {
		// this is allowed
	} else if replicationState != nil {
		info.LastWriteEventID = &replicationState.LastWriteEventID
		info.LastReplicationInfo = make(map[string]*sqlblobs.ReplicationInfo, len(replicationState.LastReplicationInfo))
		for k, v := range replicationState.LastReplicationInfo {
			info.LastReplicationInfo[k] = &sqlblobs.ReplicationInfo{Version: &v.Version, LastEventID: &v.LastEventID}
		}
	} else if versionHistories != nil {
		info.VersionHistories = versionHistories.Data
		info.VersionHistoriesEncoding = common.StringPtr(string(versionHistories.GetEncoding()))
	} else {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("build workflow execution with both version histories and replication state."),
		}
	}

	if executionInfo.ParentDomainID != "" {
		info.ParentDomainID = sqldb.MustParseUUID(executionInfo.ParentDomainID)
		info.ParentWorkflowID = &executionInfo.ParentWorkflowID
		info.ParentRunID = sqldb.MustParseUUID(executionInfo.ParentRunID)
		info.InitiatedID = &executionInfo.InitiatedID
		info.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		info.CancelRequested = common.BoolPtr(true)
		info.CancelRequestID = &executionInfo.CancelRequestID
	}

	blob, err := workflowExecutionInfoToBlob(info)
	if err != nil {
		return nil, err
	}

	return &sqldb.ExecutionsRow{
		ShardID:          shardID,
		DomainID:         sqldb.MustParseUUID(executionInfo.DomainID),
		WorkflowID:       executionInfo.WorkflowID,
		RunID:            sqldb.MustParseUUID(executionInfo.RunID),
		NextEventID:      int64(executionInfo.NextEventID),
		LastWriteVersion: lastWriteVersion,
		Data:             blob.Data,
		DataEncoding:     string(blob.Encoding),
	}, nil
}

func createExecution(
	tx sqldb.Tx,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *p.DataBlob,
	startVersion int64,
	lastWriteVersion int64,
	currentVersion int64,
	shardID int,
) error {

	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus); err != nil {
		return err
	}

	// TODO we should set the start time and last update time on business logic layer
	executionInfo.StartTimestamp = time.Now()
	executionInfo.LastUpdatedTimestamp = executionInfo.StartTimestamp

	row, err := buildExecutionRow(
		executionInfo,
		replicationState,
		versionHistories,
		startVersion,
		lastWriteVersion,
		currentVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.InsertIntoExecutions(row)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to insert executions row. Erorr: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to insert executions row. Failed to verify number of rows affected. Erorr: %v", err),
		}
	}
	if rowsAffected != 1 {
		return &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Failed to insert executions row. Affected %v rows updated instead of 1.", rowsAffected),
		}
	}

	return nil
}

func updateExecution(
	tx sqldb.Tx,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	versionHistories *p.DataBlob,
	startVersion int64,
	lastWriteVersion int64,
	currentVersion int64,
	shardID int,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateCloseStatus(
		executionInfo.State,
		executionInfo.CloseStatus); err != nil {
		return err
	}

	// TODO we should set the last update time on business logic layer
	executionInfo.LastUpdatedTimestamp = time.Now()

	row, err := buildExecutionRow(
		executionInfo,
		replicationState,
		versionHistories,
		startVersion,
		lastWriteVersion,
		currentVersion,
		shardID,
	)
	if err != nil {
		return err
	}
	result, err := tx.UpdateExecutions(row)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Erorr: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Failed to verify number of rows affected. Erorr: %v", err),
		}
	}
	if rowsAffected != 1 {
		return &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Failed to update executions row. Affected %v rows updated instead of 1.", rowsAffected),
		}
	}

	return nil
}
