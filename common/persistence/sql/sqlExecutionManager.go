// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

type sqlExecutionManager struct {
	sqlStore
	shardID int
}

// txExecuteShardLocked executes f under transaction and with read lock on shard row
func (m *sqlExecutionManager) txExecuteShardLocked(operation string, rangeID int64, f func(tx sqldb.Tx) error) error {
	return m.txExecute(operation, func(tx sqldb.Tx) error {
		if err := readLockShard(tx, m.shardID, rangeID); err != nil {
			return err
		}
		err := f(tx)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *sqlExecutionManager) GetShardID() int {
	return m.shardID
}

func (m *sqlExecutionManager) CreateWorkflowExecution(request *p.CreateWorkflowExecutionRequest) (response *p.CreateWorkflowExecutionResponse, err error) {
	err = m.txExecuteShardLocked("CreateWorkflowExecution", request.RangeID, func(tx sqldb.Tx) error {
		response, err = m.createWorkflowExecutionTx(tx, request)
		return err
	})
	return
}

func (m *sqlExecutionManager) createWorkflowExecutionTx(tx sqldb.Tx, request *p.CreateWorkflowExecutionRequest) (*p.CreateWorkflowExecutionResponse, error) {
	if request.CreateWorkflowMode == p.CreateWorkflowModeContinueAsNew {
		return nil, &workflow.InternalServiceError{
			Message: "CreateWorkflowExecution operation failed. Invalid CreateWorkflowModeContinueAsNew is used",
		}
	}
	var err error
	var row *sqldb.CurrentExecutionsRow
	workflowID := *request.Execution.WorkflowId
	domainID := sqldb.MustParseUUID(request.DomainID)
	if row, err = lockCurrentExecutionIfExists(tx, m.shardID, domainID, workflowID); err != nil {
		return nil, err
	}

	lastWriteVersion := common.EmptyVersion

	if row != nil {
		switch request.CreateWorkflowMode {
		case p.CreateWorkflowModeBrandNew:
			if request.ReplicationState != nil {
				lastWriteVersion = row.LastWriteVersion
			}

			return nil, &p.WorkflowExecutionAlreadyStartedError{
				Msg:              fmt.Sprintf("Workflow execution already running. WorkflowId: %v", row.WorkflowID),
				StartRequestID:   row.CreateRequestID,
				RunID:            row.RunID.String(),
				State:            int(row.State),
				CloseStatus:      int(row.CloseStatus),
				LastWriteVersion: lastWriteVersion,
			}
		case p.CreateWorkflowModeWorkflowIDReuse:
			if request.PreviousLastWriteVersion != row.LastWriteVersion {
				return nil, &p.CurrentWorkflowConditionFailedError{
					Msg: fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, "+
						"LastWriteVersion: %v, PreviousLastWriteVersion: %v",
						workflowID, row.LastWriteVersion, request.PreviousLastWriteVersion),
				}
			}
			if row.State != p.WorkflowStateCompleted {
				return nil, &p.CurrentWorkflowConditionFailedError{
					Msg: fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, "+
						"State: %v, Expected: %v",
						workflowID, row.State, p.WorkflowStateCompleted),
				}
			}
			runIDStr := row.RunID.String()
			if runIDStr != request.PreviousRunID {
				return nil, &p.CurrentWorkflowConditionFailedError{
					Msg: fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, "+
						"RunID: %v, PreviousRunID: %v",
						workflowID, runIDStr, request.PreviousRunID),
				}
			}
		}
	}

	runID := sqldb.MustParseUUID(*request.Execution.RunId)
	if err := createOrUpdateCurrentExecution(tx, request, m.shardID, domainID, runID); err != nil {
		return nil, err
	}

	if err := createExecution(tx, request, m.shardID, domainID, runID, time.Now()); err != nil {
		return nil, err
	}

	if err := createTransferTasks(tx, request.TransferTasks, m.shardID, domainID, workflowID, runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
		}
	}

	if err := createReplicationTasks(tx,
		request.ReplicationTasks,
		m.shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
		}
	}

	if err := createTimerTasks(tx,
		request.TimerTasks,
		nil,
		m.shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
		}
	}
	return &p.CreateWorkflowExecutionResponse{}, nil
}

func (m *sqlExecutionManager) GetWorkflowExecution(request *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	domainID := sqldb.MustParseUUID(request.DomainID)
	runID := sqldb.MustParseUUID(*request.Execution.RunId)
	wfID := *request.Execution.WorkflowId
	execution, err := m.db.SelectFromExecutions(&sqldb.ExecutionsFilter{
		ShardID: m.shardID, DomainID: domainID, WorkflowID: wfID, RunID: runID})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					*request.Execution.WorkflowId,
					*request.Execution.RunId),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution failed. Error: %v", err),
		}
	}

	var state p.InternalWorkflowMutableState
	state.ExecutionInfo = &p.InternalWorkflowExecutionInfo{
		DomainID:                     execution.DomainID.String(),
		WorkflowID:                   execution.WorkflowID,
		RunID:                        execution.RunID.String(),
		TaskList:                     execution.TaskList,
		WorkflowTypeName:             execution.WorkflowTypeName,
		WorkflowTimeout:              int32(execution.WorkflowTimeoutSeconds),
		DecisionTimeoutValue:         int32(execution.DecisionTaskTimeoutMinutes),
		State:                        int(execution.State),
		CloseStatus:                  int(execution.CloseStatus),
		LastFirstEventID:             execution.LastFirstEventID,
		NextEventID:                  execution.NextEventID,
		LastProcessedEvent:           execution.LastProcessedEvent,
		StartTimestamp:               execution.StartTime,
		LastUpdatedTimestamp:         execution.LastUpdatedTime,
		CreateRequestID:              execution.CreateRequestID,
		DecisionVersion:              execution.DecisionVersion,
		DecisionScheduleID:           execution.DecisionScheduleID,
		DecisionStartedID:            execution.DecisionStartedID,
		DecisionRequestID:            execution.DecisionRequestID,
		DecisionTimeout:              int32(execution.DecisionTimeout),
		DecisionAttempt:              execution.DecisionAttempt,
		DecisionTimestamp:            execution.DecisionTimestamp,
		StickyTaskList:               execution.StickyTaskList,
		StickyScheduleToStartTimeout: int32(execution.StickyScheduleToStartTimeout),
		ClientLibraryVersion:         execution.ClientLibraryVersion,
		ClientFeatureVersion:         execution.ClientFeatureVersion,
		ClientImpl:                   execution.ClientImpl,
		SignalCount:                  int32(execution.SignalCount),
		CronSchedule:                 execution.CronSchedule,
		CompletionEventBatchID:       common.EmptyEventID,
	}

	if execution.ExecutionContext != nil && len(*execution.ExecutionContext) > 0 {
		state.ExecutionInfo.ExecutionContext = *execution.ExecutionContext
	}

	if execution.LastWriteEventID != nil {
		state.ReplicationState = &p.ReplicationState{}
		state.ReplicationState.StartVersion = execution.StartVersion
		state.ReplicationState.CurrentVersion = execution.CurrentVersion
		state.ReplicationState.LastWriteVersion = execution.LastWriteVersion
		state.ReplicationState.LastWriteEventID = *execution.LastWriteEventID
	}
	if execution.LastReplicationInfo != nil && len(*execution.LastReplicationInfo) > 0 {
		state.ReplicationState.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
		if err := gobDeserialize(*execution.LastReplicationInfo, &state.ReplicationState.LastReplicationInfo); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to deserialize LastReplicationInfo. Error: %v", err),
			}
		}
	}

	if execution.ParentDomainID != nil {
		state.ExecutionInfo.ParentDomainID = execution.ParentDomainID.String()
		state.ExecutionInfo.ParentWorkflowID = *execution.ParentWorkflowID
		state.ExecutionInfo.ParentRunID = execution.ParentRunID.String()
		state.ExecutionInfo.InitiatedID = *execution.InitiatedID
		if state.ExecutionInfo.CompletionEvent != nil {
			state.ExecutionInfo.CompletionEvent = nil
		}
	}

	if execution.CancelRequested != nil && (*execution.CancelRequested != 0) {
		state.ExecutionInfo.CancelRequested = true
		state.ExecutionInfo.CancelRequestID = *execution.CancelRequestID
	}

	if execution.CompletionEventBatchID != nil {
		state.ExecutionInfo.CompletionEventBatchID = *execution.CompletionEventBatchID
	}

	if execution.CompletionEvent != nil {
		state.ExecutionInfo.CompletionEvent = p.NewDataBlob(*execution.CompletionEvent,
			common.EncodingType(*execution.CompletionEventEncoding))
	}
	{
		var err error
		state.ActivitInfos, err = getActivityInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get activity info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.TimerInfos, err = getTimerInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get timer info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.ChildExecutionInfos, err = getChildExecutionInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get child execution info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.RequestCancelInfos, err = getRequestCancelInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get request cancel info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.SignalInfos, err = getSignalInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get signal info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.BufferedEvents, err = getBufferedEvents(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get buffered events. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.BufferedReplicationTasks, err = getBufferedReplicationTasks(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get buffered replication tasks. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.SignalRequestedIDs, err = getSignalsRequested(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get signals requested. Error: %v", err),
			}
		}
	}

	return &p.InternalGetWorkflowExecutionResponse{State: &state}, nil
}

func getBufferedEvents(
	db sqldb.Interface, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) ([]*p.DataBlob, error) {
	rows, err := db.SelectFromBufferedEvents(&sqldb.BufferedEventsFilter{
		ShardID:    shardID,
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("getBufferedEvents operation failed. Select failed: %v", err),
		}
	}
	var result []*p.DataBlob
	for _, row := range rows {
		result = append(result, p.NewDataBlob(row.Data, common.EncodingType(row.DataEncoding)))
	}
	return result, nil
}

func (m *sqlExecutionManager) UpdateWorkflowExecution(request *p.InternalUpdateWorkflowExecutionRequest) error {
	return m.txExecuteShardLocked("UpdateWorkflowExecution", request.RangeID, func(tx sqldb.Tx) error {
		return m.updateWorkflowExecutionTx(tx, request)
	})
}

func (m *sqlExecutionManager) updateWorkflowExecutionTx(tx sqldb.Tx, request *p.InternalUpdateWorkflowExecutionRequest) error {
	executionInfo := request.ExecutionInfo
	domainID := sqldb.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqldb.MustParseUUID(executionInfo.RunID)
	shardID := m.shardID
	if err := createTransferTasks(tx, request.TransferTasks, shardID, domainID, workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
		}
	}

	if err := createReplicationTasks(tx, request.ReplicationTasks, shardID, domainID, workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
		}
	}

	if err := createTimerTasks(tx, request.TimerTasks, request.DeleteTimerTask, shardID, domainID, workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
		}
	}

	// TODO Remove me if UPDATE holds the lock to the end of a transaction
	if err := lockAndCheckNextEventID(tx, shardID, domainID, workflowID, runID, request.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx, executionInfo, request.ReplicationState, shardID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := updateActivityInfos(tx, request.UpsertActivityInfos, request.DeleteActivityInfos, shardID, domainID,
		workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx, request.UpserTimerInfos, request.DeleteTimerInfos, shardID, domainID,
		workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx, request.UpsertChildExecutionInfos, request.DeleteChildExecutionInfo,
		shardID, domainID, workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx, request.UpsertRequestCancelInfos, request.DeleteRequestCancelInfo,
		shardID, domainID, workflowID, runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx, request.UpsertSignalInfos, request.DeleteSignalInfo, shardID, domainID, workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateBufferedEvents(tx, request.NewBufferedEvents, request.ClearBufferedEvents, shardID, domainID,
		workflowID, runID, request.Condition, request.RangeID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateBufferedReplicationTasks(tx,
		request.NewBufferedReplicationTask,
		request.DeleteBufferedReplicationTask,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		request.UpsertSignalRequestedIDs,
		request.DeleteSignalRequestedID,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if request.ContinueAsNew != nil {
		newDomainID := sqldb.MustParseUUID(request.ContinueAsNew.DomainID)
		newRunID := sqldb.MustParseUUID(request.ContinueAsNew.Execution.GetRunId())
		if err := createOrUpdateCurrentExecution(tx, request.ContinueAsNew, shardID, newDomainID, newRunID); err != nil {
			return err
		}

		if err := createExecution(tx, request.ContinueAsNew, shardID, newDomainID, newRunID, time.Now()); err != nil {
			return err
		}

		if err := createTransferTasks(tx,
			request.ContinueAsNew.TransferTasks,
			shardID,
			newDomainID,
			request.ContinueAsNew.Execution.GetWorkflowId(),
			newRunID); err != nil {
			return err
		}

		if err := createTimerTasks(tx,
			request.ContinueAsNew.TimerTasks,
			nil,
			shardID,
			newDomainID,
			request.ContinueAsNew.Execution.GetWorkflowId(),
			newRunID); err != nil {
			return err
		}
	} else {
		executionInfo := request.ExecutionInfo
		startVersion := common.EmptyVersion
		lastWriteVersion := common.EmptyVersion
		if request.ReplicationState != nil {
			startVersion = request.ReplicationState.StartVersion
			lastWriteVersion = request.ReplicationState.LastWriteVersion
		}
		if request.FinishExecution {
			m.logger.Info("Finish Execution")
			// TODO when finish execution, the current record should be marked with a TTL
		} //else {
		// this is only to update the current record
		if err := continueAsNew(tx,
			m.shardID,
			domainID,
			executionInfo.WorkflowID,
			runID,
			executionInfo.RunID,
			executionInfo.CreateRequestID,
			executionInfo.State,
			executionInfo.CloseStatus,
			startVersion,
			lastWriteVersion); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update current execution. Error: %v", err),
			}
		}
		//}
	}
	return nil
}

func updateBufferedEvents(tx sqldb.Tx, batch *p.DataBlob, clear bool, shardID int, domainID sqldb.UUID,
	workflowID string, runID sqldb.UUID, condition int64, rangeID int64) error {
	if clear {
		if _, err := tx.DeleteFromBufferedEvents(&sqldb.BufferedEventsFilter{
			ShardID:    shardID,
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("updateBufferedEvents delete operation failed. Error: %v", err),
			}
		}
		return nil
	}
	if batch == nil {
		return nil
	}
	row := sqldb.BufferedEventsRow{
		ShardID:      shardID,
		DomainID:     domainID,
		WorkflowID:   workflowID,
		RunID:        runID,
		Data:         batch.Data,
		DataEncoding: string(batch.Encoding),
	}

	if _, err := tx.InsertIntoBufferedEvents([]sqldb.BufferedEventsRow{row}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("updateBufferedEvents operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) ResetWorkflowExecution(request *p.InternalResetWorkflowExecutionRequest) error {
	// TODO need to implement with history eventsV2 API
	return fmt.Errorf("Not implemented")
}

func (m *sqlExecutionManager) ResetMutableState(request *p.InternalResetMutableStateRequest) error {
	return m.txExecuteShardLocked("ResetMutableState", request.RangeID, func(tx sqldb.Tx) error {
		return m.resetMutableStateTx(tx, request)
	})
}

func (m *sqlExecutionManager) resetMutableStateTx(tx sqldb.Tx, request *p.InternalResetMutableStateRequest) error {
	info := request.ExecutionInfo
	replicationState := request.ReplicationState
	domainID := sqldb.MustParseUUID(info.DomainID)
	runID := sqldb.MustParseUUID(info.RunID)

	if err := updateCurrentExecution(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID,
		info.CreateRequestID,
		info.State,
		info.CloseStatus,
		replicationState.StartVersion,
		replicationState.LastWriteVersion,
	); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to continue as new. Error: %v", err),
		}
	}

	// TODO Is there a way to modify the various map tables without fear of other people adding rows after we delete, without locking the executions row?
	if err := lockAndCheckNextEventID(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID,
		request.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetMutableState operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx, info, request.ReplicationState, m.shardID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := deleteActivityInfoMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear activity info map. Error: %v", err),
		}
	}

	if err := updateActivityInfos(tx,
		request.InsertActivityInfos,
		nil,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteTimerInfoMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear timer info map. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		request.InsertTimerInfos,
		nil,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into timer info map after clearing. Error: %v", err),
		}
	}

	if err := deleteChildExecutionInfoMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear child execution info map. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		request.InsertChildExecutionInfos,
		nil,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteRequestCancelInfoMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear request cancel info map. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		request.InsertRequestCancelInfos,
		nil,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into request cancel info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalInfoMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear signal info map. Error: %v", err),
		}
	}

	if err := deleteBufferedReplicationTasksMap(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear buffered replications tasks map. Error: %v", err),
		}
	}

	if err := updateBufferedEvents(tx, nil,
		true,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID, 0, 0); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear buffered events. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		request.InsertSignalInfos,
		nil,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into signal info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalsRequestedSet(tx,
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear signals requested set. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		request.InsertSignalRequestedIDs,
		"",
		m.shardID,
		domainID,
		info.WorkflowID,
		runID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into signals requested set after clearing. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) DeleteWorkflowExecution(request *p.DeleteWorkflowExecutionRequest) error {
	if _, err := m.db.DeleteFromExecutions(&sqldb.ExecutionsFilter{
		ShardID:    m.shardID,
		DomainID:   sqldb.MustParseUUID(request.DomainID),
		WorkflowID: request.WorkflowID,
		RunID:      sqldb.MustParseUUID(request.RunID),
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetCurrentExecution(request *p.GetCurrentExecutionRequest) (*p.GetCurrentExecutionResponse, error) {
	row, err := m.db.SelectFromCurrentExecutions(&sqldb.CurrentExecutionsFilter{
		ShardID:    int64(m.shardID),
		DomainID:   sqldb.MustParseUUID(request.DomainID),
		WorkflowID: request.WorkflowID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{Message: err.Error()}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
		}
	}
	return &p.GetCurrentExecutionResponse{
		StartRequestID: row.CreateRequestID,
		RunID:          row.RunID.String(),
		State:          int(row.State),
		CloseStatus:    int(row.CloseStatus),
	}, nil
}

func (m *sqlExecutionManager) GetTransferTasks(request *p.GetTransferTasksRequest) (*p.GetTransferTasksResponse, error) {
	rows, err := m.db.SelectFromTransferTasks(&sqldb.TransferTasksFilter{
		ShardID: m.shardID, MinTaskID: &request.ReadLevel, MaxTaskID: &request.MaxReadLevel})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetTransferTasks operation failed. Select failed. Error: %v", err),
			}
		}
	}
	resp := &p.GetTransferTasksResponse{Tasks: make([]*p.TransferTaskInfo, len(rows))}
	for i, row := range rows {
		resp.Tasks[i] = &p.TransferTaskInfo{
			DomainID:                row.DomainID.String(),
			WorkflowID:              row.WorkflowID,
			RunID:                   row.RunID.String(),
			VisibilityTimestamp:     row.VisibilityTimestamp,
			TaskID:                  row.TaskID,
			TargetDomainID:          row.TargetDomainID.String(),
			TargetWorkflowID:        row.TargetWorkflowID,
			TargetRunID:             row.TargetRunID.String(),
			TargetChildWorkflowOnly: row.TargetChildWorkflowOnly,
			TaskList:                row.TaskList,
			TaskType:                row.TaskType,
			ScheduleID:              row.ScheduleID,
			Version:                 row.Version,
		}
	}
	return resp, nil
}

func (m *sqlExecutionManager) CompleteTransferTask(request *p.CompleteTransferTaskRequest) error {
	if _, err := m.db.DeleteFromTransferTasks(&sqldb.TransferTasksFilter{
		ShardID: m.shardID,
		TaskID:  &request.TaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteTransferTask(request *p.RangeCompleteTransferTaskRequest) error {
	if _, err := m.db.DeleteFromTransferTasks(&sqldb.TransferTasksFilter{
		ShardID:   m.shardID,
		MinTaskID: &request.ExclusiveBeginTaskID,
		MaxTaskID: &request.InclusiveEndTaskID}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetReplicationTasks(request *p.GetReplicationTasksRequest) (*p.GetReplicationTasksResponse, error) {
	var readLevel int64
	var maxReadLevelInclusive int64
	var err error
	if len(request.NextPageToken) > 0 {
		readLevel, err = deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
	} else {
		readLevel = request.ReadLevel
	}
	maxReadLevelInclusive = collection.MaxInt64(
		readLevel+int64(request.BatchSize), request.MaxReadLevel)

	rows, err := m.db.SelectFromReplicationTasks(&sqldb.ReplicationTasksFilter{
		ShardID:   m.shardID,
		MinTaskID: &readLevel,
		MaxTaskID: &maxReadLevelInclusive,
		PageSize:  &request.BatchSize,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err),
			}
		}
	}
	if len(rows) == 0 {
		return &p.GetReplicationTasksResponse{}, nil
	}

	var tasks = make([]*p.ReplicationTaskInfo, len(rows))
	for i, row := range rows {
		var lastReplicationInfo map[string]*p.ReplicationInfo
		if row.TaskType == p.ReplicationTaskTypeHistory {
			if err := gobDeserialize(row.LastReplicationInfo, &lastReplicationInfo); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("GetReplicationTasks operation failed. Failed to deserialize LastReplicationInfo. Error: %v", err),
				}
			}
		}

		tasks[i] = &p.ReplicationTaskInfo{
			DomainID:            row.DomainID.String(),
			WorkflowID:          row.WorkflowID,
			RunID:               row.RunID.String(),
			TaskID:              row.TaskID,
			TaskType:            row.TaskType,
			FirstEventID:        row.FirstEventID,
			NextEventID:         row.NextEventID,
			Version:             row.Version,
			LastReplicationInfo: lastReplicationInfo,
		}
	}
	var nextPageToken []byte
	lastTaskID := rows[len(rows)-1].TaskID
	if lastTaskID < request.MaxReadLevel {
		nextPageToken = serializePageToken(lastTaskID)
	}
	return &p.GetReplicationTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *sqlExecutionManager) CompleteReplicationTask(request *p.CompleteReplicationTaskRequest) error {
	if _, err := m.db.DeleteFromReplicationTasks(&sqldb.ReplicationTasksFilter{
		ShardID: m.shardID,
		TaskID:  &request.TaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err),
		}
	}
	return nil
}

type timerTaskPageToken struct {
	TaskID    int64
	Timestamp time.Time
}

func (t *timerTaskPageToken) serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *timerTaskPageToken) deserialize(payload []byte) error {
	return json.Unmarshal(payload, t)
}

func (m *sqlExecutionManager) GetTimerIndexTasks(request *p.GetTimerIndexTasksRequest) (*p.GetTimerIndexTasksResponse, error) {
	pageToken := &timerTaskPageToken{TaskID: math.MinInt64, Timestamp: request.MinTimestamp}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("error deserializing timerTaskPageToken: %v", err),
			}
		}
	}

	rows, err := m.db.SelectFromTimerTasks(&sqldb.TimerTasksFilter{
		ShardID:                m.shardID,
		MinVisibilityTimestamp: &pageToken.Timestamp,
		TaskID:                 pageToken.TaskID,
		MaxVisibilityTimestamp: &request.MaxTimestamp,
		PageSize:               common.IntPtr(request.BatchSize + 1),
	})

	if err != nil && err != sql.ErrNoRows {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTimerTasks operation failed. Select failed. Error: %v", err),
		}
	}

	resp := &p.GetTimerIndexTasksResponse{Timers: make([]*p.TimerTaskInfo, len(rows))}
	for i, row := range rows {
		resp.Timers[i] = &p.TimerTaskInfo{
			DomainID:            row.DomainID.String(),
			WorkflowID:          row.WorkflowID,
			RunID:               row.RunID.String(),
			VisibilityTimestamp: row.VisibilityTimestamp,
			TaskID:              row.TaskID,
			TaskType:            row.TaskType,
			TimeoutType:         row.TimeoutType,
			EventID:             row.EventID,
			ScheduleAttempt:     row.ScheduleAttempt,
			Version:             row.Version,
		}
	}

	if len(resp.Timers) > request.BatchSize {
		pageToken = &timerTaskPageToken{
			TaskID:    resp.Timers[request.BatchSize].TaskID,
			Timestamp: resp.Timers[request.BatchSize].VisibilityTimestamp,
		}
		resp.Timers = resp.Timers[:request.BatchSize]
		nextToken, err := pageToken.serialize()
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetTimerTasks: error serializing page token: %v", err),
			}
		}
		resp.NextPageToken = nextToken
	}

	return resp, nil
}

func (m *sqlExecutionManager) CompleteTimerTask(request *p.CompleteTimerTaskRequest) error {
	if _, err := m.db.DeleteFromTimerTasks(&sqldb.TimerTasksFilter{
		ShardID:             m.shardID,
		VisibilityTimestamp: &request.VisibilityTimestamp,
		TaskID:              request.TaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteTimerTask(request *p.RangeCompleteTimerTaskRequest) error {
	start := request.InclusiveBeginTimestamp
	end := request.ExclusiveEndTimestamp
	if _, err := m.db.DeleteFromTimerTasks(&sqldb.TimerTasksFilter{
		ShardID:                m.shardID,
		MinVisibilityTimestamp: &start,
		MaxVisibilityTimestamp: &end,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}
	return nil
}

// NewSQLExecutionStore creates an instance of ExecutionStore
func NewSQLExecutionStore(db sqldb.Interface, logger bark.Logger, shardID int) (p.ExecutionStore, error) {
	return &sqlExecutionManager{
		shardID: shardID,
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

// lockCurrentExecutionIfExists returns current execution or nil if none is found for the workflowID
// locking it in the DB
func lockCurrentExecutionIfExists(tx sqldb.Tx, shardID int, domainID sqldb.UUID, workflowID string) (*sqldb.CurrentExecutionsRow, error) {
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

func createExecution(
	tx sqldb.Tx,
	request *p.CreateWorkflowExecutionRequest,
	shardID int, domainID sqldb.UUID, runID sqldb.UUID, nowTimestamp time.Time) error {
	row := &sqldb.ExecutionsRow{
		ShardID:                      shardID,
		DomainID:                     domainID,
		WorkflowID:                   *request.Execution.WorkflowId,
		RunID:                        runID,
		TaskList:                     request.TaskList,
		WorkflowTypeName:             request.WorkflowTypeName,
		WorkflowTimeoutSeconds:       int64(request.WorkflowTimeout),
		DecisionTaskTimeoutMinutes:   int64(request.DecisionTimeoutValue),
		State:                        p.WorkflowStateCreated,
		CloseStatus:                  p.WorkflowCloseStatusNone,
		LastFirstEventID:             common.FirstEventID,
		NextEventID:                  request.NextEventID,
		LastProcessedEvent:           request.LastProcessedEvent,
		StartTime:                    nowTimestamp,
		LastUpdatedTime:              nowTimestamp,
		CreateRequestID:              request.RequestID,
		DecisionVersion:              int64(request.DecisionVersion),
		DecisionScheduleID:           int64(request.DecisionScheduleID),
		DecisionStartedID:            int64(request.DecisionStartedID),
		DecisionTimeout:              int64(request.DecisionStartToCloseTimeout),
		DecisionAttempt:              0,
		DecisionTimestamp:            0,
		StickyTaskList:               "",
		StickyScheduleToStartTimeout: 0,
		ClientLibraryVersion:         "",
		ClientFeatureVersion:         "",
		ClientImpl:                   "",
		SignalCount:                  int(request.SignalCount),
		CronSchedule:                 request.CronSchedule,
	}

	if request.ReplicationState != nil {
		row.StartVersion = request.ReplicationState.StartVersion
		row.CurrentVersion = request.ReplicationState.CurrentVersion
		row.LastWriteVersion = request.ReplicationState.LastWriteVersion
		row.LastWriteEventID = &request.ReplicationState.LastWriteEventID

		lastReplicationInfo, err := gobSerialize(&request.ReplicationState.LastReplicationInfo)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to serialize LastReplicationInfo. Error: %v", err),
			}
		}
		row.LastReplicationInfo = &lastReplicationInfo
	}

	if request.ParentExecution != nil {
		row.InitiatedID = &request.InitiatedID
		row.ParentDomainID = sqldb.UUIDPtr(sqldb.MustParseUUID(request.ParentDomainID))
		row.ParentWorkflowID = request.ParentExecution.WorkflowId
		row.ParentRunID = sqldb.UUIDPtr(sqldb.MustParseUUID(*request.ParentExecution.RunId))
	}

	_, err := tx.InsertIntoExecutions(row)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to insert into executions table. Error: %v", err),
		}
	}
	return nil
}

func createOrUpdateCurrentExecution(
	tx sqldb.Tx, request *p.CreateWorkflowExecutionRequest, shardID int, domainID sqldb.UUID, runID sqldb.UUID) error {
	row := sqldb.CurrentExecutionsRow{
		ShardID:          int64(shardID),
		DomainID:         domainID,
		WorkflowID:       *request.Execution.WorkflowId,
		RunID:            runID,
		CreateRequestID:  request.RequestID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusNone,
		StartVersion:     common.EmptyVersion,
		LastWriteVersion: common.EmptyVersion,
	}
	replicationState := request.ReplicationState
	if replicationState != nil {
		row.StartVersion = replicationState.StartVersion
		row.LastWriteVersion = replicationState.LastWriteVersion
	}
	if request.ParentExecution != nil {
		row.State = p.WorkflowStateCreated
	}

	switch request.CreateWorkflowMode {
	case p.CreateWorkflowModeContinueAsNew:
		if err := updateCurrentExecution(tx,
			shardID,
			domainID,
			*request.Execution.WorkflowId,
			runID,
			request.RequestID,
			p.WorkflowStateRunning,
			p.WorkflowCloseStatusNone,
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
			*request.Execution.WorkflowId,
			runID,
			request.RequestID,
			p.WorkflowStateRunning,
			p.WorkflowCloseStatusNone,
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
	default:
		return fmt.Errorf("Unknown workflow creation mode: %v", request.CreateWorkflowMode)
	}

	return nil
}

func lockAndCheckNextEventID(tx sqldb.Tx, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID, condition int64) error {
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

func lockNextEventID(tx sqldb.Tx, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) (*int64, error) {
	nextEventID, err := tx.LockExecutions(&sqldb.ExecutionsFilter{ShardID: shardID, DomainID: domainID, WorkflowID: workflowID, RunID: runID})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Failed to lock executions row with (shard, domain, workflow, run) = (%v,%v,%v,%v) which does not exist.", shardID, domainID, workflowID, runID),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock executions row. Error: %v", err),
		}
	}
	result := int64(nextEventID)
	return &result, nil
}

func createTransferTasks(tx sqldb.Tx, transferTasks []p.Task, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) error {
	if len(transferTasks) == 0 {
		return nil
	}
	transferTasksRows := make([]sqldb.TransferTasksRow, len(transferTasks))

	for i, task := range transferTasks {
		transferTasksRows[i].ShardID = shardID
		transferTasksRows[i].DomainID = domainID
		transferTasksRows[i].WorkflowID = workflowID
		transferTasksRows[i].RunID = runID
		transferTasksRows[i].TargetDomainID = domainID
		transferTasksRows[i].TargetWorkflowID = p.TransferTaskTransferTargetWorkflowID
		transferTasksRows[i].TargetChildWorkflowOnly = false
		transferTasksRows[i].TaskList = ""
		transferTasksRows[i].ScheduleID = 0

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			transferTasksRows[i].TargetDomainID = sqldb.MustParseUUID(task.(*p.ActivityTask).DomainID)
			transferTasksRows[i].TaskList = task.(*p.ActivityTask).TaskList
			transferTasksRows[i].ScheduleID = task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			transferTasksRows[i].TargetDomainID = sqldb.MustParseUUID(task.(*p.DecisionTask).DomainID)
			transferTasksRows[i].TaskList = task.(*p.DecisionTask).TaskList
			transferTasksRows[i].ScheduleID = task.(*p.DecisionTask).ScheduleID

		case p.TransferTaskTypeCancelExecution:
			transferTasksRows[i].TargetDomainID = sqldb.MustParseUUID(task.(*p.CancelExecutionTask).TargetDomainID)
			transferTasksRows[i].TargetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			if task.(*p.CancelExecutionTask).TargetRunID != "" {
				transferTasksRows[i].TargetRunID = sqldb.MustParseUUID(task.(*p.CancelExecutionTask).TargetRunID)
			}
			transferTasksRows[i].TargetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			transferTasksRows[i].ScheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			transferTasksRows[i].TargetDomainID = sqldb.MustParseUUID(task.(*p.SignalExecutionTask).TargetDomainID)
			transferTasksRows[i].TargetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			if task.(*p.SignalExecutionTask).TargetRunID != "" {
				transferTasksRows[i].TargetRunID = sqldb.MustParseUUID(task.(*p.SignalExecutionTask).TargetRunID)
			}
			transferTasksRows[i].TargetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			transferTasksRows[i].ScheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			transferTasksRows[i].TargetDomainID = sqldb.MustParseUUID(task.(*p.StartChildExecutionTask).TargetDomainID)
			transferTasksRows[i].TargetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			transferTasksRows[i].ScheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case p.TransferTaskTypeCloseExecution:
			// No explicit property needs to be set

		default:
			// hmm what should i do here?
			//d.logger.Fatal("Unknown Transfer Task.")
		}

		transferTasksRows[i].TaskID = task.GetTaskID()
		transferTasksRows[i].TaskType = task.GetType()
		transferTasksRows[i].Version = task.GetVersion()
		transferTasksRows[i].VisibilityTimestamp = task.GetVisibilityTimestamp()
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
	tx sqldb.Tx, replicationTasks []p.Task, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) error {
	if len(replicationTasks) == 0 {
		return nil
	}
	replicationTasksRows := make([]sqldb.ReplicationTasksRow, len(replicationTasks))

	for i, task := range replicationTasks {
		replicationTasksRows[i].DomainID = domainID
		replicationTasksRows[i].WorkflowID = workflowID
		replicationTasksRows[i].RunID = runID
		replicationTasksRows[i].ShardID = shardID
		replicationTasksRows[i].TaskType = task.GetType()
		replicationTasksRows[i].TaskID = task.GetTaskID()

		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		activityScheduleID := common.EmptyEventID
		var lastReplicationInfo []byte
		var err error

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
			lastReplicationInfo, err = gobSerialize(historyReplicationTask.LastReplicationInfo)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to serialize LastReplicationInfo. Task: %v", task),
				}
			}

		case p.ReplicationTaskTypeSyncActivity:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			lastReplicationInfo = []byte{}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknown replication task: %v", task),
			}
		}

		replicationTasksRows[i].FirstEventID = firstEventID
		replicationTasksRows[i].NextEventID = nextEventID
		replicationTasksRows[i].Version = version
		replicationTasksRows[i].LastReplicationInfo = lastReplicationInfo
		replicationTasksRows[i].ScheduledID = activityScheduleID
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
	tx sqldb.Tx, timerTasks []p.Task, deleteTimerTask p.Task, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) error {
	if len(timerTasks) > 0 {
		timerTasksRows := make([]sqldb.TimerTasksRow, len(timerTasks))

		for i, task := range timerTasks {
			switch t := task.(type) {
			case *p.DecisionTimeoutTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].TimeoutType = t.TimeoutType
				timerTasksRows[i].ScheduleAttempt = t.ScheduleAttempt
			case *p.ActivityTimeoutTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].TimeoutType = t.TimeoutType
				timerTasksRows[i].ScheduleAttempt = t.Attempt
			case *p.UserTimerTask:
				timerTasksRows[i].EventID = t.EventID
			case *p.ActivityRetryTimerTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].ScheduleAttempt = int64(t.Attempt)
			case *p.WorkflowBackoffTimerTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].TimeoutType = t.TimeoutType
			}

			timerTasksRows[i].ShardID = shardID
			timerTasksRows[i].DomainID = domainID
			timerTasksRows[i].WorkflowID = workflowID
			timerTasksRows[i].RunID = runID
			timerTasksRows[i].VisibilityTimestamp = task.GetVisibilityTimestamp()
			timerTasksRows[i].TaskID = task.GetTaskID()
			timerTasksRows[i].Version = task.GetVersion()
			timerTasksRows[i].TaskType = task.GetType()
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

	if deleteTimerTask != nil {
		ts := deleteTimerTask.GetVisibilityTimestamp()
		_, err := tx.DeleteFromTimerTasks(&sqldb.TimerTasksFilter{ShardID: shardID, VisibilityTimestamp: &ts, TaskID: deleteTimerTask.GetTaskID()})
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to delete timer task. Task: %v. Error: %v", deleteTimerTask, err),
			}
		}
	}

	return nil
}

func continueAsNew(tx sqldb.Tx, shardID int, domainID sqldb.UUID, workflowID string, newRunID sqldb.UUID, previousRunID string,
	createRequestID string, state int, closeStatus int, startVersion int64, lastWriteVersion int64) error {
	runID, err := tx.LockCurrentExecutions(&sqldb.CurrentExecutionsFilter{
		ShardID: int64(shardID), DomainID: domainID, WorkflowID: workflowID})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to check current run ID. Error: %v", err),
		}
	}
	if runID.String() != previousRunID {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("ContinueAsNew failed. Current run ID was %v, expected %v", runID, previousRunID),
		}
	}
	return updateCurrentExecution(tx, shardID, domainID, workflowID, newRunID, createRequestID, state, closeStatus, startVersion, lastWriteVersion)
}

func updateCurrentExecution(tx sqldb.Tx, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID,
	createRequestID string, state int, closeStatus int, startVersion int64, lastWriteVersion int64) error {
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

func updateExecution(tx sqldb.Tx,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	shardID int) error {
	row := &sqldb.ExecutionsRow{
		DomainID:                     sqldb.MustParseUUID(executionInfo.DomainID),
		WorkflowID:                   executionInfo.WorkflowID,
		RunID:                        sqldb.MustParseUUID(executionInfo.RunID),
		TaskList:                     executionInfo.TaskList,
		WorkflowTypeName:             executionInfo.WorkflowTypeName,
		WorkflowTimeoutSeconds:       int64(executionInfo.WorkflowTimeout),
		DecisionTaskTimeoutMinutes:   int64(executionInfo.DecisionTimeoutValue),
		State:                        int64(executionInfo.State),
		CloseStatus:                  int64(executionInfo.CloseStatus),
		LastFirstEventID:             int64(executionInfo.LastFirstEventID),
		NextEventID:                  int64(executionInfo.NextEventID),
		LastProcessedEvent:           int64(executionInfo.LastProcessedEvent),
		StartTime:                    executionInfo.StartTimestamp,
		LastUpdatedTime:              executionInfo.LastUpdatedTimestamp,
		CreateRequestID:              executionInfo.CreateRequestID,
		DecisionVersion:              executionInfo.DecisionVersion,
		DecisionScheduleID:           executionInfo.DecisionScheduleID,
		DecisionStartedID:            executionInfo.DecisionStartedID,
		DecisionRequestID:            executionInfo.DecisionRequestID,
		DecisionTimeout:              int64(executionInfo.DecisionTimeout),
		DecisionAttempt:              executionInfo.DecisionAttempt,
		DecisionTimestamp:            executionInfo.DecisionTimestamp,
		StickyTaskList:               executionInfo.StickyTaskList,
		StickyScheduleToStartTimeout: int64(executionInfo.StickyScheduleToStartTimeout),
		ClientLibraryVersion:         executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:         executionInfo.ClientFeatureVersion,
		ClientImpl:                   executionInfo.ClientImpl,
		ShardID:                      shardID,
		LastWriteVersion:             common.EmptyVersion,
		CurrentVersion:               common.EmptyVersion,
		SignalCount:                  int(executionInfo.SignalCount),
		CronSchedule:                 executionInfo.CronSchedule,
		CompletionEventBatchID:       &executionInfo.CompletionEventBatchID,
	}

	if executionInfo.ExecutionContext != nil {
		row.ExecutionContext = &executionInfo.ExecutionContext
	}

	completionEvent := executionInfo.CompletionEvent
	if completionEvent != nil {
		row.CompletionEvent = &completionEvent.Data
		row.CompletionEventEncoding = common.StringPtr(string(completionEvent.Encoding))
	}
	if replicationState != nil {
		row.StartVersion = replicationState.StartVersion
		row.CurrentVersion = replicationState.CurrentVersion
		row.LastWriteVersion = replicationState.LastWriteVersion
		row.LastWriteEventID = &replicationState.LastWriteEventID
		lastReplicationInfo, err := gobSerialize(&replicationState.LastReplicationInfo)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to serialize LastReplicationInfo. Error: %v", err),
			}
		}
		row.LastReplicationInfo = &lastReplicationInfo
	}

	if executionInfo.ParentDomainID != "" {
		row.ParentDomainID = sqldb.UUIDPtr(sqldb.MustParseUUID(executionInfo.ParentDomainID))
		row.ParentWorkflowID = &executionInfo.ParentWorkflowID
		row.ParentRunID = sqldb.UUIDPtr(sqldb.MustParseUUID(executionInfo.ParentRunID))
		row.InitiatedID = &executionInfo.InitiatedID
		row.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		var i int64 = 1
		row.CancelRequested = &i
		row.CancelRequestID = &executionInfo.CancelRequestID
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
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Affected %v rows updated instead of 1.", rowsAffected),
		}
	}

	return nil
}
