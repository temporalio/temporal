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
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
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

	if err := createExecutionFromRequest(tx, request, m.shardID, domainID, runID, time.Now()); err != nil {
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

	info, err := workflowExecutionInfoFromBlob(execution.Data, execution.DataEncoding)
	if err != nil {
		return nil, err
	}

	var state p.InternalWorkflowMutableState
	state.ExecutionInfo = &p.InternalWorkflowExecutionInfo{
		DomainID:                     execution.DomainID.String(),
		WorkflowID:                   execution.WorkflowID,
		RunID:                        execution.RunID.String(),
		NextEventID:                  execution.NextEventID,
		TaskList:                     info.GetTaskList(),
		WorkflowTypeName:             info.GetWorkflowTypeName(),
		WorkflowTimeout:              info.GetWorkflowTimeoutSeconds(),
		DecisionTimeoutValue:         info.GetDecisionTaskTimeoutSeconds(),
		State:                        int(info.GetState()),
		CloseStatus:                  int(info.GetCloseStatus()),
		LastFirstEventID:             info.GetLastFirstEventID(),
		LastProcessedEvent:           info.GetLastProcessedEvent(),
		StartTimestamp:               time.Unix(0, info.GetStartTimeNanos()),
		LastUpdatedTimestamp:         time.Unix(0, info.GetLastUpdatedTimeNanos()),
		CreateRequestID:              info.GetCreateRequestID(),
		DecisionVersion:              info.GetDecisionVersion(),
		DecisionScheduleID:           info.GetDecisionScheduleID(),
		DecisionStartedID:            info.GetDecisionStartedID(),
		DecisionRequestID:            info.GetDecisionRequestID(),
		DecisionTimeout:              info.GetDecisionTimeout(),
		DecisionAttempt:              info.GetDecisionAttempt(),
		DecisionTimestamp:            info.GetDecisionTimestampNanos(),
		StickyTaskList:               info.GetStickyTaskList(),
		StickyScheduleToStartTimeout: int32(info.GetStickyScheduleToStartTimeout()),
		ClientLibraryVersion:         info.GetClientLibraryVersion(),
		ClientFeatureVersion:         info.GetClientFeatureVersion(),
		ClientImpl:                   info.GetClientImpl(),
		SignalCount:                  int32(info.GetSignalCount()),
		HistorySize:                  info.GetHistorySize(),
		CronSchedule:                 info.GetCronSchedule(),
		CompletionEventBatchID:       common.EmptyEventID,
		HasRetryPolicy:               info.GetHasRetryPolicy(),
		Attempt:                      int32(info.GetRetryAttempt()),
		InitialInterval:              info.GetRetryInitialIntervalSeconds(),
		BackoffCoefficient:           info.GetRetryBackoffCoefficient(),
		MaximumInterval:              info.GetRetryMaximumIntervalSeconds(),
		MaximumAttempts:              info.GetRetryMaximumAttempts(),
		ExpirationSeconds:            info.GetRetryExpirationSeconds(),
		ExpirationTime:               time.Unix(0, info.GetRetryExpirationTimeNanos()),
		EventStoreVersion:            info.GetEventStoreVersion(),
		BranchToken:                  info.GetEventBranchToken(),
		ExecutionContext:             info.GetExecutionContext(),
		NonRetriableErrors:           info.GetRetryNonRetryableErrors(),
	}

	if info.LastWriteEventID != nil {
		state.ReplicationState = &p.ReplicationState{}
		state.ReplicationState.StartVersion = info.GetStartVersion()
		state.ReplicationState.CurrentVersion = info.GetCurrentVersion()
		state.ReplicationState.LastWriteVersion = execution.LastWriteVersion
		state.ReplicationState.LastWriteEventID = info.GetLastWriteEventID()
		state.ReplicationState.LastReplicationInfo = make(map[string]*p.ReplicationInfo, len(info.LastReplicationInfo))
		for k, v := range info.LastReplicationInfo {
			state.ReplicationState.LastReplicationInfo[k] = &p.ReplicationInfo{Version: v.GetVersion(), LastEventID: v.GetLastEventID()}
		}
	}

	if info.ParentDomainID != nil {
		state.ExecutionInfo.ParentDomainID = sqldb.UUID(info.ParentDomainID).String()
		state.ExecutionInfo.ParentWorkflowID = info.GetParentWorkflowID()
		state.ExecutionInfo.ParentRunID = sqldb.UUID(info.ParentRunID).String()
		state.ExecutionInfo.InitiatedID = info.GetInitiatedID()
		if state.ExecutionInfo.CompletionEvent != nil {
			state.ExecutionInfo.CompletionEvent = nil
		}
	}

	if info.GetCancelRequested() {
		state.ExecutionInfo.CancelRequested = true
		state.ExecutionInfo.CancelRequestID = info.GetCancelRequestID()
	}

	if info.CompletionEventBatchID != nil {
		state.ExecutionInfo.CompletionEventBatchID = info.GetCompletionEventBatchID()
	}

	if info.CompletionEvent != nil {
		state.ExecutionInfo.CompletionEvent = p.NewDataBlob(info.CompletionEvent,
			common.EncodingType(info.GetCompletionEventEncoding()))
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

		if err := createExecutionFromRequest(tx, request.ContinueAsNew, shardID, newDomainID, newRunID, time.Now()); err != nil {
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
	return m.txExecuteShardLocked("ResetWorkflowExecution", request.RangeID, func(tx sqldb.Tx) error {
		currExecutionInfo := request.CurrExecutionInfo
		currReplicationState := request.CurrReplicationState
		domainID := sqldb.MustParseUUID(currExecutionInfo.DomainID)
		workflowID := currExecutionInfo.WorkflowID
		shardID := m.shardID
		currRunID := sqldb.MustParseUUID(currExecutionInfo.RunID)

		insertExecutionInfo := request.InsertExecutionInfo
		insertReplicationState := request.InsertReplicationState
		newRunID := sqldb.MustParseUUID(insertExecutionInfo.RunID)

		startVersion := common.EmptyVersion
		lastWriteVersion := common.EmptyVersion
		if insertReplicationState != nil {
			startVersion = insertReplicationState.StartVersion
			lastWriteVersion = insertReplicationState.LastWriteVersion
		}

		// 1. update current execution with checking prevRunState and prevRunVersion
		currRow, err := lockCurrentExecutionIfExists(tx, shardID, domainID, workflowID)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed at lockCurrentExecutionIfExists. Error: %v", err),
			}
		}
		if currRow == nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. current_executions row doesn't exist."),
			}
		}
		if currRow.RunID.String() != currRunID.String() {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. runID in current_executions row doesn't match"),
			}
		}
		if currReplicationState != nil {
			// only check when with replication
			if currRow.LastWriteVersion != request.PrevRunVersion || currRow.State != request.PrevRunState {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. current_executions row last state/version doesn't match"),
				}
			}
		}

		if err := updateCurrentExecution(tx,
			shardID,
			domainID,
			insertExecutionInfo.WorkflowID,
			sqldb.MustParseUUID(insertExecutionInfo.RunID),
			insertExecutionInfo.CreateRequestID,
			insertExecutionInfo.State,
			insertExecutionInfo.CloseStatus,
			startVersion,
			lastWriteVersion,
		); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed at updateCurrentExecution. Error: %v", err),
			}
		}

		// 2. lock base run: we want to grab a read-lock for base run to prevent race condition
		// It is only needed when base run is not current run. Because we will obtain a lock on current run anyway.
		if request.BaseRunID != currExecutionInfo.RunID {
			filter := &sqldb.ExecutionsFilter{ShardID: shardID, DomainID: domainID, WorkflowID: workflowID, RunID: sqldb.MustParseUUID(request.BaseRunID)}
			_, err := tx.ReadLockExecutions(filter)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed at ReadLockExecutions. Error: %v", err),
				}
			}
		}

		// 3. update or lock current run
		if request.UpdateCurr {
			if err := createTransferTasks(tx, request.CurrTransferTasks, shardID, domainID, workflowID, currRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
				}
			}
			if err := createTimerTasks(tx, request.CurrTimerTasks, nil, shardID, domainID, workflowID, currRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
				}
			}

			if err := lockAndCheckNextEventID(tx, shardID, domainID, workflowID, currRunID, request.Condition); err != nil {
				switch err.(type) {
				case *p.ConditionFailedError:
					return err
				default:
					return &workflow.InternalServiceError{
						Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
					}
				}
			}

			if err := updateExecution(tx, currExecutionInfo, currReplicationState, shardID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
				}
			}
		} else {
			// even the current run is not running, we need to lock the current run:
			// 1). in case it is changed by conflict resolution
			// 2). in case delete history timer kicks in if the base is current
			if err := lockAndCheckNextEventID(tx, shardID, domainID, workflowID, currRunID, request.Condition); err != nil {
				switch err.(type) {
				case *p.ConditionFailedError:
					return err
				default:
					return &workflow.InternalServiceError{
						Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
					}
				}
			}
		}

		if err := createReplicationTasks(tx, request.CurrReplicationTasks, shardID, domainID, workflowID, currRunID); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
			}
		}

		// 4. insert records for new run
		if err := createReplicationTasks(tx, request.InsertReplicationTasks, shardID, domainID, workflowID, newRunID); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
			}
		}
		if err := createExecution(tx, insertExecutionInfo, insertReplicationState, shardID); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create executions row. Erorr: %v", err),
			}
		}

		if len(request.InsertActivityInfos) > 0 {
			if err := updateActivityInfos(tx,
				request.InsertActivityInfos,
				nil,
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into activity info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertTimerInfos) > 0 {
			if err := updateTimerInfos(tx,
				request.InsertTimerInfos,
				nil,
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into timer info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertRequestCancelInfos) > 0 {
			if err := updateRequestCancelInfos(tx,
				request.InsertRequestCancelInfos,
				nil,
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into request cancel info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertChildExecutionInfos) > 0 {
			if err := updateChildExecutionInfos(tx,
				request.InsertChildExecutionInfos,
				nil,
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into child execution info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertSignalInfos) > 0 {
			if err := updateSignalInfos(tx,
				request.InsertSignalInfos,
				nil,
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into signal info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertSignalRequestedIDs) > 0 {
			if err := updateSignalsRequested(tx,
				request.InsertSignalRequestedIDs,
				"",
				m.shardID,
				domainID,
				workflowID,
				newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to insert into signal requested ID info map. Error: %v", err),
				}
			}
		}

		if len(request.InsertTimerTasks) > 0 {
			if err := createTimerTasks(tx, request.InsertTimerTasks, nil, shardID, domainID, workflowID, newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
				}
			}
		}

		if len(request.InsertTransferTasks) > 0 {
			if err := createTransferTasks(tx, request.InsertTransferTasks, shardID, domainID, workflowID, newRunID); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
				}
			}
		}

		return nil
	})
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
	domainID := sqldb.MustParseUUID(request.DomainID)
	runID := sqldb.MustParseUUID(request.RunID)
	return m.txExecute("deleteWorkflowExecution", func(tx sqldb.Tx) error {
		if _, err := tx.DeleteFromExecutions(&sqldb.ExecutionsFilter{
			ShardID:    m.shardID,
			DomainID:   domainID,
			WorkflowID: request.WorkflowID,
			RunID:      runID,
		}); err != nil {
			return err
		}
		// its possible for a new run of the same workflow to have started after the run we are deleting
		// here was finished. In that case, current_executions table will have the same workflowID but different
		// runID. The following code will delete the row from current_executions if and only if the runID is
		// same as the one we are trying to delete here
		_, err := tx.DeleteFromCurrentExecutions(&sqldb.CurrentExecutionsFilter{
			ShardID:    int64(m.shardID),
			DomainID:   domainID,
			WorkflowID: request.WorkflowID,
			RunID:      runID,
		})
		return err
	})
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
		StartRequestID:   row.CreateRequestID,
		RunID:            row.RunID.String(),
		State:            int(row.State),
		CloseStatus:      int(row.CloseStatus),
		LastWriteVersion: row.LastWriteVersion,
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
		info, err := transferTaskInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		resp.Tasks[i] = &p.TransferTaskInfo{
			TaskID:                  row.TaskID,
			DomainID:                sqldb.UUID(info.DomainID).String(),
			WorkflowID:              info.GetWorkflowID(),
			RunID:                   sqldb.UUID(info.RunID).String(),
			VisibilityTimestamp:     time.Unix(0, info.GetVisibilityTimestampNanos()),
			TargetDomainID:          sqldb.UUID(info.TargetDomainID).String(),
			TargetWorkflowID:        info.GetTargetWorkflowID(),
			TargetRunID:             sqldb.UUID(info.TargetRunID).String(),
			TargetChildWorkflowOnly: info.GetTargetChildWorkflowOnly(),
			TaskList:                info.GetTaskList(),
			TaskType:                int(info.GetTaskType()),
			ScheduleID:              info.GetScheduleID(),
			Version:                 info.GetVersion(),
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
		info, err := replicationTaskInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}

		var lastReplicationInfo map[string]*p.ReplicationInfo
		if info.GetTaskType() == p.ReplicationTaskTypeHistory {
			lastReplicationInfo = make(map[string]*p.ReplicationInfo, len(info.LastReplicationInfo))
			for k, v := range info.LastReplicationInfo {
				lastReplicationInfo[k] = &p.ReplicationInfo{Version: v.GetVersion(), LastEventID: v.GetLastEventID()}
			}
		}

		tasks[i] = &p.ReplicationTaskInfo{
			TaskID:                  row.TaskID,
			DomainID:                sqldb.UUID(info.DomainID).String(),
			WorkflowID:              info.GetWorkflowID(),
			RunID:                   sqldb.UUID(info.RunID).String(),
			TaskType:                int(info.GetTaskType()),
			FirstEventID:            info.GetFirstEventID(),
			NextEventID:             info.GetNextEventID(),
			Version:                 info.GetVersion(),
			LastReplicationInfo:     lastReplicationInfo,
			ScheduledID:             info.GetScheduledID(),
			EventStoreVersion:       info.GetEventStoreVersion(),
			NewRunEventStoreVersion: info.GetNewRunEventStoreVersion(),
			BranchToken:             info.GetBranchToken(),
			NewRunBranchToken:       info.GetNewRunBranchToken(),
			ResetWorkflow:           info.GetResetWorkflow(),
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
		info, err := timerTaskInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		resp.Timers[i] = &p.TimerTaskInfo{
			VisibilityTimestamp: row.VisibilityTimestamp,
			TaskID:              row.TaskID,
			DomainID:            sqldb.UUID(info.DomainID).String(),
			WorkflowID:          info.GetWorkflowID(),
			RunID:               sqldb.UUID(info.RunID).String(),
			TaskType:            int(info.GetTaskType()),
			TimeoutType:         int(info.GetTimeoutType()),
			EventID:             info.GetEventID(),
			ScheduleAttempt:     info.GetScheduleAttempt(),
			Version:             info.GetVersion(),
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
func NewSQLExecutionStore(db sqldb.Interface, logger log.Logger, shardID int) (p.ExecutionStore, error) {
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

func createExecutionFromRequest(
	tx sqldb.Tx,
	request *p.CreateWorkflowExecutionRequest,
	shardID int, domainID sqldb.UUID, runID sqldb.UUID, nowTimestamp time.Time) error {
	emptyStr := ""
	zeroPtr := common.Int64Ptr(0)
	lastWriteVersion := common.EmptyVersion
	info := &sqlblobs.WorkflowExecutionInfo{
		TaskList:                     &request.TaskList,
		WorkflowTypeName:             &request.WorkflowTypeName,
		WorkflowTimeoutSeconds:       &request.WorkflowTimeout,
		DecisionTaskTimeoutSeconds:   &request.DecisionTimeoutValue,
		State:                        common.Int32Ptr(int32(p.WorkflowStateCreated)),
		CloseStatus:                  common.Int32Ptr(int32(p.WorkflowCloseStatusNone)),
		LastFirstEventID:             common.Int64Ptr(common.FirstEventID),
		LastEventTaskID:              &request.LastEventTaskID,
		LastProcessedEvent:           &request.LastProcessedEvent,
		StartTimeNanos:               common.Int64Ptr(nowTimestamp.UnixNano()),
		LastUpdatedTimeNanos:         common.Int64Ptr(nowTimestamp.UnixNano()),
		CreateRequestID:              &request.RequestID,
		DecisionVersion:              &request.DecisionVersion,
		DecisionScheduleID:           &request.DecisionScheduleID,
		DecisionStartedID:            &request.DecisionStartedID,
		DecisionTimeout:              &request.DecisionStartToCloseTimeout,
		DecisionAttempt:              zeroPtr,
		DecisionTimestampNanos:       zeroPtr,
		StickyTaskList:               &emptyStr,
		StickyScheduleToStartTimeout: zeroPtr,
		ClientLibraryVersion:         &emptyStr,
		ClientFeatureVersion:         &emptyStr,
		ClientImpl:                   &emptyStr,
		SignalCount:                  common.Int64Ptr(int64(request.SignalCount)),
		HistorySize:                  &request.HistorySize,
		CronSchedule:                 &request.CronSchedule,
		HasRetryPolicy:               common.BoolPtr(request.HasRetryPolicy),
		RetryAttempt:                 common.Int64Ptr(int64(request.Attempt)),
		RetryInitialIntervalSeconds:  &request.InitialInterval,
		RetryBackoffCoefficient:      &request.BackoffCoefficient,
		RetryMaximumIntervalSeconds:  &request.MaximumInterval,
		RetryMaximumAttempts:         &request.MaximumAttempts,
		RetryExpirationSeconds:       &request.ExpirationSeconds,
		RetryExpirationTimeNanos:     common.Int64Ptr(request.ExpirationTime.UnixNano()),
		RetryNonRetryableErrors:      request.NonRetriableErrors,
		ExecutionContext:             request.ExecutionContext,
	}
	if request.ReplicationState != nil {
		lastWriteVersion = request.ReplicationState.LastWriteVersion
		info.StartVersion = &request.ReplicationState.StartVersion
		info.CurrentVersion = &request.ReplicationState.CurrentVersion
		info.LastWriteEventID = &request.ReplicationState.LastWriteEventID
		info.LastReplicationInfo = make(map[string]*sqlblobs.ReplicationInfo, len(request.ReplicationState.LastReplicationInfo))
		for k, v := range request.ReplicationState.LastReplicationInfo {
			info.LastReplicationInfo[k] = &sqlblobs.ReplicationInfo{Version: &v.Version, LastEventID: &v.LastEventID}
		}
	}
	if request.ParentExecution != nil {
		info.InitiatedID = &request.InitiatedID
		info.ParentDomainID = sqldb.MustParseUUID(request.ParentDomainID)
		info.ParentWorkflowID = request.ParentExecution.WorkflowId
		info.ParentRunID = sqldb.MustParseUUID(*request.ParentExecution.RunId)
	}
	if request.EventStoreVersion == p.EventStoreVersionV2 {
		info.EventStoreVersion = common.Int32Ptr(int32(p.EventStoreVersionV2))
		info.EventBranchToken = request.BranchToken
	}

	blob, err := workflowExecutionInfoToBlob(info)
	if err != nil {
		return err
	}
	row := &sqldb.ExecutionsRow{
		ShardID:          shardID,
		DomainID:         domainID,
		WorkflowID:       *request.Execution.WorkflowId,
		RunID:            runID,
		NextEventID:      request.NextEventID,
		LastWriteVersion: lastWriteVersion,
		Data:             blob.Data,
		DataEncoding:     string(blob.Encoding),
	}
	_, err = tx.InsertIntoExecutions(row)
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
	nextEventID, err := tx.WriteLockExecutions(&sqldb.ExecutionsFilter{ShardID: shardID, DomainID: domainID, WorkflowID: workflowID, RunID: runID})
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

		case p.TransferTaskTypeCloseExecution:
			// No explicit property needs to be set

		case p.TransferTaskTypeRecordWorkflowStarted:
			// No explicit property needs to be set

		default:
			// hmm what should i do here?
			//d.logger.Fatal("Unknown Transfer Task.")
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
	tx sqldb.Tx, replicationTasks []p.Task, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) error {
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

		var eventStoreVersion, newRunEventStoreVersion int32
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
			eventStoreVersion = historyReplicationTask.EventStoreVersion
			newRunEventStoreVersion = historyReplicationTask.NewRunEventStoreVersion
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
				Message: fmt.Sprintf("Unknown replication task: %v", task),
			}
		}

		blob, err := replicationTaskInfoToBlob(&sqlblobs.ReplicationTaskInfo{
			DomainID:                domainID,
			WorkflowID:              &workflowID,
			RunID:                   runID,
			TaskType:                common.Int16Ptr(int16(task.GetType())),
			FirstEventID:            &firstEventID,
			NextEventID:             &nextEventID,
			Version:                 &version,
			LastReplicationInfo:     lastReplicationInfo,
			ScheduledID:             &activityScheduleID,
			EventStoreVersion:       &eventStoreVersion,
			NewRunEventStoreVersion: &newRunEventStoreVersion,
			BranchToken:             branchToken,
			NewRunBranchToken:       newRunBranchToken,
			ResetWorkflow:           &resetWorkflow,
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
	tx sqldb.Tx, timerTasks []p.Task, deleteTimerTask p.Task, shardID int, domainID sqldb.UUID, workflowID string, runID sqldb.UUID) error {
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

func buildExecutionRow(executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	shardID int) (row *sqldb.ExecutionsRow, err error) {
	lastWriteVersion := common.EmptyVersion
	info := &sqlblobs.WorkflowExecutionInfo{
		TaskList:                     &executionInfo.TaskList,
		WorkflowTypeName:             &executionInfo.WorkflowTypeName,
		WorkflowTimeoutSeconds:       &executionInfo.WorkflowTimeout,
		DecisionTaskTimeoutSeconds:   &executionInfo.DecisionTimeoutValue,
		ExecutionContext:             executionInfo.ExecutionContext,
		State:                        common.Int32Ptr(int32(executionInfo.State)),
		CloseStatus:                  common.Int32Ptr(int32(executionInfo.CloseStatus)),
		LastFirstEventID:             &executionInfo.LastFirstEventID,
		LastProcessedEvent:           &executionInfo.LastProcessedEvent,
		StartTimeNanos:               common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
		LastUpdatedTimeNanos:         common.TimeNowNanosPtr(),
		CreateRequestID:              &executionInfo.CreateRequestID,
		DecisionVersion:              &executionInfo.DecisionVersion,
		DecisionScheduleID:           &executionInfo.DecisionScheduleID,
		DecisionStartedID:            &executionInfo.DecisionStartedID,
		DecisionRequestID:            &executionInfo.DecisionRequestID,
		DecisionTimeout:              &executionInfo.DecisionTimeout,
		DecisionAttempt:              &executionInfo.DecisionAttempt,
		DecisionTimestampNanos:       &executionInfo.DecisionTimestamp,
		StickyTaskList:               &executionInfo.StickyTaskList,
		StickyScheduleToStartTimeout: common.Int64Ptr(int64(executionInfo.StickyScheduleToStartTimeout)),
		ClientLibraryVersion:         &executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:         &executionInfo.ClientFeatureVersion,
		ClientImpl:                   &executionInfo.ClientImpl,
		CurrentVersion:               common.Int64Ptr(common.EmptyVersion),
		SignalCount:                  common.Int64Ptr(int64(executionInfo.SignalCount)),
		HistorySize:                  &executionInfo.HistorySize,
		CronSchedule:                 &executionInfo.CronSchedule,
		CompletionEventBatchID:       &executionInfo.CompletionEventBatchID,
		HasRetryPolicy:               &executionInfo.HasRetryPolicy,
		RetryAttempt:                 common.Int64Ptr(int64(executionInfo.Attempt)),
		RetryInitialIntervalSeconds:  &executionInfo.InitialInterval,
		RetryBackoffCoefficient:      &executionInfo.BackoffCoefficient,
		RetryMaximumIntervalSeconds:  &executionInfo.MaximumInterval,
		RetryMaximumAttempts:         &executionInfo.MaximumAttempts,
		RetryExpirationSeconds:       &executionInfo.ExpirationSeconds,
		RetryExpirationTimeNanos:     common.Int64Ptr(executionInfo.ExpirationTime.UnixNano()),
		RetryNonRetryableErrors:      executionInfo.NonRetriableErrors,
		EventStoreVersion:            &executionInfo.EventStoreVersion,
		EventBranchToken:             executionInfo.BranchToken,
	}

	completionEvent := executionInfo.CompletionEvent
	if completionEvent != nil {
		info.CompletionEvent = completionEvent.Data
		info.CompletionEventEncoding = common.StringPtr(string(completionEvent.Encoding))
	}
	if replicationState != nil {
		lastWriteVersion = replicationState.LastWriteVersion
		info.StartVersion = &replicationState.StartVersion
		info.CurrentVersion = &replicationState.CurrentVersion
		info.LastWriteEventID = &replicationState.LastWriteEventID
		info.LastReplicationInfo = make(map[string]*sqlblobs.ReplicationInfo, len(replicationState.LastReplicationInfo))
		for k, v := range replicationState.LastReplicationInfo {
			info.LastReplicationInfo[k] = &sqlblobs.ReplicationInfo{Version: &v.Version, LastEventID: &v.LastEventID}
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

func updateExecution(tx sqldb.Tx,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	shardID int) error {
	row, err := buildExecutionRow(executionInfo, replicationState, shardID)
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

func createExecution(tx sqldb.Tx,
	executionInfo *p.InternalWorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	shardID int) error {
	row, err := buildExecutionRow(executionInfo, replicationState, shardID)
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
