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
	"bytes"
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
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

type sqlExecutionManager struct {
	sqlStore
	shardID int
}

var _ p.ExecutionStore = (*sqlExecutionManager)(nil)

// NewSQLExecutionStore creates an instance of ExecutionStore
func NewSQLExecutionStore(
	db sqlplugin.DB,
	logger log.Logger,
	shardID int,
) (p.ExecutionStore, error) {

	return &sqlExecutionManager{
		shardID: shardID,
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

// txExecuteShardLocked executes f under transaction and with read lock on shard row
func (m *sqlExecutionManager) txExecuteShardLocked(
	operation string,
	rangeID int64,
	fn func(tx sqlplugin.Tx) error,
) error {

	return m.txExecute(operation, func(tx sqlplugin.Tx) error {
		if err := readLockShard(tx, m.shardID, rangeID); err != nil {
			return err
		}
		err := fn(tx)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *sqlExecutionManager) GetShardID() int {
	return m.shardID
}

func (m *sqlExecutionManager) CreateWorkflowExecution(
	request *p.InternalCreateWorkflowExecutionRequest,
) (response *p.CreateWorkflowExecutionResponse, err error) {

	err = m.txExecuteShardLocked("CreateWorkflowExecution", request.RangeID, func(tx sqlplugin.Tx) error {
		response, err = m.createWorkflowExecutionTx(tx, request)
		return err
	})
	return
}

func (m *sqlExecutionManager) createWorkflowExecutionTx(
	tx sqlplugin.Tx,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.CreateWorkflowExecutionResponse, error) {

	newWorkflow := request.NewWorkflowSnapshot
	executionInfo := newWorkflow.ExecutionInfo
	startVersion := newWorkflow.StartVersion
	lastWriteVersion := newWorkflow.LastWriteVersion
	shardID := m.shardID
	domainID := sqlplugin.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqlplugin.MustParseUUID(executionInfo.RunID)

	if err := p.ValidateCreateWorkflowModeState(
		request.Mode,
		newWorkflow,
	); err != nil {
		return nil, err
	}

	switch request.Mode {
	case p.CreateWorkflowModeContinueAsNew:
		// cannot create workflow with continue as new mode
		return nil, &workflow.InternalServiceError{
			Message: "CreateWorkflowExecution: operation failed, encounter invalid CreateWorkflowModeContinueAsNew",
		}
	}

	var err error
	var row *sqlplugin.CurrentExecutionsRow
	if row, err = lockCurrentExecutionIfExists(tx, m.shardID, domainID, workflowID); err != nil {
		return nil, err
	}

	// current workflow record check
	if row != nil {
		// current run ID, last write version, current workflow state check
		switch request.Mode {
		case p.CreateWorkflowModeBrandNew:
			return nil, &p.WorkflowExecutionAlreadyStartedError{
				Msg:              fmt.Sprintf("Workflow execution already running. WorkflowId: %v", row.WorkflowID),
				StartRequestID:   row.CreateRequestID,
				RunID:            row.RunID.String(),
				State:            int(row.State),
				CloseStatus:      int(row.CloseStatus),
				LastWriteVersion: row.LastWriteVersion,
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

		case p.CreateWorkflowModeZombie:
			// zombie workflow creation with existence of current record, this is a noop
			if err := assertRunIDMismatch(sqlplugin.MustParseUUID(executionInfo.RunID), row.RunID); err != nil {
				return nil, err
			}

		default:
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf(
					"CreteWorkflowExecution: unknown mode: %v",
					request.Mode,
				),
			}
		}
	}

	if err := createOrUpdateCurrentExecution(tx,
		request.Mode,
		m.shardID,
		domainID,
		workflowID,
		runID,
		executionInfo.State,
		executionInfo.CloseStatus,
		executionInfo.CreateRequestID,
		startVersion,
		lastWriteVersion); err != nil {
		return nil, err
	}

	if err := m.applyWorkflowSnapshotTxAsNew(tx, shardID, &request.NewWorkflowSnapshot); err != nil {
		return nil, err
	}

	return &p.CreateWorkflowExecutionResponse{}, nil
}

func (m *sqlExecutionManager) GetWorkflowExecution(
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {

	domainID := sqlplugin.MustParseUUID(request.DomainID)
	runID := sqlplugin.MustParseUUID(*request.Execution.RunId)
	wfID := *request.Execution.WorkflowId
	execution, err := m.db.SelectFromExecutions(&sqlplugin.ExecutionsFilter{
		ShardID: m.shardID, DomainID: domainID, WorkflowID: wfID, RunID: runID})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf(
					"Workflow execution not found.  WorkflowId: %v, RunId: %v",
					request.Execution.GetWorkflowId(),
					request.Execution.GetRunId(),
				),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution: failed. Error: %v", err),
		}
	}

	info, err := workflowExecutionInfoFromBlob(execution.Data, execution.DataEncoding)
	if err != nil {
		return nil, err
	}

	var state p.InternalWorkflowMutableState
	state.ExecutionInfo = &p.InternalWorkflowExecutionInfo{
		DomainID:                           execution.DomainID.String(),
		WorkflowID:                         execution.WorkflowID,
		RunID:                              execution.RunID.String(),
		NextEventID:                        execution.NextEventID,
		TaskList:                           info.GetTaskList(),
		WorkflowTypeName:                   info.GetWorkflowTypeName(),
		WorkflowTimeout:                    info.GetWorkflowTimeoutSeconds(),
		DecisionStartToCloseTimeout:        info.GetDecisionTaskTimeoutSeconds(),
		State:                              int(info.GetState()),
		CloseStatus:                        int(info.GetCloseStatus()),
		LastFirstEventID:                   info.GetLastFirstEventID(),
		LastProcessedEvent:                 info.GetLastProcessedEvent(),
		StartTimestamp:                     time.Unix(0, info.GetStartTimeNanos()),
		LastUpdatedTimestamp:               time.Unix(0, info.GetLastUpdatedTimeNanos()),
		CreateRequestID:                    info.GetCreateRequestID(),
		DecisionVersion:                    info.GetDecisionVersion(),
		DecisionScheduleID:                 info.GetDecisionScheduleID(),
		DecisionStartedID:                  info.GetDecisionStartedID(),
		DecisionRequestID:                  info.GetDecisionRequestID(),
		DecisionTimeout:                    info.GetDecisionTimeout(),
		DecisionAttempt:                    info.GetDecisionAttempt(),
		DecisionStartedTimestamp:           info.GetDecisionStartedTimestampNanos(),
		DecisionScheduledTimestamp:         info.GetDecisionScheduledTimestampNanos(),
		DecisionOriginalScheduledTimestamp: info.GetDecisionOriginalScheduledTimestampNanos(),
		StickyTaskList:                     info.GetStickyTaskList(),
		StickyScheduleToStartTimeout:       int32(info.GetStickyScheduleToStartTimeout()),
		ClientLibraryVersion:               info.GetClientLibraryVersion(),
		ClientFeatureVersion:               info.GetClientFeatureVersion(),
		ClientImpl:                         info.GetClientImpl(),
		SignalCount:                        int32(info.GetSignalCount()),
		HistorySize:                        info.GetHistorySize(),
		CronSchedule:                       info.GetCronSchedule(),
		CompletionEventBatchID:             common.EmptyEventID,
		HasRetryPolicy:                     info.GetHasRetryPolicy(),
		Attempt:                            int32(info.GetRetryAttempt()),
		InitialInterval:                    info.GetRetryInitialIntervalSeconds(),
		BackoffCoefficient:                 info.GetRetryBackoffCoefficient(),
		MaximumInterval:                    info.GetRetryMaximumIntervalSeconds(),
		MaximumAttempts:                    info.GetRetryMaximumAttempts(),
		ExpirationSeconds:                  info.GetRetryExpirationSeconds(),
		ExpirationTime:                     time.Unix(0, info.GetRetryExpirationTimeNanos()),
		BranchToken:                        info.GetEventBranchToken(),
		ExecutionContext:                   info.GetExecutionContext(),
		NonRetriableErrors:                 info.GetRetryNonRetryableErrors(),
		SearchAttributes:                   info.GetSearchAttributes(),
		Memo:                               info.GetMemo(),
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

	if info.GetVersionHistories() != nil {
		state.VersionHistories = p.NewDataBlob(
			info.GetVersionHistories(),
			common.EncodingType(info.GetVersionHistoriesEncoding()),
		)
	}

	if info.ParentDomainID != nil {
		state.ExecutionInfo.ParentDomainID = sqlplugin.UUID(info.ParentDomainID).String()
		state.ExecutionInfo.ParentWorkflowID = info.GetParentWorkflowID()
		state.ExecutionInfo.ParentRunID = sqlplugin.UUID(info.ParentRunID).String()
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

	if info.AutoResetPoints != nil {
		state.ExecutionInfo.AutoResetPoints = p.NewDataBlob(info.AutoResetPoints,
			common.EncodingType(info.GetAutoResetPointsEncoding()))
	}

	{
		var err error
		state.ActivityInfos, err = getActivityInfoMap(m.db,
			m.shardID,
			domainID,
			wfID,
			runID)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get activity info. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get timer info. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get child execution info. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get request cancel info. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get signal info. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get buffered events. Error: %v", err),
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
				Message: fmt.Sprintf("GetWorkflowExecution: failed to get signals requested. Error: %v", err),
			}
		}
	}

	return &p.InternalGetWorkflowExecutionResponse{State: &state}, nil
}

func (m *sqlExecutionManager) UpdateWorkflowExecution(
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {

	return m.txExecuteShardLocked("UpdateWorkflowExecution", request.RangeID, func(tx sqlplugin.Tx) error {
		return m.updateWorkflowExecutionTx(tx, request)
	})
}

func (m *sqlExecutionManager) updateWorkflowExecutionTx(
	tx sqlplugin.Tx,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	executionInfo := updateWorkflow.ExecutionInfo
	domainID := sqlplugin.MustParseUUID(executionInfo.DomainID)
	workflowID := executionInfo.WorkflowID
	runID := sqlplugin.MustParseUUID(executionInfo.RunID)
	shardID := m.shardID

	if err := p.ValidateUpdateWorkflowModeState(
		request.Mode,
		updateWorkflow,
		newWorkflow,
	); err != nil {
		return err
	}

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		if err := assertNotCurrentExecution(tx,
			shardID,
			domainID,
			workflowID,
			runID); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			newExecutionInfo := newWorkflow.ExecutionInfo
			startVersion := newWorkflow.StartVersion
			lastWriteVersion := newWorkflow.LastWriteVersion
			newDomainID := sqlplugin.MustParseUUID(newExecutionInfo.DomainID)
			newRunID := sqlplugin.MustParseUUID(newExecutionInfo.RunID)

			if !bytes.Equal(domainID, newDomainID) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("UpdateWorkflowExecution: cannot continue as new to another domain"),
				}
			}

			if err := assertRunIDAndUpdateCurrentExecution(tx,
				shardID,
				domainID,
				workflowID,
				newRunID,
				runID,
				newWorkflow.ExecutionInfo.CreateRequestID,
				newWorkflow.ExecutionInfo.State,
				newWorkflow.ExecutionInfo.CloseStatus,
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("UpdateWorkflowExecution: failed to continue as new current execution. Error: %v", err),
				}
			}
		} else {
			startVersion := updateWorkflow.StartVersion
			lastWriteVersion := updateWorkflow.LastWriteVersion
			// this is only to update the current record
			if err := assertRunIDAndUpdateCurrentExecution(tx,
				shardID,
				domainID,
				workflowID,
				runID,
				runID,
				executionInfo.CreateRequestID,
				executionInfo.State,
				executionInfo.CloseStatus,
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("UpdateWorkflowExecution: failed to update current execution. Error: %v", err),
				}
			}
		}

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution: unknown mode: %v", request.Mode),
		}
	}

	if err := applyWorkflowMutationTx(tx, shardID, &updateWorkflow); err != nil {
		return err
	}
	if newWorkflow != nil {
		if err := m.applyWorkflowSnapshotTxAsNew(tx, shardID, newWorkflow); err != nil {
			return err
		}
	}
	return nil
}

func (m *sqlExecutionManager) ResetWorkflowExecution(
	request *p.InternalResetWorkflowExecutionRequest,
) error {

	return m.txExecuteShardLocked("ResetWorkflowExecution", request.RangeID, func(tx sqlplugin.Tx) error {
		return m.resetWorkflowExecutionTx(tx, request)
	})
}

func (m *sqlExecutionManager) resetWorkflowExecutionTx(
	tx sqlplugin.Tx,
	request *p.InternalResetWorkflowExecutionRequest,
) error {

	shardID := m.shardID

	domainID := sqlplugin.MustParseUUID(request.NewWorkflowSnapshot.ExecutionInfo.DomainID)
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	baseRunID := sqlplugin.MustParseUUID(request.BaseRunID)
	baseRunNextEventID := request.BaseRunNextEventID

	currentRunID := sqlplugin.MustParseUUID(request.CurrentRunID)
	currentRunNextEventID := request.CurrentRunNextEventID

	newWorkflowRunID := sqlplugin.MustParseUUID(request.NewWorkflowSnapshot.ExecutionInfo.RunID)
	newExecutionInfo := request.NewWorkflowSnapshot.ExecutionInfo
	startVersion := request.NewWorkflowSnapshot.StartVersion
	lastWriteVersion := request.NewWorkflowSnapshot.LastWriteVersion

	// 1. update current execution
	if err := updateCurrentExecution(tx,
		shardID,
		domainID,
		workflowID,
		newWorkflowRunID,
		newExecutionInfo.CreateRequestID,
		newExecutionInfo.State,
		newExecutionInfo.CloseStatus,
		startVersion,
		lastWriteVersion,
	); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Failed at updateCurrentExecution. Error: %v", err),
		}
	}

	// 2. lock base run: we want to grab a read-lock for base run to prevent race condition
	// It is only needed when base run is not current run. Because we will obtain a lock on current run anyway.
	if !bytes.Equal(baseRunID, currentRunID) {
		if err := lockAndCheckNextEventID(tx, shardID, domainID, workflowID, baseRunID, baseRunNextEventID); err != nil {
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

	// 3. update or lock current run
	if request.CurrentWorkflowMutation != nil {
		if err := applyWorkflowMutationTx(tx, m.shardID, request.CurrentWorkflowMutation); err != nil {
			return err
		}
	} else {
		// even the current run is not running, we need to lock the current run:
		// 1). in case it is changed by conflict resolution
		// 2). in case delete history timer kicks in if the base is current
		if err := lockAndCheckNextEventID(tx, shardID, domainID, workflowID, currentRunID, currentRunNextEventID); err != nil {
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

	// 4. create the new reset workflow
	return m.applyWorkflowSnapshotTxAsNew(tx, m.shardID, &request.NewWorkflowSnapshot)
}

func (m *sqlExecutionManager) ConflictResolveWorkflowExecution(
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {

	return m.txExecuteShardLocked("ConflictResolveWorkflowExecution", request.RangeID, func(tx sqlplugin.Tx) error {
		return m.conflictResolveWorkflowExecutionTx(tx, request)
	})
}

func (m *sqlExecutionManager) conflictResolveWorkflowExecutionTx(
	tx sqlplugin.Tx,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := m.shardID

	domainID := sqlplugin.MustParseUUID(resetWorkflow.ExecutionInfo.DomainID)
	workflowID := resetWorkflow.ExecutionInfo.WorkflowID

	if err := p.ValidateConflictResolveWorkflowModeState(
		request.Mode,
		resetWorkflow,
		newWorkflow,
		currentWorkflow,
	); err != nil {
		return err
	}

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		if err := assertNotCurrentExecution(tx,
			shardID,
			domainID,
			workflowID,
			sqlplugin.MustParseUUID(resetWorkflow.ExecutionInfo.RunID)); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionInfo := resetWorkflow.ExecutionInfo
		startVersion := resetWorkflow.StartVersion
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			executionInfo = newWorkflow.ExecutionInfo
			startVersion = newWorkflow.StartVersion
			lastWriteVersion = newWorkflow.LastWriteVersion
		}
		runID := sqlplugin.MustParseUUID(executionInfo.RunID)
		createRequestID := executionInfo.CreateRequestID
		state := executionInfo.State
		closeStatus := executionInfo.CloseStatus

		if request.CurrentWorkflowCAS != nil {
			prevRunID := sqlplugin.MustParseUUID(request.CurrentWorkflowCAS.PrevRunID)
			prevLastWriteVersion := request.CurrentWorkflowCAS.PrevLastWriteVersion
			prevState := request.CurrentWorkflowCAS.PrevState

			if err := assertAndUpdateCurrentExecution(tx,
				m.shardID,
				domainID,
				workflowID,
				runID,
				prevRunID,
				prevLastWriteVersion,
				prevState,
				createRequestID,
				state,
				closeStatus,
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{Message: fmt.Sprintf(
					"ConflictResolveWorkflowExecution. Failed to comare and swap the current record. Error: %v",
					err,
				)}
			}
		} else if currentWorkflow != nil {
			prevRunID := sqlplugin.MustParseUUID(currentWorkflow.ExecutionInfo.RunID)

			if err := assertRunIDAndUpdateCurrentExecution(tx,
				m.shardID,
				domainID,
				workflowID,
				runID,
				prevRunID,
				createRequestID,
				state,
				closeStatus,
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{Message: fmt.Sprintf(
					"ConflictResolveWorkflowExecution. Failed to comare and swap the current record. Error: %v",
					err,
				)}
			}
		} else {
			// reset workflow is current
			prevRunID := sqlplugin.MustParseUUID(resetWorkflow.ExecutionInfo.RunID)

			if err := assertRunIDAndUpdateCurrentExecution(tx,
				m.shardID,
				domainID,
				workflowID,
				runID,
				prevRunID,
				createRequestID,
				state,
				closeStatus,
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{Message: fmt.Sprintf(
					"ConflictResolveWorkflowExecution. Failed to comare and swap the current record. Error: %v",
					err,
				)}
			}
		}

	default:
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode),
		}
	}

	if err := applyWorkflowSnapshotTxAsReset(tx, shardID, &resetWorkflow); err != nil {
		return err
	}
	if currentWorkflow != nil {
		if err := applyWorkflowMutationTx(tx, shardID, currentWorkflow); err != nil {
			return err
		}
	}
	if newWorkflow != nil {
		if err := m.applyWorkflowSnapshotTxAsNew(tx, shardID, newWorkflow); err != nil {
			return err
		}
	}
	return nil
}

func (m *sqlExecutionManager) DeleteTask(request *p.DeleteTaskRequest) error {
	//TODO: This needs implement when we use sql for tasks.
	// 		https://github.com/uber/cadence/issues/2479
	return nil
}

func (m *sqlExecutionManager) DeleteWorkflowExecution(
	request *p.DeleteWorkflowExecutionRequest,
) error {

	domainID := sqlplugin.MustParseUUID(request.DomainID)
	runID := sqlplugin.MustParseUUID(request.RunID)
	_, err := m.db.DeleteFromExecutions(&sqlplugin.ExecutionsFilter{
		ShardID:    m.shardID,
		DomainID:   domainID,
		WorkflowID: request.WorkflowID,
		RunID:      runID,
	})
	return err
}

// its possible for a new run of the same workflow to have started after the run we are deleting
// here was finished. In that case, current_executions table will have the same workflowID but different
// runID. The following code will delete the row from current_executions if and only if the runID is
// same as the one we are trying to delete here
func (m *sqlExecutionManager) DeleteCurrentWorkflowExecution(
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {

	domainID := sqlplugin.MustParseUUID(request.DomainID)
	runID := sqlplugin.MustParseUUID(request.RunID)
	_, err := m.db.DeleteFromCurrentExecutions(&sqlplugin.CurrentExecutionsFilter{
		ShardID:    int64(m.shardID),
		DomainID:   domainID,
		WorkflowID: request.WorkflowID,
		RunID:      runID,
	})
	return err
}

func (m *sqlExecutionManager) GetCurrentExecution(
	request *p.GetCurrentExecutionRequest,
) (*p.GetCurrentExecutionResponse, error) {

	row, err := m.db.SelectFromCurrentExecutions(&sqlplugin.CurrentExecutionsFilter{
		ShardID:    int64(m.shardID),
		DomainID:   sqlplugin.MustParseUUID(request.DomainID),
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

func (m *sqlExecutionManager) GetTransferTasks(
	request *p.GetTransferTasksRequest,
) (*p.GetTransferTasksResponse, error) {

	rows, err := m.db.SelectFromTransferTasks(&sqlplugin.TransferTasksFilter{
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
			DomainID:                sqlplugin.UUID(info.DomainID).String(),
			WorkflowID:              info.GetWorkflowID(),
			RunID:                   sqlplugin.UUID(info.RunID).String(),
			VisibilityTimestamp:     time.Unix(0, info.GetVisibilityTimestampNanos()),
			TargetDomainID:          sqlplugin.UUID(info.TargetDomainID).String(),
			TargetWorkflowID:        info.GetTargetWorkflowID(),
			TargetRunID:             sqlplugin.UUID(info.TargetRunID).String(),
			TargetChildWorkflowOnly: info.GetTargetChildWorkflowOnly(),
			TaskList:                info.GetTaskList(),
			TaskType:                int(info.GetTaskType()),
			ScheduleID:              info.GetScheduleID(),
			Version:                 info.GetVersion(),
		}
	}
	return resp, nil
}

func (m *sqlExecutionManager) CompleteTransferTask(
	request *p.CompleteTransferTaskRequest,
) error {

	if _, err := m.db.DeleteFromTransferTasks(&sqlplugin.TransferTasksFilter{
		ShardID: m.shardID,
		TaskID:  &request.TaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteTransferTask(
	request *p.RangeCompleteTransferTaskRequest,
) error {

	if _, err := m.db.DeleteFromTransferTasks(&sqlplugin.TransferTasksFilter{
		ShardID:   m.shardID,
		MinTaskID: &request.ExclusiveBeginTaskID,
		MaxTaskID: &request.InclusiveEndTaskID}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetReplicationTasks(
	request *p.GetReplicationTasksRequest,
) (*p.GetReplicationTasksResponse, error) {

	readLevel, maxReadLevelInclusive, err := getReadLevels(request)
	if err != nil {
		return nil, err
	}

	rows, err := m.db.SelectFromReplicationTasks(
		&sqlplugin.ReplicationTasksFilter{
			ShardID:   m.shardID,
			MinTaskID: readLevel,
			MaxTaskID: maxReadLevelInclusive,
			PageSize:  request.BatchSize,
		})

	switch err {
	case nil:
		return m.populateGetReplicationTasksResponse(rows, request.MaxReadLevel)
	case sql.ErrNoRows:
		return &p.GetReplicationTasksResponse{}, nil
	default:
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err),
		}
	}
}

func getReadLevels(request *p.GetReplicationTasksRequest) (readLevel int64, maxReadLevelInclusive int64, err error) {
	readLevel = request.ReadLevel
	if len(request.NextPageToken) > 0 {
		readLevel, err = deserializePageToken(request.NextPageToken)
		if err != nil {
			return 0, 0, err
		}
	}

	maxReadLevelInclusive = collection.MaxInt64(readLevel+int64(request.BatchSize), request.MaxReadLevel)
	return readLevel, maxReadLevelInclusive, nil
}

func (m *sqlExecutionManager) populateGetReplicationTasksResponse(
	rows []sqlplugin.ReplicationTasksRow,
	requestMaxReadLevel int64,
) (*p.GetReplicationTasksResponse, error) {
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
			TaskID:              row.TaskID,
			DomainID:            sqlplugin.UUID(info.DomainID).String(),
			WorkflowID:          info.GetWorkflowID(),
			RunID:               sqlplugin.UUID(info.RunID).String(),
			TaskType:            int(info.GetTaskType()),
			FirstEventID:        info.GetFirstEventID(),
			NextEventID:         info.GetNextEventID(),
			Version:             info.GetVersion(),
			LastReplicationInfo: lastReplicationInfo,
			ScheduledID:         info.GetScheduledID(),
			BranchToken:         info.GetBranchToken(),
			NewRunBranchToken:   info.GetNewRunBranchToken(),
			ResetWorkflow:       info.GetResetWorkflow(),
		}
	}
	var nextPageToken []byte
	lastTaskID := rows[len(rows)-1].TaskID
	if lastTaskID < requestMaxReadLevel {
		nextPageToken = serializePageToken(lastTaskID)
	}
	return &p.GetReplicationTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *sqlExecutionManager) CompleteReplicationTask(
	request *p.CompleteReplicationTaskRequest,
) error {

	if _, err := m.db.DeleteFromReplicationTasks(&sqlplugin.ReplicationTasksFilter{
		ShardID: m.shardID,
		TaskID:  request.TaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteReplicationTask(
	request *p.RangeCompleteReplicationTaskRequest,
) error {

	if _, err := m.db.RangeDeleteFromReplicationTasks(&sqlplugin.ReplicationTasksFilter{
		ShardID: m.shardID,
		TaskID:  request.InclusiveEndTaskID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteReplicationTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetReplicationTasksFromDLQ(
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.GetReplicationTasksFromDLQResponse, error) {

	readLevel, maxReadLevelInclusive, err := getReadLevels(&request.GetReplicationTasksRequest)
	if err != nil {
		return nil, err
	}

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID:   m.shardID,
		MinTaskID: readLevel,
		MaxTaskID: maxReadLevelInclusive,
		PageSize:  request.BatchSize,
	}
	rows, err := m.db.SelectFromReplicationTasksDLQ(&sqlplugin.ReplicationTasksDLQFilter{
		ReplicationTasksFilter: filter,
		SourceClusterName:      request.SourceClusterName,
	})

	switch err {
	case nil:
		return m.populateGetReplicationTasksResponse(rows, request.MaxReadLevel)
	case sql.ErrNoRows:
		return &p.GetReplicationTasksResponse{}, nil
	default:
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err),
		}
	}
}

func (m *sqlExecutionManager) DeleteReplicationTaskFromDLQ(
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: m.shardID,
		TaskID:  request.TaskID,
	}

	if _, err := m.db.DeleteMessageFromReplicationTasksDLQ(&sqlplugin.ReplicationTasksDLQFilter{
		ReplicationTasksFilter: filter,
		SourceClusterName:      request.SourceClusterName,
	}); err != nil {
		return err
	}
	return nil
}

func (m *sqlExecutionManager) RangeDeleteReplicationTaskFromDLQ(
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID:            m.shardID,
		TaskID:             request.ExclusiveBeginTaskID,
		InclusiveEndTaskID: request.InclusiveEndTaskID,
	}

	if _, err := m.db.RangeDeleteMessageFromReplicationTasksDLQ(&sqlplugin.ReplicationTasksDLQFilter{
		ReplicationTasksFilter: filter,
		SourceClusterName:      request.SourceClusterName,
	}); err != nil {
		return err
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

func (m *sqlExecutionManager) GetTimerIndexTasks(
	request *p.GetTimerIndexTasksRequest,
) (*p.GetTimerIndexTasksResponse, error) {

	pageToken := &timerTaskPageToken{TaskID: math.MinInt64, Timestamp: request.MinTimestamp}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("error deserializing timerTaskPageToken: %v", err),
			}
		}
	}

	rows, err := m.db.SelectFromTimerTasks(&sqlplugin.TimerTasksFilter{
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
			DomainID:            sqlplugin.UUID(info.DomainID).String(),
			WorkflowID:          info.GetWorkflowID(),
			RunID:               sqlplugin.UUID(info.RunID).String(),
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

func (m *sqlExecutionManager) CompleteTimerTask(
	request *p.CompleteTimerTaskRequest,
) error {

	if _, err := m.db.DeleteFromTimerTasks(&sqlplugin.TimerTasksFilter{
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

func (m *sqlExecutionManager) RangeCompleteTimerTask(
	request *p.RangeCompleteTimerTaskRequest,
) error {

	start := request.InclusiveBeginTimestamp
	end := request.ExclusiveEndTimestamp
	if _, err := m.db.DeleteFromTimerTasks(&sqlplugin.TimerTasksFilter{
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

func (m *sqlExecutionManager) PutReplicationTaskToDLQ(request *p.PutReplicationTaskToDLQRequest) error {
	replicationTask := request.TaskInfo
	blob, err := replicationTaskInfoToBlob(&sqlblobs.ReplicationTaskInfo{
		DomainID:            sqlplugin.MustParseUUID(replicationTask.DomainID),
		WorkflowID:          &replicationTask.WorkflowID,
		RunID:               sqlplugin.MustParseUUID(replicationTask.RunID),
		TaskType:            common.Int16Ptr(int16(replicationTask.TaskType)),
		FirstEventID:        &replicationTask.FirstEventID,
		NextEventID:         &replicationTask.NextEventID,
		Version:             &replicationTask.Version,
		LastReplicationInfo: toSqldbReplicationInfo(replicationTask.LastReplicationInfo),
		ScheduledID:         &replicationTask.ScheduledID,
		BranchToken:         replicationTask.BranchToken,
		NewRunBranchToken:   replicationTask.NewRunBranchToken,
		ResetWorkflow:       &replicationTask.ResetWorkflow,
	})
	if err != nil {
		return err
	}

	row := &sqlplugin.ReplicationTaskDLQRow{
		SourceClusterName: request.SourceClusterName,
		ShardID:           m.shardID,
		TaskID:            replicationTask.TaskID,
		Data:              blob.Data,
		DataEncoding:      string(blob.Encoding),
	}

	_, err = m.db.InsertIntoReplicationTasksDLQ(row)

	// Tasks are immutable. So it's fine if we already persisted it before.
	// This can happen when tasks are retried (ack and cleanup can have lag on source side).
	if err != nil && !m.db.IsDupEntryError(err) {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Error: %v", err),
		}
	}

	return nil
}

func toSqldbReplicationInfo(info map[string]*p.ReplicationInfo) map[string]*sqlblobs.ReplicationInfo {
	replicationInfoMap := make(map[string]*sqlblobs.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &sqlblobs.ReplicationInfo{
			Version:     common.Int64Ptr(v.Version),
			LastEventID: common.Int64Ptr(v.LastEventID),
		}
	}

	return replicationInfoMap
}
