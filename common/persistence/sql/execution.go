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

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type sqlExecutionStore struct {
	SqlStore
}

var _ p.ExecutionStore = (*sqlExecutionStore)(nil)

// NewSQLExecutionStore creates an instance of ExecutionStore
func NewSQLExecutionStore(
	db sqlplugin.DB,
	logger log.Logger,
) (p.ExecutionStore, error) {

	return &sqlExecutionStore{
		SqlStore: NewSqlStore(db, logger),
	}, nil
}

// txExecuteShardLocked executes f under transaction and with read lock on shard row
func (m *sqlExecutionStore) txExecuteShardLocked(
	ctx context.Context,
	operation string,
	shardID int32,
	rangeID int64,
	fn func(tx sqlplugin.Tx) error,
) error {

	return m.txExecute(ctx, operation, func(tx sqlplugin.Tx) error {
		if err := readLockShard(ctx, tx, shardID, rangeID); err != nil {
			return err
		}
		err := fn(tx)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *sqlExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (response *p.InternalCreateWorkflowExecutionResponse, err error) {
	for _, req := range request.NewWorkflowNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return nil, err
		}
	}

	err = m.txExecuteShardLocked(ctx,
		"CreateWorkflowExecution",
		request.ShardID,
		request.RangeID,
		func(tx sqlplugin.Tx) error {
			response, err = m.createWorkflowExecutionTx(ctx, tx, request)
			return err
		})
	return
}

func (m *sqlExecutionStore) createWorkflowExecutionTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {

	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	shardID := request.ShardID
	namespaceID := primitives.MustParseUUID(newWorkflow.NamespaceID)
	workflowID := newWorkflow.WorkflowID
	runID := primitives.MustParseUUID(newWorkflow.RunID)

	var err error
	var currentRow *sqlplugin.CurrentExecutionsRow
	if currentRow, err = lockCurrentExecutionIfExists(ctx,
		tx,
		shardID,
		namespaceID,
		workflowID,
	); err != nil {
		return nil, err
	}

	// current run ID, last write version, current workflow state check
	switch request.Mode {
	case p.CreateWorkflowModeBrandNew:
		if currentRow == nil {
			// current row does not exists, suits the create mode
		} else {
			if currentRow.RunID.String() != request.PreviousRunID {
				return nil, extractCurrentWorkflowConflictError(
					currentRow,
					fmt.Sprintf(
						"Workflow execution creation condition failed. workflow ID: %v, current run ID: %v, request run ID: %v",
						workflowID,
						currentRow.RunID.String(),
						request.PreviousRunID,
					),
				)
			}
			// current run ID is already request ID
		}

	case p.CreateWorkflowModeUpdateCurrent:
		if currentRow == nil {
			return nil, extractCurrentWorkflowConflictError(currentRow, "")
		}

		// currentRow != nil

		if currentRow.RunID.String() != request.PreviousRunID {
			return nil, extractCurrentWorkflowConflictError(
				currentRow,
				fmt.Sprintf(
					"Workflow execution creation condition failed. workflow ID: %v, current run ID: %v, request run ID: %v",
					workflowID,
					currentRow.RunID.String(),
					request.PreviousRunID,
				),
			)
		}
		if request.PreviousLastWriteVersion != currentRow.LastWriteVersion {
			return nil, extractCurrentWorkflowConflictError(
				currentRow,
				fmt.Sprintf(
					"Workflow execution creation condition failed. workflow ID: %v, current last write version: %v, request last write version: %v",
					workflowID,
					currentRow.LastWriteVersion,
					request.PreviousLastWriteVersion,
				),
			)
		}
		if currentRow.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return nil, extractCurrentWorkflowConflictError(
				currentRow,
				fmt.Sprintf(
					"Workflow execution creation condition failed. workflow ID: %v, current state: %v, request state: %v",
					workflowID,
					currentRow.State,
					enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				),
			)
		}

	case p.CreateWorkflowModeBypassCurrent:
		if err := assertRunIDMismatch(
			primitives.MustParseUUID(newWorkflow.ExecutionState.RunId),
			currentRow,
		); err != nil {
			return nil, err
		}

	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreteWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := createOrUpdateCurrentExecution(ctx,
		tx,
		request.Mode,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
		newWorkflow.ExecutionState.State,
		newWorkflow.ExecutionState.Status,
		newWorkflow.ExecutionState.CreateRequestId,
		lastWriteVersion,
	); err != nil {
		return nil, err
	}

	if err := m.applyWorkflowSnapshotTxAsNew(ctx,
		tx,
		shardID,
		&request.NewWorkflowSnapshot,
	); err != nil {
		return nil, err
	}

	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (m *sqlExecutionStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	namespaceID := primitives.MustParseUUID(request.NamespaceID)
	workflowID := request.WorkflowID
	runID := primitives.MustParseUUID(request.RunID)
	executionsRow, err := m.Db.SelectFromExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	switch err {
	case nil:
		// noop
	case sql.ErrNoRows:
		return nil, serviceerror.NewNotFound(fmt.Sprintf("Workflow executionsRow not found.  WorkflowId: %v, RunId: %v", workflowID, runID))
	default:
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed. Error: %v", err))
	}

	state := &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(executionsRow.Data, executionsRow.DataEncoding),
		ExecutionState: p.NewDataBlob(executionsRow.State, executionsRow.StateEncoding),
		NextEventID:    executionsRow.NextEventID,

		DBRecordVersion: executionsRow.DBRecordVersion,
	}

	state.ActivityInfos, err = getActivityInfoMap(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get activity info. Error: %v", err))
	}

	state.TimerInfos, err = getTimerInfoMap(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get timer info. Error: %v", err))
	}

	state.ChildExecutionInfos, err = getChildExecutionInfoMap(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get child executionsRow info. Error: %v", err))
	}

	state.RequestCancelInfos, err = getRequestCancelInfoMap(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get request cancel info. Error: %v", err))
	}

	state.SignalInfos, err = getSignalInfoMap(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get signal info. Error: %v", err))
	}

	state.BufferedEvents, err = getBufferedEvents(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get buffered events. Error: %v", err))
	}

	state.SignalRequestedIDs, err = getSignalsRequested(ctx,
		m.Db,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution: failed to get signals requested. Error: %v", err))
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: executionsRow.DBRecordVersion,
	}, nil
}

func (m *sqlExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	// first append history
	for _, req := range request.UpdateWorkflowNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	// then update mutable state
	return m.txExecuteShardLocked(ctx,
		"UpdateWorkflowExecution",
		request.ShardID,
		request.RangeID,
		func(tx sqlplugin.Tx) error {
			return m.updateWorkflowExecutionTx(ctx, tx, request)
		})
}

func (m *sqlExecutionStore) updateWorkflowExecutionTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	namespaceID := primitives.MustParseUUID(updateWorkflow.NamespaceID)
	workflowID := updateWorkflow.WorkflowID
	runID := primitives.MustParseUUID(updateWorkflow.ExecutionState.RunId)
	shardID := request.ShardID

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		if err := assertNotCurrentExecution(ctx,
			tx,
			shardID,
			namespaceID,
			workflowID,
			runID,
		); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			lastWriteVersion := newWorkflow.LastWriteVersion
			newNamespaceID := primitives.MustParseUUID(newWorkflow.NamespaceID)
			newRunID := primitives.MustParseUUID(newWorkflow.ExecutionState.RunId)

			if !bytes.Equal(namespaceID, newNamespaceID) {
				return serviceerror.NewUnavailable("UpdateWorkflowExecution: cannot continue as new to another namespace")
			}

			if err := assertRunIDAndUpdateCurrentExecution(ctx,
				tx,
				shardID,
				namespaceID,
				workflowID,
				newRunID,
				runID,
				newWorkflow.ExecutionState.CreateRequestId,
				newWorkflow.ExecutionState.State,
				newWorkflow.ExecutionState.Status,
				lastWriteVersion,
			); err != nil {
				return err
			}
		} else {
			lastWriteVersion := updateWorkflow.LastWriteVersion
			// this is only to update the current record
			if err := assertRunIDAndUpdateCurrentExecution(ctx,
				tx,
				shardID,
				namespaceID,
				workflowID,
				runID,
				runID,
				updateWorkflow.ExecutionState.CreateRequestId,
				updateWorkflow.ExecutionState.State,
				updateWorkflow.ExecutionState.Status,
				lastWriteVersion,
			); err != nil {
				return err
			}
		}

	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("UpdateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowMutationTx(ctx,
		tx,
		shardID,
		&updateWorkflow,
	); err != nil {
		return err
	}

	if newWorkflow != nil {
		if err := m.applyWorkflowSnapshotTxAsNew(ctx,
			tx,
			shardID,
			newWorkflow,
		); err != nil {
			return err
		}
	}
	return nil
}

func (m *sqlExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	// first append history
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		if err := m.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	return m.txExecuteShardLocked(ctx,
		"ConflictResolveWorkflowExecution",
		request.ShardID,
		request.RangeID,
		func(tx sqlplugin.Tx) error {
			return m.conflictResolveWorkflowExecutionTx(ctx, tx, request)
		})
}

func (m *sqlExecutionStore) conflictResolveWorkflowExecutionTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := request.ShardID

	namespaceID := primitives.MustParseUUID(resetWorkflow.NamespaceID)
	workflowID := resetWorkflow.WorkflowID

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		if err := assertNotCurrentExecution(ctx,
			tx,
			shardID,
			namespaceID,
			workflowID,
			primitives.MustParseUUID(resetWorkflow.ExecutionState.RunId),
		); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionState := resetWorkflow.ExecutionState
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			executionState = newWorkflow.ExecutionState
			lastWriteVersion = newWorkflow.LastWriteVersion
		}
		runID := primitives.MustParseUUID(executionState.RunId)
		createRequestID := executionState.CreateRequestId
		state := executionState.State
		status := executionState.Status

		if currentWorkflow != nil {
			prevRunID := primitives.MustParseUUID(currentWorkflow.ExecutionState.RunId)

			if err := assertRunIDAndUpdateCurrentExecution(ctx,
				tx,
				shardID,
				namespaceID,
				workflowID,
				runID,
				prevRunID,
				createRequestID,
				state,
				status,
				lastWriteVersion,
			); err != nil {
				return err
			}
		} else {
			// reset workflow is current
			prevRunID := primitives.MustParseUUID(resetWorkflow.ExecutionState.RunId)

			if err := assertRunIDAndUpdateCurrentExecution(ctx,
				tx,
				shardID,
				namespaceID,
				workflowID,
				runID,
				prevRunID,
				createRequestID,
				state,
				status,
				lastWriteVersion,
			); err != nil {
				return err
			}
		}

	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotTxAsReset(ctx,
		tx,
		shardID,
		&resetWorkflow,
	); err != nil {
		return err
	}

	if currentWorkflow != nil {
		if err := applyWorkflowMutationTx(ctx,
			tx,
			shardID,
			currentWorkflow,
		); err != nil {
			return err
		}
	}

	if newWorkflow != nil {
		if err := m.applyWorkflowSnapshotTxAsNew(ctx,
			tx,
			shardID,
			newWorkflow,
		); err != nil {
			return err
		}
	}
	return nil
}

func (m *sqlExecutionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	namespaceID := primitives.MustParseUUID(request.NamespaceID)
	runID := primitives.MustParseUUID(request.RunID)
	_, err := m.Db.DeleteFromExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  request.WorkflowID,
		RunID:       runID,
	})
	return err
}

// its possible for a new run of the same workflow to have started after the run we are deleting
// here was finished. In that case, current_executions table will have the same workflowID but different
// runID. The following code will delete the row from current_executions if and only if the runID is
// same as the one we are trying to delete here
func (m *sqlExecutionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	namespaceID := primitives.MustParseUUID(request.NamespaceID)
	runID := primitives.MustParseUUID(request.RunID)
	_, err := m.Db.DeleteFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  request.WorkflowID,
		RunID:       runID,
	})
	return err
}

func (m *sqlExecutionStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, error) {
	row, err := m.Db.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: primitives.MustParseUUID(request.NamespaceID),
		WorkflowID:  request.WorkflowID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(err.Error())
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID: row.RunID.String(),
		ExecutionState: &persistence.WorkflowExecutionState{
			CreateRequestId: row.CreateRequestID,
			State:           row.State,
			Status:          row.Status,
		},
	}, nil
}

func (m *sqlExecutionStore) SetWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
) error {
	return m.txExecuteShardLocked(ctx,
		"SetWorkflowExecution",
		request.ShardID,
		request.RangeID,
		func(tx sqlplugin.Tx) error {
			return m.setWorkflowExecutionTx(ctx, tx, request)
		})
}

func (m *sqlExecutionStore) setWorkflowExecutionTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	request *p.InternalSetWorkflowExecutionRequest,
) error {
	shardID := request.ShardID
	setSnapshot := request.SetWorkflowSnapshot

	if err := applyWorkflowSnapshotTxAsReset(ctx,
		tx,
		shardID,
		&setSnapshot,
	); err != nil {
		return err
	}
	return nil
}

func (m *sqlExecutionStore) ListConcreteExecutions(
	_ context.Context,
	_ *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListConcreteExecutions is not implemented")
}
