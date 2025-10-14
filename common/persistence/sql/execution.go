package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type sqlExecutionStore struct {
	SqlStore
	p.HistoryBranchUtilImpl
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
		return nil, serviceerror.NewInternalf("CreteWorkflowExecution: unknown mode: %v", request.Mode)
	}

	row := sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  newWorkflow.ExecutionState.CreateRequestId,
		State:            newWorkflow.ExecutionState.State,
		Status:           newWorkflow.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
		StartTime:        getStartTimeFromState(newWorkflow.ExecutionState),
		Data:             newWorkflow.ExecutionStateBlob.Data,
		DataEncoding:     newWorkflow.ExecutionStateBlob.EncodingType.String(),
	}

	if err := createOrUpdateCurrentExecution(ctx, tx, row, request.Mode); err != nil {
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
	executionsRow, err := m.DB.SelectFromExecutions(ctx, sqlplugin.ExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	switch err {
	case nil:
		// noop
	case sql.ErrNoRows:
		return nil, serviceerror.NewNotFoundf("Workflow executionsRow not found.  WorkflowId: %v, RunId: %v", workflowID, runID)
	default:
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed. Error: %v", err)
	}

	state := &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(executionsRow.Data, executionsRow.DataEncoding),
		ExecutionState: p.NewDataBlob(executionsRow.State, executionsRow.StateEncoding),
		NextEventID:    executionsRow.NextEventID,

		DBRecordVersion: executionsRow.DBRecordVersion,
	}

	state.ActivityInfos, err = getActivityInfoMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get activity info. Error: %v", err)
	}

	state.TimerInfos, err = getTimerInfoMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get timer info. Error: %v", err)
	}

	state.ChildExecutionInfos, err = getChildExecutionInfoMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get child executionsRow info. Error: %v", err)
	}

	state.RequestCancelInfos, err = getRequestCancelInfoMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get request cancel info. Error: %v", err)
	}

	state.SignalInfos, err = getSignalInfoMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get signal info. Error: %v", err)
	}

	state.BufferedEvents, err = getBufferedEvents(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get buffered events. Error: %v", err)
	}

	state.ChasmNodes, err = getChasmNodeMap(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get CHASM nodes. Error: %v", err)
	}

	state.SignalRequestedIDs, err = getSignalsRequested(ctx,
		m.DB,
		request.ShardID,
		namespaceID,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get signals requested. Error: %v", err)
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
	case p.UpdateWorkflowModeIgnoreCurrent:
		// noop

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
		row := sqlplugin.CurrentExecutionsRow{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			StartTime:   nil,
		}

		if newWorkflow != nil {
			row.CreateRequestID = newWorkflow.ExecutionState.CreateRequestId
			row.State = newWorkflow.ExecutionState.State
			row.Status = newWorkflow.ExecutionState.Status
			row.LastWriteVersion = newWorkflow.LastWriteVersion
			row.NamespaceID = primitives.MustParseUUID(newWorkflow.NamespaceID)
			row.RunID = primitives.MustParseUUID(newWorkflow.ExecutionState.RunId)
			row.StartTime = getStartTimeFromState(newWorkflow.ExecutionState)
			row.Data = newWorkflow.ExecutionStateBlob.Data
			row.DataEncoding = newWorkflow.ExecutionStateBlob.EncodingType.String()

			if !bytes.Equal(namespaceID, row.NamespaceID) {
				return serviceerror.NewUnavailable("UpdateWorkflowExecution: cannot continue as new to another namespace")
			}
		} else {
			row.CreateRequestID = updateWorkflow.ExecutionState.CreateRequestId
			row.State = updateWorkflow.ExecutionState.State
			row.Status = updateWorkflow.ExecutionState.Status
			row.LastWriteVersion = updateWorkflow.LastWriteVersion
			row.RunID = runID
			row.StartTime = getStartTimeFromState(updateWorkflow.ExecutionState)
			row.Data = updateWorkflow.ExecutionStateBlob.Data
			row.DataEncoding = updateWorkflow.ExecutionStateBlob.EncodingType.String()
			// we still call update only to update the current record
		}
		if err := assertRunIDAndUpdateCurrentExecution(ctx, tx, row, runID); err != nil {
			return err
		}

	default:
		return serviceerror.NewUnavailablef("UpdateWorkflowExecution: unknown mode: %v", request.Mode)
	}

	if err := applyWorkflowMutationTx(ctx, tx, shardID, &updateWorkflow); err != nil {
		return err
	}

	if newWorkflow != nil {
		if err := m.applyWorkflowSnapshotTxAsNew(ctx, tx, shardID, newWorkflow); err != nil {
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
		executionStateBlob := resetWorkflow.ExecutionStateBlob
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			executionState = newWorkflow.ExecutionState
			executionStateBlob = newWorkflow.ExecutionStateBlob
			lastWriteVersion = newWorkflow.LastWriteVersion
		}
		runID := primitives.MustParseUUID(executionState.RunId)
		createRequestID := executionState.CreateRequestId
		state := executionState.State
		status := executionState.Status

		row := sqlplugin.CurrentExecutionsRow{
			ShardID:          shardID,
			NamespaceID:      namespaceID,
			WorkflowID:       workflowID,
			RunID:            runID,
			CreateRequestID:  createRequestID,
			State:            state,
			Status:           status,
			LastWriteVersion: lastWriteVersion,
			StartTime:        getStartTimeFromState(executionState),
			Data:             executionStateBlob.Data,
			DataEncoding:     executionStateBlob.EncodingType.String(),
		}
		var prevRunID primitives.UUID
		if currentWorkflow != nil {
			prevRunID = primitives.MustParseUUID(currentWorkflow.ExecutionState.RunId)
		} else {
			// reset workflow is current
			prevRunID = primitives.MustParseUUID(resetWorkflow.ExecutionState.RunId)
		}
		if err := assertRunIDAndUpdateCurrentExecution(ctx, tx, row, prevRunID); err != nil {
			return err
		}

	default:
		return serviceerror.NewUnavailablef("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode)
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
	return m.txExecute(ctx, "DeleteWorkflowExecution", func(tx sqlplugin.Tx) error {
		_, err := tx.DeleteAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromChildExecutionInfoMaps: %w", err)
		}
		_, err = tx.DeleteAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromActivityInfoMaps: %w", err)
		}
		_, err = tx.DeleteAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromRequestCancelInfoMaps: %w", err)
		}
		_, err = tx.DeleteAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromSignalInfoMaps: %w", err)
		}
		_, err = tx.DeleteAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromTimerInfoMaps: %w", err)
		}
		_, err = tx.DeleteAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteAllFromSignalsRequestedSets: %w", err)
		}
		_, err = tx.DeleteFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteFromBufferedEvents: %w", err)
		}
		_, err = tx.DeleteFromExecutions(ctx, sqlplugin.ExecutionsFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  request.WorkflowID,
			RunID:       runID,
		})
		if err != nil {
			return fmt.Errorf("failed to execute DeleteFromExecutions: %w", err)
		}
		return nil
	})
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
	_, err := m.DB.DeleteFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
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
	row, err := m.DB.SelectFromCurrentExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
		ShardID:     request.ShardID,
		NamespaceID: primitives.MustParseUUID(request.NamespaceID),
		WorkflowID:  request.WorkflowID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(err.Error())
		}
		return nil, serviceerror.NewUnavailablef("GetCurrentExecution operation failed. Error: %v", err)
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID: row.RunID.String(),
		ExecutionState: &persistencespb.WorkflowExecutionState{
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

	return applyWorkflowSnapshotTxAsReset(ctx,
		tx,
		shardID,
		&setSnapshot,
	)
}

func (m *sqlExecutionStore) ListConcreteExecutions(
	_ context.Context,
	_ *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListConcreteExecutions is not implemented")
}

func getStartTimeFromState(state *persistencespb.WorkflowExecutionState) *time.Time {
	if state == nil || state.StartTime == nil {
		return nil
	}
	startTime := state.StartTime.AsTime()
	return &startTime
}
