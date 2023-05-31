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
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	templateUpdateLeaseQuery = `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateCurrentWorkflowExecutionQuery = `UPDATE executions USING TTL 0 ` +
		`SET current_run_id = ?, execution_state = ?, execution_state_encoding = ?, workflow_last_write_version = ?, workflow_state = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF current_run_id = ? `

	templateUpdateCurrentWorkflowExecutionForNewQuery = templateUpdateCurrentWorkflowExecutionQuery +
		`and workflow_last_write_version = ? ` +
		`and workflow_state = ? `

	templateCreateCurrentWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, ` +
		`visibility_ts, task_id, current_run_id, execution_state, execution_state_encoding, ` +
		`workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS USING TTL 0 `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, type, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, db_record_version, ` +
		`visibility_ts, task_id, checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS `

	templateGetWorkflowExecutionQuery = `SELECT execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, activity_map, activity_map_encoding, timer_map, timer_map_encoding, ` +
		`child_executions_map, child_executions_map_encoding, request_cancel_map, request_cancel_map_encoding, signal_map, signal_map_encoding, signal_requested, buffered_events_list, ` +
		`checksum, checksum_encoding, db_record_version ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution, execution_encoding, execution_state, execution_state_encoding, workflow_last_write_version ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateListWorkflowExecutionQuery = `SELECT run_id, execution, execution_encoding, execution_state, execution_state_encoding, next_event_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?`

	// TODO deprecate templateUpdateWorkflowExecutionQueryDeprecated in favor of templateUpdateWorkflowExecutionQuery
	// Deprecated.
	templateUpdateWorkflowExecutionQueryDeprecated = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, db_record_version = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? `
	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, db_record_version = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF db_record_version = ? `

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map[ ? ] = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map[ ? ] = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map[ ? ] = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateAppendBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = buffered_events_list + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = [] ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteChildExecutionInfoQuery = `DELETE child_executions_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteRequestCancelInfoQuery = `DELETE request_cancel_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteSignalInfoQuery = `DELETE signal_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionMutableStateQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionCurrentRowQuery = templateDeleteWorkflowExecutionMutableStateQuery + " if current_run_id = ? "

	templateDeleteWorkflowExecutionSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested - ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `
)

type (
	MutableStateStore struct {
		Session gocql.Session
		Logger  log.Logger
	}
)

func NewMutableStateStore(
	session gocql.Session,
	logger log.Logger,
) *MutableStateStore {
	return &MutableStateStore{
		Session: session,
		Logger:  logger,
	}
}

func (d *MutableStateStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	shardID := request.ShardID
	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := newWorkflow.NamespaceID
	workflowID := newWorkflow.WorkflowID
	runID := newWorkflow.RunID

	var requestCurrentRunID string

	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// noop

	case p.CreateWorkflowModeUpdateCurrent:
		batch.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
			runID,
			newWorkflow.ExecutionStateBlob.Data,
			newWorkflow.ExecutionStateBlob.EncodingType.String(),
			lastWriteVersion,
			newWorkflow.ExecutionState.State,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.PreviousRunID,
			request.PreviousLastWriteVersion,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		)

		requestCurrentRunID = request.PreviousRunID

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
			newWorkflow.ExecutionStateBlob.Data,
			newWorkflow.ExecutionStateBlob.EncodingType.String(),
			lastWriteVersion,
			newWorkflow.ExecutionState.State,
		)

		requestCurrentRunID = ""

	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotBatchAsNew(batch,
		request.ShardID,
		&newWorkflow,
	); err != nil {
		return nil, err
	}

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	conflictRecord := newConflictRecord()
	applied, conflictIter, err := d.Session.MapExecuteBatchCAS(batch, conflictRecord)
	if err != nil {
		return nil, gocql.ConvertError("CreateWorkflowExecution", err)
	}
	defer func() {
		_ = conflictIter.Close()
	}()

	if !applied {
		return nil, convertErrors(
			conflictRecord,
			conflictIter,
			shardID,
			request.RangeID,
			requestCurrentRunID,
			[]executionCASCondition{{
				runID: newWorkflow.ExecutionState.RunId,
				// dbVersion is for CAS, so the db record version will be set to `updateWorkflow.DBRecordVersion`
				// while CAS on `updateWorkflow.DBRecordVersion - 1`
				dbVersion:   newWorkflow.DBRecordVersion - 1,
				nextEventID: newWorkflow.Condition,
			}},
		)
	}

	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (d *MutableStateStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	query := d.Session.Query(templateGetWorkflowExecutionQuery,
		request.ShardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	).WithContext(ctx)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetWorkflowExecution", err)
	}

	state, err := mutableStateFromRow(result)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}

	activityInfos := make(map[int64]*commonpb.DataBlob)
	aMap := result["activity_map"].(map[int64][]byte)
	aMapEncoding := result["activity_map_encoding"].(string)
	for key, value := range aMap {
		activityInfos[key] = p.NewDataBlob(value, aMapEncoding)
	}
	state.ActivityInfos = activityInfos

	timerInfos := make(map[string]*commonpb.DataBlob)
	tMapEncoding := result["timer_map_encoding"].(string)
	tMap := result["timer_map"].(map[string][]byte)
	for key, value := range tMap {
		timerInfos[key] = p.NewDataBlob(value, tMapEncoding)
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*commonpb.DataBlob)
	cMap := result["child_executions_map"].(map[int64][]byte)
	cMapEncoding := result["child_executions_map_encoding"].(string)
	for key, value := range cMap {
		childExecutionInfos[key] = p.NewDataBlob(value, cMapEncoding)
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*commonpb.DataBlob)
	rMapEncoding := result["request_cancel_map_encoding"].(string)
	rMap := result["request_cancel_map"].(map[int64][]byte)
	for key, value := range rMap {
		requestCancelInfos[key] = p.NewDataBlob(value, rMapEncoding)
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*commonpb.DataBlob)
	sMapEncoding := result["signal_map_encoding"].(string)
	sMap := result["signal_map"].(map[int64][]byte)
	for key, value := range sMap {
		signalInfos[key] = p.NewDataBlob(value, sMapEncoding)
	}
	state.SignalInfos = signalInfos
	state.SignalRequestedIDs = gocql.UUIDsToStringSlice(result["signal_requested"])

	eList := result["buffered_events_list"].([]map[string]interface{})
	bufferedEventsBlobs := make([]*commonpb.DataBlob, 0, len(eList))
	for _, v := range eList {
		blob := createHistoryEventBatchBlob(v)
		bufferedEventsBlobs = append(bufferedEventsBlobs, blob)
	}
	state.BufferedEvents = bufferedEventsBlobs

	state.Checksum = p.NewDataBlob(result["checksum"].([]byte), result["checksum_encoding"].(string))

	dbVersion := int64(0)
	if dbRecordVersion, ok := result["db_record_version"]; ok {
		dbVersion = dbRecordVersion.(int64)
	} else {
		dbVersion = 0
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbVersion,
	}, nil
}

func (d *MutableStateStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	namespaceID := updateWorkflow.NamespaceID
	workflowID := updateWorkflow.WorkflowID
	runID := updateWorkflow.RunID
	shardID := request.ShardID

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			ctx,
			request.ShardID,
			namespaceID,
			workflowID,
			runID,
		); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			newLastWriteVersion := newWorkflow.LastWriteVersion
			newNamespaceID := newWorkflow.NamespaceID
			newWorkflowID := newWorkflow.WorkflowID
			newRunID := newWorkflow.RunID

			if namespaceID != newNamespaceID {
				return serviceerror.NewInternal("UpdateWorkflowExecution: cannot continue as new to another namespace")
			}

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				newRunID,
				newWorkflow.ExecutionStateBlob.Data,
				newWorkflow.ExecutionStateBlob.EncodingType.String(),
				newLastWriteVersion,
				newWorkflow.ExecutionState.State,
				shardID,
				rowTypeExecution,
				newNamespaceID,
				newWorkflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				runID,
			)

		} else {
			lastWriteVersion := updateWorkflow.LastWriteVersion

			executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				updateWorkflow.ExecutionState.State,
				request.ShardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				runID,
			)
		}

	default:
		return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowMutationBatch(batch, shardID, &updateWorkflow); err != nil {
		return err
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotBatchAsNew(batch,
			request.ShardID,
			newWorkflow,
		); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	conflictRecord := newConflictRecord()
	applied, conflictIter, err := d.Session.MapExecuteBatchCAS(batch, conflictRecord)
	if err != nil {
		return gocql.ConvertError("UpdateWorkflowExecution", err)
	}
	defer func() {
		_ = conflictIter.Close()
	}()

	if !applied {
		return convertErrors(
			conflictRecord,
			conflictIter,
			request.ShardID,
			request.RangeID,
			updateWorkflow.ExecutionState.RunId,
			[]executionCASCondition{{
				runID: updateWorkflow.ExecutionState.RunId,
				// dbVersion is for CAS, so the db record version will be set to `updateWorkflow.DBRecordVersion`
				// while CAS on `updateWorkflow.DBRecordVersion - 1`
				dbVersion:   updateWorkflow.DBRecordVersion - 1,
				nextEventID: updateWorkflow.Condition,
			}},
		)
	}
	return nil
}

func (d *MutableStateStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := request.ShardID

	namespaceID := resetWorkflow.NamespaceID
	workflowID := resetWorkflow.WorkflowID

	var currentRunID string

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			ctx,
			shardID,
			namespaceID,
			workflowID,
			resetWorkflow.ExecutionState.RunId,
		); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionState := resetWorkflow.ExecutionState
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			lastWriteVersion = newWorkflow.LastWriteVersion
			executionState = newWorkflow.ExecutionState
		}
		runID := executionState.RunId
		createRequestID := executionState.CreateRequestId
		state := executionState.State
		status := executionState.Status

		executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
			RunId:           runID,
			CreateRequestId: createRequestID,
			State:           state,
			Status:          status,
		})
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Error: %v", err))
		}

		if currentWorkflow != nil {
			currentRunID = currentWorkflow.ExecutionState.RunId

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				state,
				shardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				currentRunID,
			)

		} else {
			// reset workflow is current
			currentRunID = resetWorkflow.ExecutionState.RunId

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				state,
				shardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				currentRunID,
			)
		}

	default:
		return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotBatchAsReset(batch, shardID, &resetWorkflow); err != nil {
		return err
	}

	if currentWorkflow != nil {
		if err := applyWorkflowMutationBatch(batch, shardID, currentWorkflow); err != nil {
			return err
		}
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotBatchAsNew(batch, shardID, newWorkflow); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	conflictRecord := newConflictRecord()
	applied, conflictIter, err := d.Session.MapExecuteBatchCAS(batch, conflictRecord)
	if err != nil {
		return gocql.ConvertError("ConflictResolveWorkflowExecution", err)
	}
	defer func() {
		_ = conflictIter.Close()
	}()

	if !applied {
		executionCASConditions := []executionCASCondition{{
			runID: resetWorkflow.RunID,
			// dbVersion is for CAS, so the db record version will be set to `resetWorkflow.DBRecordVersion`
			// while CAS on `resetWorkflow.DBRecordVersion - 1`
			dbVersion:   resetWorkflow.DBRecordVersion - 1,
			nextEventID: resetWorkflow.Condition,
		}}
		if currentWorkflow != nil {
			executionCASConditions = append(executionCASConditions, executionCASCondition{
				runID: currentWorkflow.RunID,
				// dbVersion is for CAS, so the db record version will be set to `currentWorkflow.DBRecordVersion`
				// while CAS on `currentWorkflow.DBRecordVersion - 1`
				dbVersion:   currentWorkflow.DBRecordVersion - 1,
				nextEventID: currentWorkflow.Condition,
			})
		}
		return convertErrors(
			conflictRecord,
			conflictIter,
			request.ShardID,
			request.RangeID,
			currentRunID,
			executionCASConditions,
		)
	}
	return nil
}

func (d *MutableStateStore) assertNotCurrentExecution(
	ctx context.Context,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	if resp, err := d.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}); err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// allow bypassing no current record
			return nil
		}
		return err
	} else if resp.RunID == runID {
		return &p.CurrentWorkflowConditionFailedError{
			Msg:              fmt.Sprintf("Assertion on current record failed. Current run ID is not expected: %v", resp.RunID),
			RequestID:        "",
			RunID:            "",
			State:            enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
			LastWriteVersion: 0,
		}
	}

	return nil
}

func (d *MutableStateStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	query := d.Session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		request.ShardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowExecution", err)
}

func (d *MutableStateStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	query := d.Session.Query(templateDeleteWorkflowExecutionCurrentRowQuery,
		request.ShardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.RunID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowCurrentRow", err)
}

func (d *MutableStateStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, error) {
	query := d.Session.Query(templateGetCurrentExecutionQuery,
		request.ShardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	).WithContext(ctx)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetCurrentExecution", err)
	}

	currentRunID := gocql.UUIDToString(result["current_run_id"])
	executionStateBlob, err := executionStateBlobFromRow(result)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}

	// TODO: fix blob ExecutionState in storage should not be a blob.
	executionState, err := serialization.WorkflowExecutionStateFromBlob(executionStateBlob.Data, executionStateBlob.EncodingType.String())
	if err != nil {
		return nil, err
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          currentRunID,
		ExecutionState: executionState,
	}, nil
}

func (d *MutableStateStore) SetWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	shardID := request.ShardID
	setSnapshot := request.SetWorkflowSnapshot

	if err := applyWorkflowSnapshotBatchAsReset(batch, shardID, &setSnapshot); err != nil {
		return err
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	conflictRecord := newConflictRecord()
	applied, conflictIter, err := d.Session.MapExecuteBatchCAS(batch, conflictRecord)
	if err != nil {
		return gocql.ConvertError("SetWorkflowExecution", err)
	}
	defer func() {
		_ = conflictIter.Close()
	}()

	if !applied {
		executionCASConditions := []executionCASCondition{{
			runID: setSnapshot.RunID,
			// dbVersion is for CAS, so the db record version will be set to `setSnapshot.DBRecordVersion`
			// while CAS on `setSnapshot.DBRecordVersion - 1`
			dbVersion:   setSnapshot.DBRecordVersion - 1,
			nextEventID: setSnapshot.Condition,
		}}
		return convertErrors(
			conflictRecord,
			conflictIter,
			request.ShardID,
			request.RangeID,
			"",
			executionCASConditions,
		)
	}
	return nil
}

func (d *MutableStateStore) ListConcreteExecutions(
	ctx context.Context,
	request *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	query := d.Session.Query(
		templateListWorkflowExecutionQuery,
		request.ShardID,
		rowTypeExecution,
	).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.PageToken).Iter()

	response := &p.InternalListConcreteExecutionsResponse{}
	result := make(map[string]interface{})
	for iter.MapScan(result) {
		runID := gocql.UUIDToString(result["run_id"])
		if runID == permanentRunID {
			result = make(map[string]interface{})
			continue
		}
		if _, ok := result["execution"]; ok {
			state, err := mutableStateFromRow(result)
			if err != nil {
				return nil, err
			}
			response.States = append(response.States, state)
		}
		result = make(map[string]interface{})
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	return response, nil
}

func mutableStateFromRow(
	result map[string]interface{},
) (*p.InternalWorkflowMutableState, error) {
	eiBytes, ok := result["execution"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution", "", eiBytes, result)
	}

	eiEncoding, ok := result["execution_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_encoding", "", eiEncoding, result)
	}

	nextEventID, ok := result["next_event_id"].(int64)
	if !ok {
		return nil, newPersistedTypeMismatchError("next_event_id", "", nextEventID, result)
	}

	protoState, err := executionStateBlobFromRow(result)
	if err != nil {
		return nil, err
	}

	mutableState := &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(eiBytes, eiEncoding),
		ExecutionState: protoState,
		NextEventID:    nextEventID,
	}
	return mutableState, nil
}

func executionStateBlobFromRow(
	result map[string]interface{},
) (*commonpb.DataBlob, error) {
	state, ok := result["execution_state"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state", "", state, result)
	}

	stateEncoding, ok := result["execution_state_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state_encoding", "", stateEncoding, result)
	}

	return p.NewDataBlob(state, stateEncoding), nil
}
