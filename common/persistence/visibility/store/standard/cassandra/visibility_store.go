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
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
)

// Fixed namespace values for now
const (
	namespacePartition       = 0
	CassandraPersistenceName = "cassandra"
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT INTO open_executions (` +
		`namespace_id, namespace_partition, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateDeleteWorkflowExecutionStarted = `DELETE FROM open_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND start_time = ? ` +
		`AND run_id = ?`

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions (` +
		`namespace_id, namespace_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateDeleteWorkflowExecutionClosed = `DELETE FROM closed_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND close_time = ? ` +
		`AND run_id = ?`

	templateGetOpenWorkflowExecutions = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_queue ` +
		`FROM open_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ORDER BY start_time desc`

	templateGetOpenWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_queue ` +
		`FROM open_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetOpenWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_queue ` +
		`FROM open_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutions = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue ` +
		`FROM closed_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ORDER BY close_time desc`

	templateGetClosedWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue ` +
		`FROM closed_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue ` +
		`FROM closed_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatus = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue ` +
		`FROM closed_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND status = ? `
)

type (
	visibilityStore struct {
		session      gocql.Session
		lowConslevel gocql.Consistency
	}
)

var _ store.VisibilityStore = (*visibilityStore)(nil)

func NewVisibilityStore(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	logger log.Logger,
) (*visibilityStore, error) {
	session, err := gocql.NewSession(cfg, r, logger)
	if err != nil {
		logger.Fatal("unable to initialize cassandra session", tag.Error(err))
	}

	return &visibilityStore{
		session:      session,
		lowConslevel: gocql.One,
	}, nil
}

func (v *visibilityStore) GetName() string {
	return CassandraPersistenceName
}

// Close releases the resources held by this object
func (v *visibilityStore) Close() {
	v.session.Close()
}

func (v *visibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {

	query := v.session.Query(templateCreateWorkflowExecutionStarted,
		request.NamespaceID,
		namespacePartition,
		request.WorkflowID,
		request.RunID,
		persistence.UnixMilliseconds(request.StartTime),
		persistence.UnixMilliseconds(request.ExecutionTime),
		request.WorkflowTypeName,
		request.Memo.Data,
		request.Memo.EncodingType.String(),
		request.TaskQueue,
	).WithContext(ctx)
	// It is important to specify timestamp for all `open_executions` queries because
	// we are using milliseconds instead of default microseconds. If custom timestamp collides with
	// default timestamp, default one will always win because they are 1000 times bigger.
	query = query.WithTimestamp(persistence.UnixMilliseconds(request.StartTime))
	err := query.Exec()
	return gocql.ConvertError("RecordWorkflowExecutionStarted", err)
}

func (v *visibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	batch := v.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	// First, remove execution from the open table
	batch.Query(templateDeleteWorkflowExecutionStarted,
		request.NamespaceID,
		namespacePartition,
		persistence.UnixMilliseconds(request.StartTime),
		request.RunID,
	)

	// Next, add a row in the closed table.
	batch.Query(templateCreateWorkflowExecutionClosed,
		request.NamespaceID,
		namespacePartition,
		request.WorkflowID,
		request.RunID,
		persistence.UnixMilliseconds(request.StartTime),
		persistence.UnixMilliseconds(request.ExecutionTime),
		persistence.UnixMilliseconds(request.CloseTime),
		request.WorkflowTypeName,
		request.Status,
		request.HistoryLength,
		request.Memo.Data,
		request.Memo.EncodingType.String(),
		request.TaskQueue,
	)

	// RecordWorkflowExecutionStarted is using StartTime as the timestamp for every query in `open_executions` table.
	// Due to the fact that cross DC using mutable state creation time as workflow start time and visibility using event time
	// instead of last update time (https://github.com/uber/cadence/pull/1501) CloseTime can be before StartTime (or very close it).
	// In this case, use (StartTime + minWorkflowDuration) for delete operation to guarantee that it is greater than StartTime
	// and won't be ignored.

	const minWorkflowDuration = time.Second
	var batchTimestamp time.Time
	if request.CloseTime.Sub(request.StartTime) < minWorkflowDuration {
		batchTimestamp = request.StartTime.Add(minWorkflowDuration)
	} else {
		batchTimestamp = request.CloseTime
	}

	batch = batch.WithTimestamp(persistence.UnixMilliseconds(batchTimestamp))
	err := v.session.ExecuteBatch(batch)
	return gocql.ConvertError("RecordWorkflowExecutionClosed", err)
}

func (v *visibilityStore) UpsertWorkflowExecution(
	_ context.Context,
	_ *store.InternalUpsertWorkflowExecutionRequest,
) error {
	// Not OperationNotSupportedErr!
	return nil
}

func (v *visibilityStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(
		templateGetOpenWorkflowExecutions,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutions", err)
	}
	return response, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(
		templateGetOpenWorkflowExecutionsByType,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
		request.WorkflowTypeName,
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutionsByType", err)
	}
	return response, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(
		templateGetOpenWorkflowExecutionsByID,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
		request.WorkflowID,
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutionsByWorkflowID", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutions,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutions", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(
		templateGetClosedWorkflowExecutionsByType,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
		request.WorkflowTypeName,
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByType", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByID,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
		request.WorkflowID,
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByWorkflowID", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByStatus,
		request.NamespaceID.String(),
		namespacePartition,
		persistence.UnixMilliseconds(request.EarliestStartTime),
		persistence.UnixMilliseconds(request.LatestStartTime),
		request.Status,
	).Consistency(v.lowConslevel).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &store.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*store.InternalWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByStatus", err)
	}
	return response, nil
}

func (v *visibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	var query gocql.Query
	if request.StartTime != nil {
		query = v.session.Query(templateDeleteWorkflowExecutionStarted,
			request.NamespaceID.String(),
			namespacePartition,
			persistence.UnixMilliseconds(*request.StartTime),
			request.RunID,
		).WithContext(ctx)
	} else if request.CloseTime != nil {
		query = v.session.Query(templateDeleteWorkflowExecutionClosed,
			request.NamespaceID.String(),
			namespacePartition,
			persistence.UnixMilliseconds(*request.CloseTime),
			request.RunID,
		).WithContext(ctx)
	} else {
		panic("Cassandra visibility store: DeleteWorkflowExecution: both StartTime and CloseTime are nil")
	}

	if err := query.Consistency(v.lowConslevel).Exec(); err != nil {
		return gocql.ConvertError("DeleteWorkflowExecution", err)
	}
	return nil
}

func (v *visibilityStore) ListWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (v *visibilityStore) ScanWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (v *visibilityStore) CountWorkflowExecutions(
	_ context.Context,
	_ *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func readOpenWorkflowExecutionRecord(iter gocql.Iter) (*store.InternalWorkflowExecutionInfo, bool) {
	var workflowID string
	var runID string
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var memo []byte
	var encoding string
	var taskQueue string
	if iter.Scan(&workflowID, &runID, &startTime, &executionTime, &typeName, &memo, &encoding, &taskQueue) {
		record := &store.InternalWorkflowExecutionInfo{
			WorkflowID:    workflowID,
			RunID:         runID,
			TypeName:      typeName,
			StartTime:     startTime,
			ExecutionTime: executionTime,
			Memo:          persistence.NewDataBlob(memo, encoding),
			TaskQueue:     taskQueue,
			Status:        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}
		return record, true
	}
	return nil, false
}

func readClosedWorkflowExecutionRecord(iter gocql.Iter) (*store.InternalWorkflowExecutionInfo, bool) {
	var workflowID string
	var runID string
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var closeTime time.Time
	var status enumspb.WorkflowExecutionStatus
	var historyLength int64
	var memo []byte
	var encoding string
	var taskQueue string
	if iter.Scan(&workflowID, &runID, &startTime, &executionTime, &closeTime, &typeName, &status, &historyLength, &memo, &encoding, &taskQueue) {
		record := &store.InternalWorkflowExecutionInfo{
			WorkflowID:    workflowID,
			RunID:         runID,
			TypeName:      typeName,
			StartTime:     startTime,
			ExecutionTime: executionTime,
			CloseTime:     closeTime,
			Status:        status,
			HistoryLength: historyLength,
			Memo:          persistence.NewDataBlob(memo, encoding),
			TaskQueue:     taskQueue,
		}
		return record, true
	}
	return nil, false
}
