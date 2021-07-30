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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/resolver"
)

// Fixed namespace values for now
const (
	namespacePartition       = 0
	cassandraPersistenceName = "cassandra"

	// ref: https://docs.datastax.com/en/dse-trblshoot/doc/troubleshooting/recoveringTtlYear2038Problem.html
	maxCassandraTTL = int64(315360000) // Cassandra max support time is 2038-01-19T03:14:06+00:00. Updated this to 10 years to support until year 2028
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

	templateCreateWorkflowExecutionClosedWithTTL = `INSERT INTO closed_executions (` +
		`namespace_id, namespace_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions (` +
		`namespace_id, namespace_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetOpenWorkflowExecutions = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_queue ` +
		`FROM open_executions ` +
		`WHERE namespace_id = ? ` +
		`AND namespace_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

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
		`AND namespace_partition IN (?) ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? `

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

var _ visibility.VisibilityStore = (*visibilityStore)(nil)

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
	return cassandraPersistenceName
}

// Close releases the resources held by this object
func (v *visibilityStore) Close() {
	v.session.Close()
}

func (v *visibilityStore) RecordWorkflowExecutionStarted(
	request *visibility.InternalRecordWorkflowExecutionStartedRequest) error {

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
	)
	// It is important to specify timestamp for all `open_executions` queries because
	// we are using milliseconds instead of default microseconds. If custom timestamp collides with
	// default timestamp, default one will always win because they are 1000 times bigger.
	query = query.WithTimestamp(persistence.UnixMilliseconds(request.StartTime))
	err := query.Exec()
	return gocql.ConvertError("RecordWorkflowExecutionStarted", err)
}

func (v *visibilityStore) RecordWorkflowExecutionClosed(request *visibility.InternalRecordWorkflowExecutionClosedRequest) error {
	batch := v.session.NewBatch(gocql.LoggedBatch)

	// First, remove execution from the open table
	batch.Query(templateDeleteWorkflowExecutionStarted,
		request.NamespaceID,
		namespacePartition,
		persistence.UnixMilliseconds(request.StartTime),
		request.RunID,
	)

	// Next, add a row in the closed table.

	// Find how long to keep the row
	var retentionSeconds int64
	if request.Retention != nil {
		retentionSeconds = int64(request.Retention.Seconds())
	} else {
		retentionSeconds = maxCassandraTTL + 1
	}

	if retentionSeconds > maxCassandraTTL {
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
	} else {
		batch.Query(templateCreateWorkflowExecutionClosedWithTTL,
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
			retentionSeconds,
		)
	}

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

func (v *visibilityStore) UpsertWorkflowExecution(_ *visibility.InternalUpsertWorkflowExecutionRequest) error {
	// Not OperationNotSupportedErr!
	return nil
}

func (v *visibilityStore) ListOpenWorkflowExecutions(
	request *visibility.ListWorkflowExecutionsRequest,
) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetOpenWorkflowExecutions,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime)).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutions", err)
	}
	return response, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByType(
	request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetOpenWorkflowExecutionsByType,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime),
			request.WorkflowTypeName).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutionsByType", err)
	}
	return response, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetOpenWorkflowExecutionsByID,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime),
			request.WorkflowID).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListOpenWorkflowExecutionsByWorkflowID", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutions(
	request *visibility.ListWorkflowExecutionsRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetClosedWorkflowExecutions,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime)).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutions", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByType(
	request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetClosedWorkflowExecutionsByType,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime),
			request.WorkflowTypeName).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByType", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetClosedWorkflowExecutionsByID,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime),
			request.WorkflowID).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByWorkflowID", err)
	}
	return response, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	request *visibility.ListClosedWorkflowExecutionsByStatusRequest) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.
		Query(templateGetClosedWorkflowExecutionsByStatus,
			request.NamespaceID,
			namespacePartition,
			persistence.UnixMilliseconds(request.EarliestStartTime),
			persistence.UnixMilliseconds(request.LatestStartTime),
			request.Status).
		Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &visibility.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*visibility.VisibilityWorkflowExecutionInfo, 0, request.PageSize)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListClosedWorkflowExecutionsByStatus", err)
	}
	return response, nil
}

// DeleteWorkflowExecution is a no-op since deletes are auto-handled by cassandra TTLs
func (v *visibilityStore) DeleteWorkflowExecution(_ *visibility.VisibilityDeleteWorkflowExecutionRequest) error {
	return nil
}

func (v *visibilityStore) ListWorkflowExecutions(_ *visibility.ListWorkflowExecutionsRequestV2) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func (v *visibilityStore) ScanWorkflowExecutions(_ *visibility.ListWorkflowExecutionsRequestV2) (*visibility.InternalListWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func (v *visibilityStore) CountWorkflowExecutions(_ *visibility.CountWorkflowExecutionsRequest) (*visibility.CountWorkflowExecutionsResponse, error) {
	return nil, visibility.OperationNotSupportedErr
}

func readOpenWorkflowExecutionRecord(iter gocql.Iter) (*visibility.VisibilityWorkflowExecutionInfo, bool) {
	var workflowID string
	var runID string
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var memo []byte
	var encoding string
	var taskQueue string
	if iter.Scan(&workflowID, &runID, &startTime, &executionTime, &typeName, &memo, &encoding, &taskQueue) {
		record := &visibility.VisibilityWorkflowExecutionInfo{
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

func readClosedWorkflowExecutionRecord(iter gocql.Iter) (*visibility.VisibilityWorkflowExecutionInfo, bool) {
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
		record := &visibility.VisibilityWorkflowExecutionInfo{
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
