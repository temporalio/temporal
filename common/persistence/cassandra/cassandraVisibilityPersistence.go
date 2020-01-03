// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"github.com/uber/cadence/common/cassandra"

	"github.com/gocql/gocql"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

// Fixed domain values for now
const (
	domainPartition        = 0
	defaultCloseTTLSeconds = 86400
	openExecutionTTLBuffer = int64(86400) // setting it to a day to account for shard going down

	maxCassandraTTL = int64(630720000) // Cassandra TTL maximum, 20 years in second
)

const (
	templateCreateWorkflowExecutionStartedWithTTL = `INSERT INTO open_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionStarted = `INSERT INTO open_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateDeleteWorkflowExecutionStarted = `DELETE FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time = ? ` +
		`AND run_id = ?`

	templateCreateWorkflowExecutionClosedWithTTL = `INSERT INTO closed_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateWorkflowExecutionClosedWithTTLV2 = `INSERT INTO closed_executions_v2 (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionClosedV2 = `INSERT INTO closed_executions_v2 (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetOpenWorkflowExecutions = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetClosedWorkflowExecutions = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetOpenWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetOpenWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatus = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND status = ? `

	templateGetClosedWorkflowExecution = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? ALLOW FILTERING `
)

type (
	cassandraVisibilityPersistence struct {
		cassandraStore
		lowConslevel gocql.Consistency
	}
)

// newVisibilityPersistence is used to create an instance of VisibilityManager implementation
func newVisibilityPersistence(cfg config.Cassandra, logger log.Logger) (p.VisibilityStore, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraVisibilityPersistence{
		cassandraStore: cassandraStore{session: session, logger: logger},
		lowConslevel:   gocql.One,
	}, nil
}

// Close releases the resources held by this object
func (v *cassandraVisibilityPersistence) Close() {
	if v.session != nil {
		v.session.Close()
	}
}

func (v *cassandraVisibilityPersistence) RecordWorkflowExecutionStarted(
	request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	ttl := request.WorkflowTimeout + openExecutionTTLBuffer
	var query *gocql.Query

	if ttl > maxCassandraTTL {
		query = v.session.Query(templateCreateWorkflowExecutionStarted,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			request.WorkflowTypeName,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
		)
	} else {
		query = v.session.Query(templateCreateWorkflowExecutionStartedWithTTL,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			request.WorkflowTypeName,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
			ttl,
		)
	}
	query = query.WithTimestamp(p.UnixNanoToDBTimestamp(request.StartTimestamp))
	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("RecordWorkflowExecutionStarted operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RecordWorkflowExecutionStarted operation failed. Error: %v", err),
		}
	}

	return nil
}

func (v *cassandraVisibilityPersistence) RecordWorkflowExecutionClosed(
	request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	batch := v.session.NewBatch(gocql.LoggedBatch)

	// First, remove execution from the open table
	batch.Query(templateDeleteWorkflowExecutionStarted,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.StartTimestamp),
		request.RunID,
	)

	// Next, add a row in the closed table.

	// Find how long to keep the row
	retention := request.RetentionSeconds
	if retention == 0 {
		retention = defaultCloseTTLSeconds
	}

	if retention > maxCassandraTTL {
		batch.Query(templateCreateWorkflowExecutionClosed,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			p.UnixNanoToDBTimestamp(request.CloseTimestamp),
			request.WorkflowTypeName,
			request.Status,
			request.HistoryLength,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
		)
		// duplicate write to v2 to order by close time
		batch.Query(templateCreateWorkflowExecutionClosedV2,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			p.UnixNanoToDBTimestamp(request.CloseTimestamp),
			request.WorkflowTypeName,
			request.Status,
			request.HistoryLength,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
		)
	} else {
		batch.Query(templateCreateWorkflowExecutionClosedWithTTL,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			p.UnixNanoToDBTimestamp(request.CloseTimestamp),
			request.WorkflowTypeName,
			request.Status,
			request.HistoryLength,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
			retention,
		)
		// duplicate write to v2 to order by close time
		batch.Query(templateCreateWorkflowExecutionClosedWithTTLV2,
			request.DomainUUID,
			domainPartition,
			request.WorkflowID,
			request.RunID,
			p.UnixNanoToDBTimestamp(request.StartTimestamp),
			p.UnixNanoToDBTimestamp(request.ExecutionTimestamp),
			p.UnixNanoToDBTimestamp(request.CloseTimestamp),
			request.WorkflowTypeName,
			request.Status,
			request.HistoryLength,
			request.Memo.Data,
			string(request.Memo.GetEncoding()),
			retention,
		)
	}

	// RecordWorkflowExecutionStarted is using StartTimestamp as
	// the timestamp to issue query to Cassandra
	// due to the fact that cross DC using mutable state creation time as workflow start time
	// and visibility using event time instead of last update time (#1501)
	// CloseTimestamp can be before StartTimestamp, meaning using CloseTimestamp
	// can cause the deletion of open visibility record to be ignored.
	queryTimeStamp := request.CloseTimestamp
	if queryTimeStamp < request.StartTimestamp {
		queryTimeStamp = request.StartTimestamp + time.Second.Nanoseconds()
	}
	batch = batch.WithTimestamp(p.UnixNanoToDBTimestamp(queryTimeStamp))
	err := v.session.ExecuteBatch(batch)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("RecordWorkflowExecutionClosed operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RecordWorkflowExecutionClosed operation failed. Error: %v", err),
		}
	}
	return nil
}

func (v *cassandraVisibilityPersistence) UpsertWorkflowExecution(
	request *p.InternalUpsertWorkflowExecutionRequest) error {
	if p.IsNopUpsertWorkflowRequest(request) {
		return nil
	}
	return p.NewOperationNotSupportErrorForVis()
}

func (v *cassandraVisibilityPersistence) ListOpenWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutions,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime)).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListOpenWorkflowExecutions operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutions operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListClosedWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutions,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime)).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListClosedWorkflowExecutions operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutions operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListOpenWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutionsByType,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime),
		request.WorkflowTypeName).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutionsByType operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListOpenWorkflowExecutionsByType operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutionsByType operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListClosedWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByType,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime),
		request.WorkflowTypeName).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByType operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListClosedWorkflowExecutionsByType operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByType operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListOpenWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutionsByID,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime),
		request.WorkflowID).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutionsByWorkflowID operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readOpenWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readOpenWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutionsByWorkflowID operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListClosedWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByID,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime),
		request.WorkflowID).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByWorkflowID operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByWorkflowID operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListClosedWorkflowExecutionsByStatus(
	request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByStatus,
		request.DomainUUID,
		domainPartition,
		p.UnixNanoToDBTimestamp(request.EarliestStartTime),
		p.UnixNanoToDBTimestamp(request.LatestStartTime),
		request.Status).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByStatus operation failed.  Not able to create query iterator.",
		}
	}

	response := &p.InternalListWorkflowExecutionsResponse{}
	response.Executions = make([]*p.VisibilityWorkflowExecutionInfo, 0)
	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	for has {
		response.Executions = append(response.Executions, wfexecution)
		wfexecution, has = readClosedWorkflowExecutionRecord(iter)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ListClosedWorkflowExecutionsByStatus operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutionsByStatus operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) GetClosedWorkflowExecution(
	request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := v.session.Query(templateGetClosedWorkflowExecution,
		request.DomainUUID,
		domainPartition,
		execution.GetWorkflowId(),
		execution.GetRunId())

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetClosedWorkflowExecution operation failed.  Not able to create query iterator.",
		}
	}

	wfexecution, has := readClosedWorkflowExecutionRecord(iter)
	if !has {
		return nil, &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
				execution.GetWorkflowId(), execution.GetRunId()),
		}
	}

	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetClosedWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetClosedWorkflowExecution operation failed. Error: %v", err),
		}
	}

	return &p.InternalGetClosedWorkflowExecutionResponse{
		Execution: wfexecution,
	}, nil
}

// DeleteWorkflowExecution is a no-op since deletes are auto-handled by cassandra TTLs
func (v *cassandraVisibilityPersistence) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	return nil
}

func (v *cassandraVisibilityPersistence) ListWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func (v *cassandraVisibilityPersistence) ScanWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func (v *cassandraVisibilityPersistence) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (*p.CountWorkflowExecutionsResponse, error) {
	return nil, p.NewOperationNotSupportErrorForVis()
}

func readOpenWorkflowExecutionRecord(iter *gocql.Iter) (*p.VisibilityWorkflowExecutionInfo, bool) {
	var workflowID string
	var runID gocql.UUID
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var memo []byte
	var encoding string
	if iter.Scan(&workflowID, &runID, &startTime, &executionTime, &typeName, &memo, &encoding) {
		record := &p.VisibilityWorkflowExecutionInfo{
			WorkflowID:    workflowID,
			RunID:         runID.String(),
			TypeName:      typeName,
			StartTime:     startTime,
			ExecutionTime: executionTime,
			Memo:          p.NewDataBlob(memo, common.EncodingType(encoding)),
		}
		return record, true
	}
	return nil, false
}

func readClosedWorkflowExecutionRecord(iter *gocql.Iter) (*p.VisibilityWorkflowExecutionInfo, bool) {
	var workflowID string
	var runID gocql.UUID
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var closeTime time.Time
	var status workflow.WorkflowExecutionCloseStatus
	var historyLength int64
	var memo []byte
	var encoding string
	if iter.Scan(&workflowID, &runID, &startTime, &executionTime, &closeTime, &typeName, &status, &historyLength, &memo, &encoding) {
		record := &p.VisibilityWorkflowExecutionInfo{
			WorkflowID:    workflowID,
			RunID:         runID.String(),
			TypeName:      typeName,
			StartTime:     startTime,
			ExecutionTime: executionTime,
			CloseTime:     closeTime,
			Status:        &status,
			HistoryLength: historyLength,
			Memo:          p.NewDataBlob(memo, common.EncodingType(encoding)),
		}
		return record, true
	}
	return nil, false
}
