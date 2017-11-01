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

package persistence

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// Fixed domain values for now
const (
	domainPartition        = 0
	defaultCloseTTLSeconds = 86400
	openExecutionTTLBuffer = int64(20)
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT INTO open_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, workflow_type_name) ` +
		`VALUES (?, ?, ?, ?, ?, ?) using TTL ?`

	templateDeleteWorkflowExecutionStarted = `DELETE FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time = ? ` +
		`AND run_id = ?`

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateGetOpenWorkflowExecutions = `SELECT workflow_id, run_id, start_time, workflow_type_name ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetClosedWorkflowExecutions = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetOpenWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, workflow_type_name ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByType = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetOpenWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, workflow_type_name ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByID = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatus = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND status = ? `

	templateGetClosedWorkflowExecution = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name, status, history_length ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? ALLOW FILTERING `
)

type (
	cassandraVisibilityPersistence struct {
		session      *gocql.Session
		lowConslevel gocql.Consistency
		logger       bark.Logger
	}
)

// NewCassandraVisibilityPersistence is used to create an instance of VisibilityManager implementation
func NewCassandraVisibilityPersistence(
	hosts string, port int, user, password, dc string, keyspace string, logger bark.Logger) (VisibilityManager, error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraVisibilityPersistence{session: session, lowConslevel: gocql.One, logger: logger}, nil
}

// Close releases the resources held by this object
func (v *cassandraVisibilityPersistence) Close() {
	if v.session != nil {
		v.session.Close()
	}
}

func (v *cassandraVisibilityPersistence) RecordWorkflowExecutionStarted(
	request *RecordWorkflowExecutionStartedRequest) error {
	ttl := request.WorkflowTimeout + openExecutionTTLBuffer
	query := v.session.Query(templateCreateWorkflowExecutionStarted,
		request.DomainUUID,
		domainPartition,
		*request.Execution.WorkflowId,
		*request.Execution.RunId,
		common.UnixNanoToCQLTimestamp(request.StartTimestamp),
		request.WorkflowTypeName,
		ttl,
	)
	query = query.WithTimestamp(common.UnixNanoToCQLTimestamp(request.StartTimestamp))
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
	request *RecordWorkflowExecutionClosedRequest) error {
	batch := v.session.NewBatch(gocql.LoggedBatch)

	// First, remove execution from the open table
	batch.Query(templateDeleteWorkflowExecutionStarted,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.StartTimestamp),
		*request.Execution.RunId,
	)

	// Next, add a row in the closed table.

	// Find how long to keep the row
	retention := request.RetentionSeconds
	if retention == 0 {
		retention = defaultCloseTTLSeconds
	}

	batch.Query(templateCreateWorkflowExecutionClosed,
		request.DomainUUID,
		domainPartition,
		*request.Execution.WorkflowId,
		*request.Execution.RunId,
		common.UnixNanoToCQLTimestamp(request.StartTimestamp),
		common.UnixNanoToCQLTimestamp(request.CloseTimestamp),
		request.WorkflowTypeName,
		request.Status,
		request.HistoryLength,
		retention,
	)

	batch = batch.WithTimestamp(common.UnixNanoToCQLTimestamp(request.CloseTimestamp))
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

func (v *cassandraVisibilityPersistence) ListOpenWorkflowExecutions(
	request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutions,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime)).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutions,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime)).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutionsByType,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime),
		request.WorkflowTypeName).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutionsByType operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByType,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime),
		request.WorkflowTypeName).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByType operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutionsByID,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime),
		request.WorkflowID).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutionsByWorkflowID operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByID,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime),
		request.WorkflowID).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByWorkflowID operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByStatus,
		request.DomainUUID,
		domainPartition,
		common.UnixNanoToCQLTimestamp(request.EarliestStartTime),
		common.UnixNanoToCQLTimestamp(request.LatestStartTime),
		request.Status).Consistency(v.lowConslevel)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListClosedWorkflowExecutionsByStatus operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*workflow.WorkflowExecutionInfo, 0)
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
	request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := v.session.Query(templateGetClosedWorkflowExecution,
		request.DomainUUID,
		domainPartition,
		*execution.WorkflowId,
		*execution.RunId)

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
				*execution.WorkflowId, *execution.RunId),
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

	return &GetClosedWorkflowExecutionResponse{
		Execution: wfexecution,
	}, nil
}

func readOpenWorkflowExecutionRecord(iter *gocql.Iter) (*workflow.WorkflowExecutionInfo, bool) {
	var workflowID string
	var runID gocql.UUID
	var typeName string
	var startTime time.Time
	if iter.Scan(&workflowID, &runID, &startTime, &typeName) {
		execution := &workflow.WorkflowExecution{}
		execution.WorkflowId = common.StringPtr(workflowID)
		execution.RunId = common.StringPtr(runID.String())

		wfType := &workflow.WorkflowType{}
		wfType.Name = common.StringPtr(typeName)

		record := &workflow.WorkflowExecutionInfo{}
		record.Execution = execution
		record.StartTime = common.Int64Ptr(startTime.UnixNano())
		record.Type = wfType
		return record, true
	}
	return nil, false
}

func readClosedWorkflowExecutionRecord(iter *gocql.Iter) (*workflow.WorkflowExecutionInfo, bool) {
	var workflowID string
	var runID gocql.UUID
	var typeName string
	var startTime time.Time
	var closeTime time.Time
	var status workflow.WorkflowExecutionCloseStatus
	var historyLength int64
	if iter.Scan(&workflowID, &runID, &startTime, &closeTime, &typeName, &status, &historyLength) {
		execution := &workflow.WorkflowExecution{}
		execution.WorkflowId = common.StringPtr(workflowID)
		execution.RunId = common.StringPtr(runID.String())

		wfType := &workflow.WorkflowType{}
		wfType.Name = common.StringPtr(typeName)

		record := &workflow.WorkflowExecutionInfo{}
		record.Execution = execution
		record.StartTime = common.Int64Ptr(startTime.UnixNano())
		record.CloseTime = common.Int64Ptr(closeTime.UnixNano())
		record.Type = wfType
		record.CloseStatus = &status
		record.HistoryLength = common.Int64Ptr(historyLength)
		return record, true
	}
	return nil, false
}
