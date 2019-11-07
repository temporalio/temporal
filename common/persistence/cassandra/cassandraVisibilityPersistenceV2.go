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

	"github.com/uber/cadence/common/cassandra"

	"github.com/gocql/gocql"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

const (
	templateGetClosedWorkflowExecutionsV2 = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? `

	templateGetClosedWorkflowExecutionsByTypeV2 = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByIDV2 = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatusV2 = `SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding ` +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND status = ? `
)

type (
	cassandraVisibilityPersistenceV2 struct {
		cassandraStore
		lowConslevel gocql.Consistency
		persistence  p.VisibilityStore
	}
)

// NewVisibilityPersistenceV2 create a wrapper of cassandra visibilityPersistence, with all list closed executions using v2 table
func NewVisibilityPersistenceV2(persistence p.VisibilityStore, cfg *config.Cassandra, logger log.Logger) (p.VisibilityStore, error) {
	cluster := cassandra.NewCassandraCluster(*cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraVisibilityPersistenceV2{
		cassandraStore: cassandraStore{session: session, logger: logger},
		lowConslevel:   gocql.One,
		persistence:    persistence,
	}, nil
}

// Close releases the resources held by this object
func (v *cassandraVisibilityPersistenceV2) Close() {
	if v.session != nil {
		v.session.Close()
	}
	v.persistence.Close()
}

func (v *cassandraVisibilityPersistenceV2) GetName() string {
	return v.persistence.GetName()
}

func (v *cassandraVisibilityPersistenceV2) RecordWorkflowExecutionStarted(
	request *p.InternalRecordWorkflowExecutionStartedRequest) error {
	return v.persistence.RecordWorkflowExecutionStarted(request)
}

func (v *cassandraVisibilityPersistenceV2) RecordWorkflowExecutionClosed(
	request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	return v.persistence.RecordWorkflowExecutionClosed(request)
}

func (v *cassandraVisibilityPersistenceV2) UpsertWorkflowExecution(
	request *p.InternalUpsertWorkflowExecutionRequest) error {
	return v.persistence.UpsertWorkflowExecution(request)
}

func (v *cassandraVisibilityPersistenceV2) ListOpenWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return v.persistence.ListOpenWorkflowExecutions(request)
}

func (v *cassandraVisibilityPersistenceV2) ListOpenWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return v.persistence.ListOpenWorkflowExecutionsByType(request)
}

func (v *cassandraVisibilityPersistenceV2) ListOpenWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	return v.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (v *cassandraVisibilityPersistenceV2) GetClosedWorkflowExecution(
	request *p.GetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {
	return v.persistence.GetClosedWorkflowExecution(request)
}

func (v *cassandraVisibilityPersistenceV2) ListClosedWorkflowExecutions(
	request *p.ListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsV2,
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

func (v *cassandraVisibilityPersistenceV2) ListClosedWorkflowExecutionsByType(
	request *p.ListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByTypeV2,
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

func (v *cassandraVisibilityPersistenceV2) ListClosedWorkflowExecutionsByWorkflowID(
	request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByIDV2,
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

func (v *cassandraVisibilityPersistenceV2) ListClosedWorkflowExecutionsByStatus(
	request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutionsByStatusV2,
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

func (v *cassandraVisibilityPersistenceV2) ListWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return v.persistence.ListWorkflowExecutions(request)
}

func (v *cassandraVisibilityPersistenceV2) ScanWorkflowExecutions(request *p.ListWorkflowExecutionsRequestV2) (*p.InternalListWorkflowExecutionsResponse, error) {
	return v.persistence.ScanWorkflowExecutions(request)
}

func (v *cassandraVisibilityPersistenceV2) CountWorkflowExecutions(request *p.CountWorkflowExecutionsRequest) (*p.CountWorkflowExecutionsResponse, error) {
	return v.persistence.CountWorkflowExecutions(request)
}

// DeleteWorkflowExecution is a no-op since deletes are auto-handled by cassandra TTLs
func (v *cassandraVisibilityPersistenceV2) DeleteWorkflowExecution(request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	return nil
}
